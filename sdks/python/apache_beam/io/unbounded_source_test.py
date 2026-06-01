#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for apache_beam.io.unbounded_source.

Semantics are covered by deterministic unit tests; the end-to-end DirectRunner
tests assert ordering and termination only (no flaky defer-timing assertions).
"""

# pytype: skip-file

import logging
import os
import pickle
import tempfile
import unittest

from typing_extensions import override

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import unbounded_source as _unbounded_source_module
from apache_beam.io.unbounded_source import _NO_DATA
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.io.unbounded_source import _set_watermark_if_greater
from apache_beam.io.unbounded_source import _UnboundedSourceRestriction
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionCoder
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionProvider
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import core
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

# pylint: disable=expression-not-assigned

# Realistic (non-epoch) event-time base for the demo source.
_EVENT_TIME_BASE = Timestamp(1729987200)  # 2024-10-27T00:00:00Z

# ------------------------------------------------------------------------------
# In-memory demo source emitting integers 0..count-1 (event time
# ``_EVENT_TIME_BASE + index``); self-terminates at EOF, resumes from
# ``last_index + 1``, and splits into even/odd sub-sources when is_splittable.
# ------------------------------------------------------------------------------


class _CountingCheckpointMark(CheckpointMark):
  def __init__(self, last_index, finalize_log=None):
    self.last_index = last_index
    self._finalize_log = finalize_log

  @override
  def finalize_checkpoint(self):
    if self._finalize_log is not None:
      self._finalize_log.append(self.last_index)

  def __eq__(self, other):
    return (
        isinstance(other, _CountingCheckpointMark) and
        other.last_index == self.last_index)

  def __hash__(self):
    return hash(self.last_index)

  def __repr__(self):
    return '_CountingCheckpointMark(last_index=%r)' % (self.last_index, )


class _CountingReader(UnboundedReader):
  def __init__(
      self, count, start_index, finalize_log=None, modulus=1, residue=0):
    self._count = count
    self._next = start_index
    self._modulus = modulus
    self._residue = residue
    self._current = None
    self._exhausted = False
    self._finalize_log = finalize_log
    self.closed = False

  def _read_next(self):
    while self._next < self._count:
      index = self._next
      self._next += 1
      if index % self._modulus == self._residue:
        self._current = index
        return True
    self._exhausted = True
    return False

  @override
  def start(self):
    return self._read_next()

  @override
  def advance(self):
    return self._read_next()

  @override
  def get_current(self):
    return self._current

  @override
  def get_current_timestamp(self):
    return _EVENT_TIME_BASE + self._current

  @override
  def get_watermark(self):
    if self._exhausted:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return _EVENT_TIME_BASE + self._current

  @override
  def get_checkpoint_mark(self):
    last = self._current if self._current is not None else self._next - 1
    return _CountingCheckpointMark(last, finalize_log=self._finalize_log)

  @override
  def close(self):
    self.closed = True


class UnboundedCountingSource(UnboundedSource):
  def __init__(
      self,
      count,
      finalize_log=None,
      is_splittable=False,
      modulus=1,
      residue=0):
    self._count = count
    self._finalize_log = finalize_log
    self._is_splittable = is_splittable
    self._modulus = modulus
    self._residue = residue
    self.last_reader = None

  @override
  def split(self, desired_num_splits, options=None):
    if not self._is_splittable or desired_num_splits < 2:
      return [self]
    # Split into independent even/odd sub-sources (each non-splittable).
    return [
        UnboundedCountingSource(
            self._count,
            finalize_log=self._finalize_log,
            modulus=2,
            residue=residue) for residue in (0, 1)
    ]

  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _CountingReader(
        self._count,
        start_index,
        finalize_log=self._finalize_log,
        modulus=self._modulus,
        residue=self._residue)
    return self.last_reader

  @override
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _StringCountingReader(_CountingReader):
  @override
  def get_current(self):
    return 'v%s' % self._current


class _StringCountingSource(UnboundedCountingSource):
  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _StringCountingReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  @override
  def default_output_coder(self):
    return coders.StrUtf8Coder()


class _PrefixStrCoder(coders.Coder):
  def __init__(self, prefix):
    self._prefix = prefix

  @override
  def encode(self, value):
    if not value.startswith(self._prefix):
      raise ValueError('expected %r prefix' % self._prefix)
    return value[len(self._prefix):].encode('utf-8')

  @override
  def decode(self, value):
    return self._prefix + value.decode('utf-8')

  @override
  def is_deterministic(self):
    return True

  @override
  def to_type_hint(self):
    return str


class _PrefixStringReader(_StringCountingReader):
  @override
  def get_current(self):
    return 'prefix:%s' % super().get_current()


class _PrefixStringSource(_StringCountingSource):
  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _PrefixStringReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  @override
  def default_output_coder(self):
    return _PrefixStrCoder('prefix:')


class _NoDataReader(UnboundedReader):
  """Always reports 'no data right now' (watermark < MAX, so never EOF)."""
  @override
  def start(self):
    return False

  @override
  def advance(self):
    return False

  @override
  def get_current(self):
    raise AssertionError('no data available')

  @override
  def get_current_timestamp(self):
    raise AssertionError('no data available')

  @override
  def get_watermark(self):
    return Timestamp(0)

  @override
  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(-1)


class _NoDataSource(UnboundedSource):
  @override
  def split(self, desired_num_splits, options=None):
    return [self]

  @override
  def create_reader(self, options, checkpoint_mark):
    return _NoDataReader()

  @override
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


def _new_tracker(source, checkpoint=None):
  restriction = _UnboundedSourceRestriction(
      source=source, checkpoint_mark=checkpoint)
  return _UnboundedSourceRestrictionTracker(restriction)


def _claim(tracker):
  """Claims once; returns (claimed_bool, holder_value)."""
  holder = [None]
  claimed = tracker.try_claim(holder)
  return claimed, holder[0]


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


class AbcContractTest(unittest.TestCase):
  def test_checkpointmark_default_finalize_is_noop(self):
    self.assertIsNone(CheckpointMark().finalize_checkpoint())

  def test_unboundedsource_is_bounded_false(self):
    self.assertFalse(UnboundedCountingSource(3).is_bounded())

  def test_reader_lifecycle_start_advance_eof(self):
    reader = UnboundedCountingSource(3).create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)
    self.assertEqual(reader.get_current_timestamp(), _EVENT_TIME_BASE)
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 1)
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 2)
    self.assertFalse(reader.advance())
    self.assertEqual(reader.get_watermark(), MAX_TIMESTAMP)


class RestrictionCoderTest(unittest.TestCase):
  def test_roundtrip_no_checkpoint(self):
    source = UnboundedCountingSource(3)
    coder = _UnboundedSourceRestrictionCoder()
    decoded = coder.decode(
        coder.encode(_UnboundedSourceRestriction(source=source)))
    self.assertIsNone(decoded.checkpoint_mark)
    self.assertEqual(decoded.watermark, MIN_TIMESTAMP)
    self.assertFalse(decoded.is_done)
    reader = decoded.source.create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)

  def test_roundtrip_with_checkpoint_resumes(self):
    source = UnboundedCountingSource(5)
    coder = _UnboundedSourceRestrictionCoder()
    restriction = _UnboundedSourceRestriction(
        source=source,
        checkpoint_mark=_CountingCheckpointMark(1),
        watermark=Timestamp(1),
        is_done=False)
    decoded = coder.decode(coder.encode(restriction))
    self.assertEqual(decoded.checkpoint_mark.last_index, 1)
    self.assertEqual(decoded.watermark, Timestamp(1))
    self.assertFalse(decoded.is_done)
    # A reader built from the decoded checkpoint resumes at the next index.
    reader = decoded.source.create_reader(None, decoded.checkpoint_mark)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 2)


class RestrictionProviderTest(unittest.TestCase):
  def test_initial_split_calls_source_split(self):
    split_log = []

    class _NamedSource(UnboundedCountingSource):
      def __init__(self, name):
        super().__init__(0)
        self.name = name

      @override
      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [_NamedSource('a'), _NamedSource('b')]

    source = _NamedSource('root')
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(
        source=source, watermark=Timestamp(7))

    splits = list(provider.split(source, restriction))

    # The provider is a stateless module-level singleton, so it always
    # passes ``None`` as the ``options`` argument to ``UnboundedSource.split``.
    self.assertEqual(split_log, [(20, None)])
    self.assertEqual([split.source.name for split in splits], ['a', 'b'])
    self.assertEqual([split.watermark for split in splits], [Timestamp(7)] * 2)
    self.assertTrue(all(split.checkpoint_mark is None for split in splits))
    self.assertTrue(
        all(split.finalization_checkpoint_mark is None for split in splits))

  def test_initial_split_does_not_split_checkpointed_restriction(self):
    split_log = []

    class _SplitSource(UnboundedCountingSource):
      @override
      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [self]

    source = _SplitSource(5)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(
        source=source, checkpoint_mark=_CountingCheckpointMark(2))

    self.assertEqual(list(provider.split(source, restriction)), [restriction])
    self.assertEqual(split_log, [])

  def test_initial_split_falls_back_to_original_on_split_error(self):
    class _BoomSource(UnboundedCountingSource):
      @override
      def split(self, desired_num_splits, options=None):
        raise RuntimeError('split boom')

    source = _BoomSource(5)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(source=source)

    self.assertEqual(list(provider.split(source, restriction)), [restriction])

  def test_splittable_source_partitions_into_independent_subsources(self):
    # A splittable source fans out into two sub-sources; reading each in
    # isolation yields the even and the odd integers, and their union is the
    # full sequence with no overlap.
    source = UnboundedCountingSource(6, is_splittable=True)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(source=source)

    splits = list(provider.split(source, restriction))
    self.assertEqual(len(splits), 2)

    shards = []
    for split in splits:
      tracker = _UnboundedSourceRestrictionTracker(split)
      shard = []
      while True:
        claimed, record = _claim(tracker)
        if not claimed:
          break
        if record is not _NO_DATA:
          shard.append(record[0])
      shards.append(shard)
    self.assertEqual(sorted(shards), [[0, 2, 4], [1, 3, 5]])


class RestrictionTrackerTest(unittest.TestCase):
  def test_claim_emits_in_order(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
    values = []
    while True:
      claimed, record = _claim(tracker)
      if not claimed:
        break
      self.assertIsNot(record, _NO_DATA)
      values.append(record[0])
    self.assertEqual(values, [0, 1, 2])
    self.assertTrue(tracker.check_done())

  def test_claim_emits_final_record_when_watermark_is_max(self):
    # A reader may return its last record with a MAX_TIMESTAMP watermark on the
    # same call; the record must still be emitted (EOF comes on the next claim).
    class _FinalRecordReader(UnboundedReader):
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'last'

      @override
      def get_current_timestamp(self):
        return Timestamp(0)

      @override
      def get_watermark(self):
        return MAX_TIMESTAMP

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _FinalSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _FinalRecordReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_FinalSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)
    self.assertIsNot(record, _NO_DATA)
    self.assertEqual(record[0], 'last')
    # The next claim observes EOF and finishes (no second, phantom record).
    claimed_again, _ = _claim(tracker)
    self.assertFalse(claimed_again)
    self.assertTrue(tracker.check_done())

  def test_try_split_zero_produces_resumable_residual(self):
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    # Claim 0 and 1.
    self.assertEqual(_claim(tracker)[1][0], 0)
    self.assertEqual(_claim(tracker)[1][0], 1)

    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    primary, residual = split
    self.assertTrue(primary.is_done)
    self.assertFalse(residual.is_done)
    # Resume / finalize channel separation: primary carries only the
    # finalize hook, residual carries only the resume state.
    self.assertIsNone(primary.checkpoint_mark)
    self.assertIsNotNone(primary.finalization_checkpoint_mark)
    self.assertEqual(primary.finalization_checkpoint_mark.last_index, 1)
    self.assertEqual(residual.checkpoint_mark.last_index, 1)
    self.assertIsNone(residual.finalization_checkpoint_mark)
    # check_done passes on the (now done) primary.
    self.assertTrue(tracker.check_done())

    # Resuming from the residual continues at index 2.
    resumed = _new_tracker(source, checkpoint=residual.checkpoint_mark)
    self.assertEqual(_claim(resumed)[1][0], 2)

  def test_try_split_nonzero_declined(self):
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    self.assertEqual(_claim(tracker)[1][0], 0)

    self.assertIsNone(tracker.try_split(0.5))
    self.assertFalse(tracker.current_restriction().is_done)
    self.assertIsNotNone(tracker._reader)
    self.assertEqual(_claim(tracker)[1][0], 1)

  def test_no_data_returns_sentinel_without_finishing(self):
    tracker = _new_tracker(_NoDataSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)  # not EOF
    self.assertIs(record, _NO_DATA)
    # A self-checkpoint is still possible (poll/resume path).
    self.assertIsNotNone(tracker.try_split(0))

  def test_check_done_raises_when_not_done(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_is_bounded_false(self):
    self.assertFalse(_new_tracker(UnboundedCountingSource(3)).is_bounded())


class WatermarkTest(unittest.TestCase):
  def test_set_watermark_is_monotonic(self):
    estimator = ManualWatermarkEstimator(None)
    _set_watermark_if_greater(estimator, Timestamp(5))
    self.assertEqual(estimator.current_watermark(), Timestamp(5))
    # A regression is ignored (would otherwise raise inside set_watermark).
    _set_watermark_if_greater(estimator, Timestamp(3))
    self.assertEqual(estimator.current_watermark(), Timestamp(5))
    _set_watermark_if_greater(estimator, Timestamp(7))
    self.assertEqual(estimator.current_watermark(), Timestamp(7))


class FinalizationTest(unittest.TestCase):
  def test_finalize_checkpoint_invoked(self):
    # Unit-level finalize test (the e2e finalize may run in a worker process);
    # the hook lives on the primary, independent of the residual's resume state.
    finalize_log = []
    source = UnboundedCountingSource(5, finalize_log=finalize_log)
    tracker = _new_tracker(source)
    _claim(tracker)  # 0
    _claim(tracker)  # 1
    primary, _ = tracker.try_split(0)
    primary.finalization_checkpoint_mark.finalize_checkpoint()
    self.assertEqual(finalize_log, [1])


class EndToEndTest(unittest.TestCase):
  def test_direct_runner_emits_all_in_order(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(UnboundedCountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_eof_lets_event_time_window_fire(self):
    # On EOF the DoFn advances the watermark estimator to MAX_TIMESTAMP so the
    # downstream FixedWindow closes and the GroupByKey fires; otherwise the
    # output would be empty.
    with TestPipeline() as p:
      out = (
          p
          | ReadFromUnboundedSource(UnboundedCountingSource(5))
          | beam.WindowInto(FixedWindows(100))
          | beam.Map(lambda v: ('all', v))
          | beam.GroupByKey()
          | beam.MapTuple(lambda _key, values: sorted(values)))
      assert_that(out, equal_to([[0, 1, 2, 3, 4]]))

  def test_read_dispatches_through_iobase_read(self):
    # ``beam.io.Read(source)`` must produce the same records as
    # ``ReadFromUnboundedSource(source)``.
    with TestPipeline() as p:
      out = p | beam.io.Read(UnboundedCountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_splittable_source_reads_all_records_across_splits(self):
    # A splittable source fans out into even/odd sub-sources during initial
    # SDF splitting; the union of all sub-source reads is the full sequence.
    with TestPipeline() as p:
      out = p | beam.io.Read(UnboundedCountingSource(6, is_splittable=True))
      assert_that(out, equal_to([0, 1, 2, 3, 4, 5]))

  def test_source_default_output_coder_sets_output_type(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(_StringCountingSource(2))
      self.assertEqual(out.element_type, str)
      assert_that(out, equal_to(['v0', 'v1']))


class ReadFromUnboundedSourceCoderTest(unittest.TestCase):
  def test_parameterized_output_coder_does_not_mutate_global_registry(self):
    try:
      p = beam.Pipeline()
      out = p | ReadFromUnboundedSource(_PrefixStringSource(1))

      self.assertNotEqual(out.element_type, str)
      self.assertEqual(coders.registry.get_coder(str), coders.StrUtf8Coder())
      self.assertEqual(
          ReadFromUnboundedSource(_PrefixStringSource(1))._infer_output_coder(),
          _PrefixStrCoder('prefix:'))
    finally:
      coders.registry.register_coder(str, coders.StrUtf8Coder)


# ------------------------------------------------------------------------------
# Reader lifecycle, watermark, and contract regression tests (reader close on
# every exit path, the NotImplementedError message, finalize idempotency).
# ------------------------------------------------------------------------------


class ReaderCloseTest(unittest.TestCase):
  """Reader lifecycle: close() must run on every tracker-driven exit path."""
  def test_tracker_closes_reader_on_eof(self):
    source = UnboundedCountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    self.assertIsNone(tracker._reader)
    self.assertTrue(source.last_reader.closed)

  def test_tracker_closes_reader_on_split(self):
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    _claim(tracker)  # creates reader, claims 0
    reader = source.last_reader
    self.assertFalse(reader.closed)
    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    self.assertIsNone(tracker._reader)
    self.assertTrue(reader.closed)

  def test_close_helper_is_idempotent_and_safe_on_empty_tracker(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
    # No reader yet -- helper must be a no-op.
    tracker._close_reader_if_open()
    _claim(tracker)
    reader = tracker._reader
    tracker._close_reader_if_open()
    self.assertTrue(reader.closed)
    self.assertIsNone(tracker._reader)
    # Second call is a no-op (no reader to close).
    tracker._close_reader_if_open()

  def test_close_helper_swallows_reader_close_errors(self):
    class _BoomReader(UnboundedReader):
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'x'

      @override
      def get_current_timestamp(self):
        return Timestamp(0)

      @override
      def get_watermark(self):
        return Timestamp(0)

      @override
      def get_checkpoint_mark(self):
        return CheckpointMark()

      @override
      def close(self):
        raise RuntimeError('close blew up')

    class _BoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_BoomSource())
    _claim(tracker)
    # Helper must not propagate the reader's close() exception, otherwise the
    # DoFn's finally / split paths would mask the original error.
    tracker._close_reader_if_open()
    self.assertIsNone(tracker._reader)


class TrackerContractRegressionTest(unittest.TestCase):
  """Tracker contract: source-watermark on the data path, finalize/resume
  channel separation, and reader close on a reader-method failure."""
  def test_data_path_holder_carries_source_watermark(self):
    class _LaggingReader(UnboundedReader):
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'rec'

      @override
      def get_current_timestamp(self):
        return Timestamp(1000)  # record event time

      @override
      def get_watermark(self):
        return Timestamp(990)  # source watermark lags 10us behind

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _LaggingSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _LaggingReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_LaggingSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)
    self.assertIsNot(record, _NO_DATA)
    value, record_timestamp, source_watermark = record
    self.assertEqual(value, 'rec')
    self.assertEqual(record_timestamp, Timestamp(1000))
    # Critical: watermark slot is the SOURCE watermark, NOT record timestamp.
    self.assertEqual(source_watermark, Timestamp(990))
    self.assertNotEqual(source_watermark, record_timestamp)

  def test_split_separates_finalize_and_resume_channels(self):
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    _claim(tracker)  # claim 0 so reader has progress
    primary, residual = tracker.try_split(0)
    # Primary carries ONLY the finalize hook -- no resume state.
    self.assertIsNone(primary.checkpoint_mark)
    self.assertIsNotNone(primary.finalization_checkpoint_mark)
    self.assertTrue(primary.is_done)
    # Residual carries ONLY the resume state -- no finalize hook (a future
    # bundle that splits THIS residual will produce ITS own finalize mark).
    self.assertIsNotNone(residual.checkpoint_mark)
    self.assertIsNone(residual.finalization_checkpoint_mark)
    self.assertFalse(residual.is_done)
    # The two marks reference the same underlying checkpoint object.
    self.assertEqual(
        primary.finalization_checkpoint_mark.last_index,
        residual.checkpoint_mark.last_index)

  def test_eof_populates_finalize_and_clears_resume(self):
    # EOF transition: restriction.checkpoint_mark goes to None (no more
    # records to resume from), finalization_checkpoint_mark carries the
    # final commit hook.
    source = UnboundedCountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    r = tracker.current_restriction()
    self.assertTrue(r.is_done)
    self.assertEqual(r.watermark, MAX_TIMESTAMP)
    self.assertIsNone(r.checkpoint_mark)
    self.assertIsNotNone(r.finalization_checkpoint_mark)

  def test_tracker_closes_reader_when_advance_raises(self):
    # try_claim closes the reader before re-raising a reader-method failure, so
    # the DoFn's finally need not traverse the SDF chain for these.
    class _BoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      @override
      def start(self):
        return True

      @override
      def advance(self):
        raise RuntimeError('advance boom')

      @override
      def get_current(self):
        return 'first'

      @override
      def get_current_timestamp(self):
        return Timestamp(0)

      @override
      def get_watermark(self):
        return Timestamp(0)

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      @override
      def close(self):
        self.closed = True

    class _BoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    src = _BoomSource()
    tracker = _new_tracker(src)
    # First claim succeeds (start returns True).
    self.assertTrue(tracker.try_claim([None]))
    reader_after_first = tracker._reader
    self.assertIsNotNone(reader_after_first)
    # The second claim's advance() raises; the tracker must close the reader
    # before propagating.
    with self.assertRaises(RuntimeError):
      tracker.try_claim([None])
    self.assertTrue(reader_after_first.closed)
    self.assertIsNone(tracker._reader)

  def test_tracker_closes_reader_when_get_watermark_raises(self):
    # Reader method failures other than advance() also trigger close.
    class _WatermarkBoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      @override
      def start(self):
        return False  # no data -> drops into get_watermark path

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        raise AssertionError

      @override
      def get_current_timestamp(self):
        raise AssertionError

      @override
      def get_watermark(self):
        raise RuntimeError('watermark boom')

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      @override
      def close(self):
        self.closed = True

    class _WatermarkBoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _WatermarkBoomReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    src = _WatermarkBoomSource()
    tracker = _new_tracker(src)
    with self.assertRaises(RuntimeError):
      tracker.try_claim([None])
    self.assertIsNone(tracker._reader)


class UnboundedSourceContractTest(unittest.TestCase):
  def test_get_checkpoint_mark_coder_default_names_subclass(self):
    class MySource(UnboundedSource):
      pass

    with self.assertRaises(NotImplementedError) as cm:
      MySource().get_checkpoint_mark_coder()
    self.assertIn('MySource', str(cm.exception))

  def test_default_finalize_is_idempotent(self):
    mark = CheckpointMark()
    # Default no-op must tolerate repeated invocation; the SDK's bundle
    # finalizer makes no exactly-once guarantee on this callback.
    self.assertIsNone(mark.finalize_checkpoint())
    self.assertIsNone(mark.finalize_checkpoint())


class ReadFromUnboundedSourceValidationTest(unittest.TestCase):
  def test_non_source_argument_raises(self):
    with self.assertRaises(TypeError):
      ReadFromUnboundedSource('not-a-source')  # type: ignore[arg-type]


class StdlibPicklabilityTest(unittest.TestCase):
  """``_ReadFromUnboundedSourceDoFn`` and ``_PROVIDER`` are module-level (not
  nested in ``ReadFromUnboundedSource.expand``) specifically so stdlib pickle --
  not just cloudpickle -- can serialise them.
  """
  def test_module_level_dofn_round_trips_through_stdlib_pickle(self):
    restored = pickle.loads(
        pickle.dumps(_unbounded_source_module._ReadFromUnboundedSourceDoFn()))
    self.assertIsInstance(
        restored, _unbounded_source_module._ReadFromUnboundedSourceDoFn)

  def test_module_level_provider_round_trips_through_stdlib_pickle(self):
    restored = pickle.loads(pickle.dumps(_unbounded_source_module._PROVIDER))
    self.assertIsInstance(restored, _UnboundedSourceRestrictionProvider)


class CircularImportOrderTest(unittest.TestCase):
  """`iobase.py` and `unbounded_source.py` form a cycle (UnboundedSource extends
  iobase.SourceBase; iobase.Read.expand lazy-imports unbounded_source). All
  three import-order scenarios must complete without ImportError. Subprocesses
  ensure each test starts from a clean module cache.
  """
  def _run_in_subprocess(self, script):
    import os
    import subprocess
    import sys
    env = os.environ.copy()
    beam_python = os.path.join(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    env['PYTHONPATH'] = beam_python + os.pathsep + env.get('PYTHONPATH', '')
    fd, path = tempfile.mkstemp(suffix='.py')
    try:
      with os.fdopen(fd, 'w') as fp:
        fp.write(script)
      return subprocess.run([sys.executable, path],
                            capture_output=True,
                            check=False,
                            text=True,
                            env=env,
                            timeout=60)
    finally:
      if os.path.exists(path):
        os.unlink(path)

  def test_iobase_then_unbounded_source(self):
    result = self._run_in_subprocess(
        'import apache_beam.io.iobase\n'
        'import apache_beam.io.unbounded_source\n'
        'print("ok")\n')
    self.assertEqual(
        result.returncode,
        0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)

  def test_unbounded_source_then_iobase(self):
    result = self._run_in_subprocess(
        'import apache_beam.io.unbounded_source\n'
        'import apache_beam.io.iobase\n'
        'print("ok")\n')
    self.assertEqual(
        result.returncode,
        0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)

  def test_read_expand_lazy_imports_unbounded_source(self):
    # Import iobase, then Read.expand() on an UnboundedSource must lazy-import
    # unbounded_source without ImportError.
    script = '''
import sys
import apache_beam as beam
from apache_beam import coders
import apache_beam.io.iobase as iobase
# Now import unbounded_source AFTER iobase, then verify Read.expand
# successfully lazy-imports ReadFromUnboundedSource:
from apache_beam.io.unbounded_source import UnboundedSource

class _S(UnboundedSource):
  def split(self, n, options=None):
    return [self]
  def create_reader(self, o, cp):
    return None
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()

r = iobase.Read(_S())
p = beam.Pipeline()
result = r.expand(p)
assert not result.is_bounded, 'expanded PCollection should be unbounded'
print("ok")
'''
    result = self._run_in_subprocess(script)
    self.assertEqual(
        result.returncode,
        0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
