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
"""Unit tests for the Python UnboundedSource SDF wrapper in iobase.py."""

# pytype: skip-file

import logging
import unittest
from unittest import mock

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io.iobase import BACKLOG_UNKNOWN
from apache_beam.io.iobase import CheckpointMark
from apache_beam.io.iobase import UnboundedReader
from apache_beam.io.iobase import UnboundedSource
from apache_beam.io.iobase import _NoopCheckpointMark
from apache_beam.io.iobase import _SDFUnboundedSourceRestriction
from apache_beam.io.iobase import _SDFUnboundedSourceRestrictionTracker
from apache_beam.io.iobase import _SDFUnboundedSourceWrapperFn
from apache_beam.io.iobase import _SDFUnboundedSourceWrapperRestrictionCoder
from apache_beam.io.iobase import _UnboundedSourceAsRead
from apache_beam.io.iobase import _UnboundedSourceWatermarkEstimatorProvider
from apache_beam.runners.common import DoFnInvoker
from apache_beam.runners.direct import sdf_direct_runner
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


# ---------------------------------------------------------------------------
# Test helpers: a minimal in-memory UnboundedSource the wrapper can drive.
# ---------------------------------------------------------------------------


class _CountingCheckpointMark(CheckpointMark):
  """A pickleable checkpoint mark tracking the next offset to read.

  Also carries the remaining number of "no data right now" pauses so the
  pause/resume test does not get stuck looping with a fresh pause budget
  every time the reader is reconstructed.
  """

  def __init__(
      self, next_offset=0, pauses_remaining=None, finalize_callback=None):
    self.next_offset = next_offset
    self.pauses_remaining = pauses_remaining
    self._finalize_callback = finalize_callback
    self.finalized = False

  def finalize_checkpoint(self):
    self.finalized = True
    if self._finalize_callback is not None:
      self._finalize_callback()

  def __reduce__(self):
    # The finalize callback is intentionally dropped on pickling; runners may
    # ship the mark to a separate worker for finalization.
    return (
        _CountingCheckpointMark,
        (self.next_offset, self.pauses_remaining, None))

  def __eq__(self, other):
    return (
        isinstance(other, _CountingCheckpointMark) and
        self.next_offset == other.next_offset)

  def __hash__(self):
    return hash(self.next_offset)


class _CountingReader(UnboundedReader):
  """Reads integers in ``[start, start + count)`` from a counting source.

  Optionally pauses partway through by returning ``False`` from ``advance``
  for a single tick; this lets us exercise the wrapper's ``defer_remainder``
  path without depending on real wall-clock timing.
  """

  def __init__(self, source, checkpoint_mark):
    self._source = source
    if checkpoint_mark is not None:
      self._next = checkpoint_mark.next_offset
      self._pauses_remaining = (
          checkpoint_mark.pauses_remaining
          if checkpoint_mark.pauses_remaining is not None
          else source.pause_count)
    else:
      self._next = source.start
      self._pauses_remaining = source.pause_count
    self._stop = source.start + source.count
    self._current = None
    self._pause_at = source.pause_at
    self.closed = False

  def start(self):
    return self.advance()

  def advance(self):
    if self._next >= self._stop:
      self._current = None
      return False
    if (self._pause_at is not None and self._next == self._pause_at and
        self._pauses_remaining > 0):
      self._pauses_remaining -= 1
      return False
    self._current = self._next
    self._next += 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(seconds=self._current)

  def get_current_record_id(self):
    return ('rec-%d' % self._current).encode('utf-8')

  def get_watermark(self):
    # The watermark is the timestamp of the last emitted record (or MIN if
    # we have not emitted anything yet). Once the source is exhausted we
    # report MAX_TIMESTAMP so the wrapper transitions to "done" instead of
    # spinning in a defer/resume loop.
    if self._next >= self._stop and self._pauses_remaining == 0:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return Timestamp(seconds=self._current)

  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(self._next, self._pauses_remaining)

  def get_split_backlog_bytes(self):
    return max(0, self._stop - self._next) * 8

  def close(self):
    self.closed = True


class _CountingUnboundedSource(UnboundedSource):
  """In-memory unbounded source emitting integers ``[start, start + count)``.

  ``pause_at`` and ``pause_count`` let tests force the reader to return
  ``False`` from ``advance`` at a configured offset so we exercise the
  wrapper's "no data right now" path. ``num_splits`` controls how many
  sub-sources :meth:`split` returns.
  """

  def __init__(
      self,
      count,
      start=0,
      num_splits=1,
      requires_dedup=False,
      pause_at=None,
      pause_count=0,
  ):
    self.start = start
    self.count = count
    self.num_splits = num_splits
    self._requires_dedup = requires_dedup
    self.pause_at = pause_at
    self.pause_count = pause_count

  def split(self, desired_num_splits, options=None):
    splits_to_make = max(1, min(desired_num_splits, self.num_splits))
    if splits_to_make == 1 or self.count == 0:
      return [self]
    chunk = max(1, self.count // splits_to_make)
    splits = []
    offset = self.start
    for i in range(splits_to_make):
      if i == splits_to_make - 1:
        sub_count = self.start + self.count - offset
      else:
        sub_count = chunk
      if sub_count <= 0:
        break
      splits.append(
          _CountingUnboundedSource(
              count=sub_count,
              start=offset,
              num_splits=1,
              requires_dedup=self._requires_dedup,
              pause_at=self.pause_at,
              pause_count=self.pause_count))
      offset += sub_count
    return splits

  def create_reader(self, options, checkpoint_mark):
    return _CountingReader(self, checkpoint_mark)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()

  def requires_deduping(self):
    return self._requires_dedup

  def default_output_coder(self):
    return coders.VarIntCoder()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class CheckpointMarkContractTest(unittest.TestCase):

  def test_default_finalize_is_noop(self):
    mark = _NoopCheckpointMark()
    # Should not raise.
    mark.finalize_checkpoint()

  def test_get_offset_limit_default_raises(self):

    class _Mark(CheckpointMark):
      def finalize_checkpoint(self):
        pass

    with self.assertRaises(NotImplementedError):
      _Mark().get_offset_limit()

  def test_finalize_invokes_callback(self):
    calls = []
    mark = _CountingCheckpointMark(
        next_offset=3, finalize_callback=lambda: calls.append('done'))
    mark.finalize_checkpoint()
    self.assertTrue(mark.finalized)
    self.assertEqual(calls, ['done'])


class UnboundedSourceContractTest(unittest.TestCase):

  def test_is_unbounded(self):
    self.assertFalse(_CountingUnboundedSource(count=3).is_bounded())

  def test_requires_deduping_defaults_false(self):
    self.assertFalse(_CountingUnboundedSource(count=3).requires_deduping())

  def test_offset_based_deduplication_defaults_false(self):
    self.assertFalse(
        _CountingUnboundedSource(count=3).offset_based_deduplication_supported())

  def test_split_returns_at_least_one(self):
    source = _CountingUnboundedSource(count=10, num_splits=4)
    splits = source.split(desired_num_splits=4)
    self.assertEqual(len(splits), 4)
    # The total count must be preserved by splitting.
    self.assertEqual(sum(s.count for s in splits), 10)

  def test_reader_default_record_id_is_empty(self):

    class _Reader(UnboundedReader):
      def start(self):
        return False

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return MIN_TIMESTAMP

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        pass

    self.assertEqual(_Reader().get_current_record_id(), b'')
    self.assertEqual(_Reader().get_split_backlog_bytes(), BACKLOG_UNKNOWN)
    self.assertEqual(_Reader().get_total_backlog_bytes(), BACKLOG_UNKNOWN)


class SDFUnboundedSourceRestrictionTest(unittest.TestCase):

  def test_default_watermark_is_min(self):
    source = _CountingUnboundedSource(count=3)
    r = _SDFUnboundedSourceRestriction(source)
    self.assertEqual(r.watermark, MIN_TIMESTAMP)
    self.assertIsNone(r.checkpoint_mark)
    self.assertFalse(r.is_done)

  def test_pickle_round_trip(self):
    import pickle
    source = _CountingUnboundedSource(count=3, start=1)
    mark = _CountingCheckpointMark(7)
    r = _SDFUnboundedSourceRestriction(
        source, checkpoint_mark=mark, watermark=Timestamp(seconds=5))
    round_tripped = pickle.loads(pickle.dumps(r))
    self.assertEqual(round_tripped.watermark, Timestamp(seconds=5))
    self.assertEqual(round_tripped.checkpoint_mark, mark)
    self.assertEqual(round_tripped.source.count, 3)
    self.assertEqual(round_tripped.source.start, 1)


class SDFUnboundedSourceWrapperRestrictionCoderTest(unittest.TestCase):

  def test_round_trip_with_checkpoint(self):
    coder = _SDFUnboundedSourceWrapperRestrictionCoder(coders.PickleCoder())
    source = _CountingUnboundedSource(count=5)
    mark = _CountingCheckpointMark(2)
    r = _SDFUnboundedSourceRestriction(
        source, checkpoint_mark=mark, watermark=Timestamp(seconds=3))
    decoded = coder.decode(coder.encode(r))
    self.assertEqual(decoded.watermark, Timestamp(seconds=3))
    self.assertEqual(decoded.checkpoint_mark, mark)
    self.assertEqual(decoded.source.count, 5)
    self.assertFalse(decoded.is_done)

  def test_round_trip_without_checkpoint(self):
    coder = _SDFUnboundedSourceWrapperRestrictionCoder()
    source = _CountingUnboundedSource(count=2)
    r = _SDFUnboundedSourceRestriction(source)
    decoded = coder.decode(coder.encode(r))
    self.assertIsNone(decoded.checkpoint_mark)
    self.assertEqual(decoded.watermark, MIN_TIMESTAMP)

  def test_null_tag_disambiguates_empty_encoding(self):
    # If a user-supplied checkpoint coder encodes to empty bytes, the null
    # tag must still preserve the non-None checkpoint mark.
    class _EmptyEncodingCoder(coders.Coder):
      def encode(self, mark):
        return b''

      def decode(self, data):
        return _CountingCheckpointMark(next_offset=99)

      def is_deterministic(self):
        return False

    rc = _SDFUnboundedSourceWrapperRestrictionCoder(_EmptyEncodingCoder())
    source = _CountingUnboundedSource(count=1)
    r = _SDFUnboundedSourceRestriction(
        source, checkpoint_mark=_CountingCheckpointMark(7))
    decoded = rc.decode(rc.encode(r))
    self.assertIsNotNone(decoded.checkpoint_mark)
    self.assertEqual(decoded.checkpoint_mark.next_offset, 99)


class SDFUnboundedSourceRestrictionTrackerTest(unittest.TestCase):

  def _make(self, count=5, **kwargs):
    source = _CountingUnboundedSource(count=count, **kwargs)
    restriction = _SDFUnboundedSourceRestriction(source)
    return source, _SDFUnboundedSourceRestrictionTracker(restriction)

  def test_try_claim_emits_value(self):
    # try_claim now performs the read under the tracker lock and fills the
    # passed-in mutable container with a _UnboundedSourceValue (or None).
    _, tracker = self._make(count=2)
    out = [None]
    self.assertTrue(tracker.try_claim(out))
    self.assertIsNotNone(out[0])
    self.assertEqual(out[0].value, 0)

  def test_try_claim_returns_none_when_no_data_and_not_terminal(self):
    # A reader with no data right now but watermark < MAX_TIMESTAMP
    # should return True with out[0]=None so the wrapper defers.

    class _IdleReader(UnboundedReader):
      def start(self):
        return False

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return Timestamp(seconds=5)  # finite, not MAX

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        pass

    class _IdleSrc(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _IdleReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_IdleSrc())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    out = [None]
    # True + None means "no data, defer me" (NOT terminal).
    self.assertTrue(tracker.try_claim(out))
    self.assertIsNone(out[0])
    # Watermark was refreshed inside the lock.
    self.assertEqual(tracker.last_watermark(), Timestamp(seconds=5))

  def test_try_claim_after_done_is_false(self):
    source = _CountingUnboundedSource(count=2)
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    self.assertFalse(tracker.try_claim([None]))

  def test_try_claim_rejects_missing_record_id_when_dedup_required(self):
    source = _CountingUnboundedSource(count=1, requires_dedup=True)
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)

    class _BadReader(UnboundedReader):
      def __init__(self):
        self._emitted = False

      def start(self):
        self._emitted = True
        return True

      def advance(self):
        return False

      def get_current(self):
        return 'v'

      def get_current_timestamp(self):
        return Timestamp(seconds=1)

      def get_current_record_id(self):
        return b''  # missing id

      def get_watermark(self):
        return Timestamp(seconds=1)

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        pass

    # Force the tracker to use a reader with a missing record id.
    tracker._reader = _BadReader()
    tracker._reader_started = False
    out = [None]
    with self.assertRaisesRegex(ValueError, 'requires deduping'):
      tracker.try_claim(out)

  def test_try_split_before_start_returns_none(self):
    _, tracker = self._make()
    self.assertIsNone(tracker.try_split(0.5))

  def test_try_split_after_start_returns_residual_with_fresh_checkpoint(self):
    source, tracker = self._make(count=10)
    out = [None]
    tracker.try_claim(out)
    reader = tracker.reader()
    self.assertIsNotNone(reader)
    result = tracker.try_split(0.5)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertTrue(primary.is_done)
    self.assertFalse(residual.is_done)
    # Residual carries the most recent checkpoint, not the original None.
    self.assertIsNotNone(residual.checkpoint_mark)
    self.assertEqual(residual.checkpoint_mark.next_offset, 1)
    self.assertIs(residual.source, source)
    # The split must close the live reader; further try_split is a no-op.
    self.assertTrue(reader.closed)
    self.assertIsNone(tracker.try_split(0.5))

  def test_try_split_caches_checkpoint_for_finalization(self):
    _, tracker = self._make(count=10)
    tracker.try_claim([None])
    self.assertIsNone(tracker.last_checkpoint_mark())
    tracker.try_split(0.5)
    self.assertIsNotNone(tracker.last_checkpoint_mark())

  def test_current_restriction_snapshots_latest_checkpoint(self):
    # Once the reader has advanced, current_restriction() must reflect the
    # reader's latest get_checkpoint_mark() so defer/split residuals
    # resume from the most recent position rather than the original.
    _, tracker = self._make(count=4)
    tracker.try_claim([None])
    tracker.try_claim([None])
    snap = tracker.current_restriction()
    self.assertIsNotNone(snap.checkpoint_mark)
    self.assertEqual(snap.checkpoint_mark.next_offset, 2)

  def test_update_watermark_is_monotonic(self):
    _, tracker = self._make()
    tracker.update_watermark(Timestamp(seconds=5))
    tracker.update_watermark(Timestamp(seconds=3))
    self.assertEqual(tracker.last_watermark(), Timestamp(seconds=5))

  def test_check_done_is_permissive(self):
    # Unbounded sources have no natural "all work done" point; check_done
    # must accept all exits so it does not mask legitimate process()
    # exceptions or break runners with their own SDF stop semantics.
    _, tracker = self._make(count=4)
    tracker.check_done()  # should not raise

  def test_check_done_passes_after_mark_done(self):
    _, tracker = self._make(count=1)
    tracker.try_claim([None])
    tracker.mark_done()
    tracker.check_done()  # should not raise

  def test_check_done_passes_after_try_split(self):
    _, tracker = self._make(count=4)
    tracker.try_claim([None])
    tracker.try_split(0)
    tracker.check_done()  # should not raise

  def test_is_unbounded(self):
    _, tracker = self._make()
    self.assertFalse(tracker.is_bounded())

  def test_progress_uses_backlog_when_available(self):
    _, tracker = self._make(count=4)
    tracker.try_claim([None])
    progress = tracker.current_progress()
    # Backlog after one claim is 3 * 8 bytes = 24 remaining.
    self.assertEqual(progress.remaining_work, 24.0)


class SDFUnboundedSourceWrapperFnAsProviderTest(unittest.TestCase):
  """Verifies the DoFn itself implements the RestrictionProvider protocol
  so the user-supplied checkpoint mark coder is wired through."""

  def test_initial_restriction_wraps_source(self):
    wrapper = _SDFUnboundedSourceWrapperFn()
    source = _CountingUnboundedSource(count=3)
    r = wrapper.initial_restriction(source)
    self.assertIs(r.source, source)
    self.assertIsNone(r.checkpoint_mark)
    self.assertEqual(r.watermark, MIN_TIMESTAMP)

  def test_initial_restriction_rejects_non_source(self):
    wrapper = _SDFUnboundedSourceWrapperFn()
    with self.assertRaises(TypeError):
      wrapper.initial_restriction('not-a-source')

  def test_split_delegates_to_source(self):
    wrapper = _SDFUnboundedSourceWrapperFn(desired_num_splits=4)
    source = _CountingUnboundedSource(count=12, num_splits=4)
    restriction = wrapper.initial_restriction(source)
    splits = list(wrapper.split(source, restriction))
    self.assertEqual(len(splits), 4)
    self.assertTrue(
        all(s.source.count + s.source.start <= 12 for s in splits))

  def test_split_after_checkpoint_yields_same_restriction(self):
    wrapper = _SDFUnboundedSourceWrapperFn()
    source = _CountingUnboundedSource(count=10)
    r = _SDFUnboundedSourceRestriction(
        source, checkpoint_mark=_CountingCheckpointMark(3))
    splits = list(wrapper.split(source, r))
    self.assertEqual(splits, [r])

  def test_restriction_coder_uses_supplied_checkpoint_coder(self):
    # If we pass a sentinel coder we should see it round-tripped in the
    # restriction coder, proving the wiring works.
    class _SentinelCoder(coders.Coder):
      encoded = []

      def encode(self, mark):
        _SentinelCoder.encoded.append(mark)
        return b'SENTINEL'

      def decode(self, data):
        return _CountingCheckpointMark(next_offset=42)

      def is_deterministic(self):
        return False

    wrapper = _SDFUnboundedSourceWrapperFn(
        checkpoint_mark_coder=_SentinelCoder())
    rc = wrapper.restriction_coder()
    source = _CountingUnboundedSource(count=1)
    r = _SDFUnboundedSourceRestriction(
        source,
        checkpoint_mark=_CountingCheckpointMark(5),
        watermark=Timestamp(seconds=2))
    decoded = rc.decode(rc.encode(r))
    self.assertEqual(decoded.checkpoint_mark.next_offset, 42)
    self.assertEqual(len(_SentinelCoder.encoded), 1)


class UnboundedSourceWatermarkEstimatorProviderTest(unittest.TestCase):

  def test_initial_state_uses_restriction_watermark(self):
    provider = _UnboundedSourceWatermarkEstimatorProvider()
    restriction = _SDFUnboundedSourceRestriction(
        _CountingUnboundedSource(count=1), watermark=Timestamp(seconds=2))
    self.assertEqual(
        provider.initial_estimator_state(None, restriction),
        Timestamp(seconds=2))

  def test_estimator_clamps_to_min_max(self):
    provider = _UnboundedSourceWatermarkEstimatorProvider()
    # Crafted out-of-range state to confirm clamping; should not raise.
    estimator = provider.create_watermark_estimator(
        Timestamp(micros=MAX_TIMESTAMP.micros + 1))
    self.assertEqual(estimator.current_watermark(), MAX_TIMESTAMP)


class UnboundedSourceAsReadTest(unittest.TestCase):
  """End-to-end tests for ``Read(UnboundedSource)``.

  These tests rely on :class:`_CountingUnboundedSource` reporting
  ``MAX_TIMESTAMP`` from ``get_watermark()`` once exhausted; that signal
  causes the SDF wrapper to mark the restriction as done instead of
  spinning forever in defer/resume cycles.
  """

  def test_rejects_non_unbounded_source(self):
    with self.assertRaises(TypeError):
      _UnboundedSourceAsRead('not a source')

  def test_read_dispatches_to_unbounded_path(self):
    # Sanity-check that Read.expand picks the unbounded wrapper for an
    # UnboundedSource without running the pipeline. Building TestPipeline
    # without entering the context manager skips the implicit ``run()`` so
    # this stays a graph-construction test.
    transform = iobase.Read(_CountingUnboundedSource(count=3))
    p = TestPipeline()
    result = transform.expand(p)
    self.assertIsNotNone(result)
    # The result must be an unbounded PCollection.
    self.assertFalse(result.is_bounded)

  def test_end_to_end_emits_records_in_order(self):
    with TestPipeline() as p:
      actual = p | iobase.Read(_CountingUnboundedSource(count=5))
      assert_that(actual, equal_to([0, 1, 2, 3, 4]))

  def test_end_to_end_handles_pause_and_resume(self):
    # Force the reader to return False from advance() once mid-stream to
    # exercise the wrapper's defer_remainder path.
    source = _CountingUnboundedSource(count=4, pause_at=2, pause_count=1)
    with TestPipeline() as p:
      actual = p | iobase.Read(source)
      assert_that(actual, equal_to([0, 1, 2, 3]))

  def test_end_to_end_dedup_strips_record_id(self):
    # With requires_deduping=True the wrapper emits (record_id, value)
    # tuples, the dedup transform collapses duplicates, and StripRecordId
    # hands plain values downstream. The "happy path" assertion (unique
    # values pass through unchanged) lives here; the actual dedup
    # collapsing is verified in
    # ``test_end_to_end_dedup_collapses_duplicate_record_ids``.
    source = _CountingUnboundedSource(count=3, requires_dedup=True)
    with TestPipeline() as p:
      actual = p | iobase.Read(source)
      assert_that(actual, equal_to([0, 1, 2]))

  def test_end_to_end_dedup_collapses_duplicate_record_ids(self):
    # Feeds five records whose record ids repeat: rec-A, rec-B, rec-A,
    # rec-C, rec-B. After DeduplicatePerKey collapses by record id, only
    # the three unique values must reach downstream. This is the
    # regression test that proves the typed KV plumbing into the stateful
    # dedup DoFn actually keys state per record id.
    source = _DuplicateRecordIdSource()
    with TestPipeline() as p:
      actual = p | iobase.Read(source)
      assert_that(actual, equal_to([10, 20, 30]))

  def test_dedup_window_is_overridable(self):
    transform = _UnboundedSourceAsRead(
        _CountingUnboundedSource(count=1, requires_dedup=True),
        dedup_duration_seconds=42.0)
    self.assertEqual(transform._dedup_duration_seconds, 42.0)


# ---------------------------------------------------------------------------
# Helpers for the duplicate-record-id regression test above.
# ---------------------------------------------------------------------------


class _DuplicateRecordIdReader(UnboundedReader):
  """Emits a fixed sequence with repeating record ids so that the dedup
  transform must actually collapse them."""
  _PAIRS = (
      (10, b'rec-A'),
      (20, b'rec-B'),
      (10, b'rec-A'),  # duplicate of the first
      (30, b'rec-C'),
      (20, b'rec-B'),  # duplicate of the second
  )

  def __init__(self):
    self._idx = -1

  def start(self):
    return self.advance()

  def advance(self):
    self._idx += 1
    return self._idx < len(self._PAIRS)

  def get_current(self):
    return self._PAIRS[self._idx][0]

  def get_current_timestamp(self):
    return Timestamp(seconds=self._PAIRS[self._idx][0])

  def get_current_record_id(self):
    return self._PAIRS[self._idx][1]

  def get_watermark(self):
    if self._idx >= len(self._PAIRS) - 1:
      return MAX_TIMESTAMP
    if self._idx < 0:
      return MIN_TIMESTAMP
    return Timestamp(seconds=self._PAIRS[self._idx][0])

  def get_checkpoint_mark(self):
    return _NoopCheckpointMark()

  def close(self):
    pass


class _DuplicateRecordIdSource(UnboundedSource):
  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    return _DuplicateRecordIdReader()

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()

  def requires_deduping(self):
    return True

  def default_output_coder(self):
    return coders.registry.get_coder(int)


class _MinimalUnboundedSource(UnboundedSource):
  """The smallest UnboundedSource that satisfies the documented contract.

  Implements only :meth:`split`, :meth:`create_reader`, and
  :meth:`get_checkpoint_mark_coder` -- the three methods the
  :class:`UnboundedSource` docstring lists as required. Crucially does
  NOT override :meth:`default_output_coder`; the inherited
  :class:`UnboundedSource` default must be enough for expand-time use.
  """
  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    # The regression test only drives pipeline construction (expand), which
    # never calls create_reader. Raise the base-class error type so the
    # fixture stays consistent with the documented contract while still
    # surfacing any accidental call during expand() as a clear failure.
    raise NotImplementedError(
        '_MinimalUnboundedSource.create_reader should not be called during '
        'pipeline expansion; this regression test only exercises expand().')

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class DefaultOutputCoderRegressionTest(unittest.TestCase):
  """Guards against the regression where ``UnboundedSource`` did not
  override :meth:`default_output_coder` and ``_UnboundedSourceAsRead.expand``
  blew up at construction time on minimal user sources."""
  def test_minimal_user_source_expands_without_overriding_default_output_coder(
      self):
    # If UnboundedSource silently loses the default coder again, this line
    # raises NotImplementedError from SourceBase.default_output_coder.
    transform = _UnboundedSourceAsRead(_MinimalUnboundedSource())
    p = TestPipeline()
    output = transform.expand(p)
    self.assertIsNotNone(output)

  def test_unbounded_source_default_coder_matches_bounded_default(self):
    # Both base classes should fall back to the SAME default coder so that
    # subclasses which only implement the documented contract keep working
    # regardless of bounded/unbounded.
    from apache_beam.io.iobase import BoundedSource

    class _MinimalBoundedSource(BoundedSource):
      pass

    self.assertEqual(
        type(_MinimalUnboundedSource().default_output_coder()),
        type(_MinimalBoundedSource().default_output_coder()))


class MissingRecordIdErrorTest(unittest.TestCase):
  """Verifies the wrapper errors loudly when ``requires_deduping`` is on but
  the reader did not override ``get_current_record_id``.

  Without the runtime check this would silently collapse the whole stream
  into a single record because every element would get the empty record id.
  """

  def test_missing_record_id_raises(self):

    class _BadReader(UnboundedReader):
      def __init__(self):
        self._emitted = False

      def start(self):
        self._emitted = True
        return True

      def advance(self):
        return False

      def get_current(self):
        return 'v'

      def get_current_timestamp(self):
        return Timestamp(seconds=1)

      def get_watermark(self):
        return MAX_TIMESTAMP if not self._emitted else Timestamp(seconds=1)

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        pass

      # Intentionally inherits the empty-byte default for get_current_record_id.

    class _BadSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _BadReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def requires_deduping(self):
        return True

      def default_output_coder(self):
        return coders.StrUtf8Coder()

    with self.assertRaisesRegex(Exception, 'requires deduping'):
      with TestPipeline() as p:
        _ = p | iobase.Read(_BadSource())


def _drive_process(restriction):
  """Runs ``_SDFUnboundedSourceWrapperFn.process`` end-to-end against the
  given restriction with mock collaborators and returns (finalizer, tracker).

  Module-level helper so tests across multiple classes can invoke the
  wrapper without reaching into another test class's bound methods.
  """
  from apache_beam.io.watermark_estimators import ManualWatermarkEstimator

  tracker = _SDFUnboundedSourceRestrictionTracker(restriction)

  class _FakeView:
    def __init__(self, t):
      self._threadsafe_restriction_tracker = ThreadsafeRestrictionTracker(t)

    def try_claim(self, position):
      return self._threadsafe_restriction_tracker.try_claim(position)

    def defer_remainder(self, delay=None):
      self._threadsafe_restriction_tracker.defer_remainder(delay)

    def current_restriction(self):
      return self._threadsafe_restriction_tracker.current_restriction()

  class _FakeFinalizer:
    def __init__(self):
      self.callbacks = []

    def register(self, cb):
      self.callbacks.append(cb)

  fn = _SDFUnboundedSourceWrapperFn(coders.PickleCoder())
  finalizer = _FakeFinalizer()
  view = _FakeView(tracker)
  estimator = ManualWatermarkEstimator(MIN_TIMESTAMP)

  list(
      fn.process(
          element=restriction.source,
          bundle_finalizer=finalizer,
          restriction_tracker=view,
          watermark_estimator=estimator))
  return finalizer, tracker


class BundleFinalizationTest(unittest.TestCase):
  """Verifies the wrapper hands a finalizer to the bundle finalizer when the
  reader produces a meaningful checkpoint mark.

  We invoke the wrapper's ``process`` directly (via the module-level
  ``_drive_process`` helper) with mock collaborators so we do not depend
  on the runner-specific timing of bundle finalization, while still
  covering the registration path.
  """

  def test_finalizer_registered_first_bundle_from_initial_none(self):
    # Starting from None checkpoint, emitting one record, and reaching the
    # MAX_TIMESTAMP termination still needs to register the new checkpoint
    # for finalization (this is the H4 regression Codex flagged).
    source = _CountingUnboundedSource(count=1)
    restriction = _SDFUnboundedSourceRestriction(source)
    finalizer, _ = _drive_process(restriction)
    self.assertEqual(len(finalizer.callbacks), 1)
    finalizer.callbacks[0]()  # must not raise

  def test_finalizer_registered_for_non_noop_mark(self):
    source = _CountingUnboundedSource(count=1)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint_mark=_CountingCheckpointMark(next_offset=0))
    finalizer, _ = _drive_process(restriction)
    self.assertEqual(len(finalizer.callbacks), 1)
    finalizer.callbacks[0]()

  def test_finalizer_skipped_for_noop_checkpoint_mark(self):
    # An idle source whose reader hands back a ``_NoopCheckpointMark``
    # never registers a finalizer: the wrapper's
    # ``isinstance(ckpt, _NoopCheckpointMark)`` guard in the ``finally``
    # block intentionally suppresses registration for marks that have no
    # side effects to commit.

    class _IdleSource(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _IdleReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    class _IdleReader(UnboundedReader):
      def start(self):
        return False

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return MIN_TIMESTAMP  # not MAX, so the wrapper will defer

      def get_checkpoint_mark(self):
        return self._mark

      _mark = _NoopCheckpointMark()

      def close(self):
        pass

    src = _IdleSource()
    restriction = _SDFUnboundedSourceRestriction(src)
    finalizer, _ = _drive_process(restriction)
    # The mark is the NoOp instance, so no finalizer is registered even
    # though the defer path did snapshot a checkpoint.
    self.assertEqual(len(finalizer.callbacks), 0)


class LockGuardedTerminalTransitionTest(unittest.TestCase):
  """Regression tests for the in-lock terminal-transition contract.

  The terminal transition (``mark_done`` + final-checkpoint snapshot +
  close) is performed inside ``try_claim`` so all reader/tracker mutations
  run under the SDF threadsafe lock. The wrapper detects this via
  ``try_claim`` returning ``False`` with ``out[0] == None`` (versus
  ``True`` with ``out[0] == None`` for "defer me").
  """

  def test_try_claim_returns_false_on_max_timestamp(self):

    class _ExhaustedReader(UnboundedReader):
      def __init__(self):
        self._n = 0

      def start(self):
        return False  # no data; will check watermark

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return MAX_TIMESTAMP

      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(next_offset=0)

      def close(self):
        pass

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _ExhaustedReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_Src())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    out = [None]
    # First call detects MAX_TIMESTAMP and transitions to terminal
    # INSIDE the lock, returning False (not True+None).
    self.assertFalse(tracker.try_claim(out))
    self.assertIsNone(out[0])
    # The tracker must now be terminal and have a cached checkpoint.
    self.assertIsNotNone(tracker.last_checkpoint_mark())
    # Subsequent try_claim still returns False (idempotent).
    self.assertFalse(tracker.try_claim([None]))

  def test_finally_block_only_reads_cached_checkpoint(self):
    # The wrapper's finally block must not touch the live reader (that
    # would bypass the SDF lock). Driving process() with a tracker whose
    # reader was already closed by mark_done should still produce a
    # finalizer callback from the cached mark.
    source = _CountingUnboundedSource(count=1)
    restriction = _SDFUnboundedSourceRestriction(source)
    finalizer, tracker = _drive_process(restriction)
    # After process() returns, the reader should be closed (mark_done
    # path) and the finalizer should have been registered from the
    # cached mark.
    self.assertIsNone(tracker.reader())
    self.assertEqual(len(finalizer.callbacks), 1)

  def test_unwrap_fallback_raises_runtime_error_in_process(self):
    # An unfamiliar view shape must fail loudly rather than silently
    # dropping finalization.
    from apache_beam.io.watermark_estimators import ManualWatermarkEstimator

    class _AlienView:
      def try_claim(self, position):
        return False

      def defer_remainder(self, delay=None):
        pass

      def current_restriction(self):
        return None

    fn = _SDFUnboundedSourceWrapperFn()
    estimator = ManualWatermarkEstimator(MIN_TIMESTAMP)
    with self.assertRaisesRegex(RuntimeError, 'view shape'):
      list(
          fn.process(
              element=_CountingUnboundedSource(count=1),
              bundle_finalizer=None,
              restriction_tracker=_AlienView(),
              watermark_estimator=estimator))

  def test_unwrap_succeeds_against_real_view_and_threadsafe_tracker(self):
    # Belt-and-suspenders for the wrapper's reliance on the private
    # ``_threadsafe_restriction_tracker`` / ``_restriction_tracker``
    # attribute names. If either name is renamed in sdf_utils.py, the
    # production wrapper silently breaks while every mock-based test in
    # this file continues to pass. This test wires up the REAL
    # ``RestrictionTrackerView`` + ``ThreadsafeRestrictionTracker`` so a
    # rename of either attribute breaks this assertion.
    from apache_beam.io.iobase import _unwrap_restriction_tracker
    from apache_beam.runners.sdf_utils import RestrictionTrackerView

    source = _CountingUnboundedSource(count=1)
    restriction = _SDFUnboundedSourceRestriction(source)
    inner = _SDFUnboundedSourceRestrictionTracker(restriction)
    threadsafe = ThreadsafeRestrictionTracker(inner)
    view = RestrictionTrackerView(threadsafe)

    unwrapped = _unwrap_restriction_tracker(view)
    self.assertIs(unwrapped, inner)


class SnapshotFailureAndFinalizerEdgeCasesTest(unittest.TestCase):
  """Regression tests for snapshot-failure handling and finalizer edge cases.

  Covers: ``mark_done`` re-raising on snapshot failure with claimed
  records, no-data branch propagating reader I/O errors via the outer
  close path, ``bundle_made_progress`` no longer counting bare defer as
  progress, PipelineOptions plumbing through ``_UnboundedSourceAsRead``,
  and fail-fast behavior when the runner's tracker view shape is unknown.
  """

  def test_mark_done_reraises_on_snapshot_failure_after_claim(self):

    class _SnapshotBoomReader(UnboundedReader):
      def __init__(self):
        self._n = 0

      def start(self):
        self._n = 1
        return True

      def advance(self):
        return False

      def get_current(self):
        return 0

      def get_current_timestamp(self):
        return Timestamp(seconds=1)

      def get_watermark(self):
        return MAX_TIMESTAMP if self._n > 0 else MIN_TIMESTAMP

      def get_checkpoint_mark(self):
        raise IOError('snapshot exploded')

      def close(self):
        pass

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _SnapshotBoomReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_Src())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim([None])  # claims 1 record
    with self.assertRaises(IOError):
      tracker.mark_done()
    # Reader must be closed and the tracker must NOT be marked terminal,
    # so the runner can retry the bundle with proper accounting.
    self.assertIsNone(tracker.reader())

  def test_mark_done_swallows_snapshot_failure_when_no_records_claimed(self):

    class _BadReader(UnboundedReader):
      def start(self):
        return False

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return MAX_TIMESTAMP

      def get_checkpoint_mark(self):
        raise IOError('snapshot failed')

      def close(self):
        pass

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _BadReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_Src())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim([None])  # no record claimed; out[0] stays None
    # Snapshot failure with no records claimed is benign — no
    # acknowledgements to lose, so mark_done proceeds.
    tracker.mark_done()

  def test_try_claim_no_data_propagates_get_watermark_error(self):

    class _Reader(UnboundedReader):
      def start(self):
        return False

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        raise IOError('watermark exploded')

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      _closed = False

      def close(self):
        type(self)._closed = True

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _Reader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_Src())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    with self.assertRaisesRegex(IOError, 'watermark exploded'):
      tracker.try_claim([None])
    # The outer except must close the reader on any I/O failure.
    self.assertIsNone(tracker.reader())
    self.assertTrue(_Reader._closed)

  def test_pipeline_options_propagated_to_wrapper(self):
    # _UnboundedSourceAsRead.expand should snapshot PipelineOptions from
    # the pipeline onto the wrapper, so workers see real options instead
    # of None when calling source.split / source.create_reader. Patch the
    # wrapper's __init__ scoped to this test only so a mid-test failure
    # cannot leak into other tests in the module.
    captured = []
    original_init = _SDFUnboundedSourceWrapperFn.__init__

    def _capturing_init(self, *args, **kwargs):
      original_init(self, *args, **kwargs)
      captured.append(self._options)

    with mock.patch.object(
        _SDFUnboundedSourceWrapperFn, '__init__', _capturing_init):
      transform = _UnboundedSourceAsRead(_CountingUnboundedSource(count=1))
      p = TestPipeline()
      transform.expand(p)
    self.assertEqual(len(captured), 1)
    self.assertIsNotNone(captured[0])

  def test_wrapper_passes_options_to_tracker(self):
    # The DoFn's create_tracker forwards self._options. This is the
    # actual integration point exercised on workers — create_reader is
    # called inside the tracker with these options.
    sentinel_options = object()
    wrapper = _SDFUnboundedSourceWrapperFn(options=sentinel_options)
    source = _CountingUnboundedSource(count=1)
    restriction = wrapper.initial_restriction(source)
    tracker = wrapper.create_tracker(restriction)
    self.assertIs(tracker._options, sentinel_options)


class DeferSplitSnapshotRegressionTest(unittest.TestCase):
  """Regression tests for defer-vs-split snapshot-failure semantics.

  Verifies that ``defer_remainder`` driven self-checkpoints re-raise on
  snapshot failure while runner-initiated splits decline gracefully, and
  the related close-reader / cached-mark behaviors.
  """

  def _started_snapshot_boom_tracker(self):
    class _SnapshotBoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      def start(self):
        return True

      def advance(self):
        return False

      def get_current(self):
        return 0

      def get_current_timestamp(self):
        return Timestamp(seconds=1)

      def get_watermark(self):
        return Timestamp(seconds=1)

      def get_checkpoint_mark(self):
        raise IOError('snapshot failed')

      def close(self):
        self.closed = True

    class _Src(UnboundedSource):
      def __init__(self):
        self.reader = None

      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        self.reader = _SnapshotBoomReader()
        return self.reader

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    src = _Src()
    restriction = _SDFUnboundedSourceRestriction(src)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim([None])
    return tracker, src.reader

  def test_try_claim_no_data_refreshes_reader_watermark(self):
    # H6: when try_claim() returns out[0]=None, the tracker must refresh
    # last_watermark() from the reader so the wrapper can advance the
    # estimator with a fresh value before deferring.

    class _Reader(UnboundedReader):
      def __init__(self):
        self.calls = 0

      def start(self):
        return False  # no data

      def advance(self):
        return False

      def get_current(self):
        raise AssertionError('should not be called')

      def get_current_timestamp(self):
        raise AssertionError('should not be called')

      def get_watermark(self):
        self.calls += 1
        return Timestamp(seconds=7)

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        pass

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _Reader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    src = _Src()
    restriction = _SDFUnboundedSourceRestriction(src)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    out = [None]
    tracker.try_claim(out)
    self.assertIsNone(out[0])
    # last_watermark() must reflect the reader's get_watermark() (7s),
    # not the original MIN_TIMESTAMP from the restriction.
    self.assertEqual(tracker.last_watermark(), Timestamp(seconds=7))

  def test_try_claim_closes_reader_on_reader_exception(self):
    # H5: try_claim() must release the reader if any reader I/O raises so
    # the bundle retry does not leak a half-initialized reader.

    class _BoomReader(UnboundedReader):
      closed = False

      def start(self):
        raise RuntimeError('boom in start')

      def advance(self):
        return False

      def get_current(self):
        return None

      def get_current_timestamp(self):
        return MIN_TIMESTAMP

      def get_watermark(self):
        return MIN_TIMESTAMP

      def get_checkpoint_mark(self):
        return _NoopCheckpointMark()

      def close(self):
        type(self).closed = True

    class _BoomSrc(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _BoomReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    restriction = _SDFUnboundedSourceRestriction(_BoomSrc())
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    with self.assertRaisesRegex(RuntimeError, 'boom'):
      tracker.try_claim([None])
    self.assertIsNone(tracker.reader())
    self.assertTrue(_BoomReader.closed)

  def test_try_split_declines_runner_initiated_when_snapshot_fails(self):
    # Runner-initiated split (fraction > 0): declining is safe; we want
    # processing to continue, not the bundle to fail.
    tracker, reader = self._started_snapshot_boom_tracker()
    # fraction > 0 is the runner-balancing path; declining is safe.
    self.assertIsNone(tracker.try_split(0.5))
    self.assertIs(tracker.reader(), reader)
    self.assertFalse(reader.closed)

  def test_try_split_zero_declines_runner_initiated_when_snapshot_fails(self):
    # DirectRunner's checkpoint path also calls try_split(0). Without the
    # explicit defer_remainder context this is still runner-initiated, so
    # declining is safe and processing should continue.
    tracker, reader = self._started_snapshot_boom_tracker()

    self.assertIsNone(tracker.try_split(0))
    self.assertIs(tracker.reader(), reader)
    self.assertFalse(reader.closed)

  def test_direct_runner_checkpoint_zero_declines_when_snapshot_fails(self):
    # Exercise DirectRunner's real checkpoint call shape: max-output
    # checkpointing invokes sdf_invoker.try_split(0), but it is not an
    # SDK-initiated defer_remainder call and must be allowed to decline.
    tracker, reader = self._started_snapshot_boom_tracker()
    threadsafe_tracker = ThreadsafeRestrictionTracker(tracker)
    output_processor = sdf_direct_runner._OutputHandler()

    class _FakeSdfInvoker(DoFnInvoker):
      def __init__(self):
        super().__init__(output_processor, None)
        self.split_fractions = []

      def invoke_process(
          self,
          windowed_value,
          restriction=None,
          watermark_estimator_state=None,
          additional_args=None,
          additional_kwargs=None):
        self.output_handler.handle_process_outputs(
            windowed_value, iter(['record']))

      def try_split(self, fraction):
        self.split_fractions.append(fraction)
        return threadsafe_tracker.try_split(fraction)

    class _NoopTimer:
      def __init__(self, unused_duration, unused_callback):
        pass

      def start(self):
        pass

    with mock.patch.object(sdf_direct_runner, 'Timer', _NoopTimer):
      sdf_invoker = _FakeSdfInvoker()
      invoker = sdf_direct_runner.SDFProcessElementInvoker(
          max_num_outputs=1, max_duration=60)
      outputs = list(
          invoker.invoke_process_element(
              sdf_invoker,
              output_processor,
              element='element',
              restriction=None,
              watermark_estimator_state=None))

    self.assertEqual(outputs[0], 'record')
    self.assertIsInstance(
        outputs[-1], sdf_direct_runner.SDFProcessElementInvoker.Result)
    self.assertIsNone(outputs[-1].residual_restriction)
    self.assertEqual(sdf_invoker.split_fractions, [0])
    self.assertIs(tracker.reader(), reader)
    self.assertFalse(reader.closed)

  def test_defer_remainder_raises_and_closes_when_snapshot_fails(self):
    # Actual defer_remainder must produce a residual. Silently returning
    # None would abandon the restriction (no residual stored), so the
    # wrapper raises and closes the reader so the runner retries cleanly.
    tracker, reader = self._started_snapshot_boom_tracker()
    threadsafe_tracker = ThreadsafeRestrictionTracker(tracker)

    with self.assertRaisesRegex(IOError, 'snapshot failed'):
      threadsafe_tracker.defer_remainder()
    self.assertIsNone(tracker.reader())
    self.assertFalse(tracker.reader_started())
    self.assertTrue(reader.closed)
    self.assertIsNone(threadsafe_tracker.deferred_status())

  def test_check_done_raises_when_records_claimed_without_termination(self):
    # M9: check_done() must raise on the genuine wrapper-bug case where
    # records were claimed but no terminal transition fired.
    source = _CountingUnboundedSource(count=4)
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim([None])  # increment _records_claimed
    with self.assertRaisesRegex(ValueError, 'did not transition'):
      tracker.check_done()

  def test_finalization_does_not_use_identity_for_mutating_mark(self):
    # NEW-2: a reader that mutates the same checkpoint mark in place
    # between bundles should still get finalization registered.

    class _MutMark(CheckpointMark):
      offset = 0

      def finalize_checkpoint(self):
        _MutMark.finalized_at.append(self.offset)

      finalized_at: list = []

    shared_mark = _MutMark()

    class _MutReader(UnboundedReader):
      def __init__(self):
        self._n = 0

      def start(self):
        self._n = 1
        return True

      def advance(self):
        return False

      def get_current(self):
        return self._n - 1

      def get_current_timestamp(self):
        return Timestamp(seconds=self._n)

      def get_watermark(self):
        return MAX_TIMESTAMP

      def get_checkpoint_mark(self):
        shared_mark.offset = self._n
        return shared_mark

      def close(self):
        pass

    class _Src(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        return _MutReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def default_output_coder(self):
        return coders.VarIntCoder()

    # We pass the same checkpoint mark as initial AND have the reader
    # return the same object after progress; finalization must still
    # fire because the bundle observably made progress.
    src = _Src()
    restriction = _SDFUnboundedSourceRestriction(
        src, checkpoint_mark=shared_mark)
    finalizer, _ = _drive_process(restriction)
    self.assertEqual(len(finalizer.callbacks), 1)
    finalizer.callbacks[0]()
    self.assertEqual(_MutMark.finalized_at, [1])

  def test_offset_based_dedup_is_rejected(self):
    class _OffsetSrc(UnboundedSource):
      def split(self, n, options=None):
        return [self]

      def create_reader(self, options, ck):
        raise AssertionError('should not be reached')

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

      def offset_based_deduplication_supported(self):
        return True

      def default_output_coder(self):
        return coders.VarIntCoder()

    with self.assertRaisesRegex(NotImplementedError, 'offset-based'):
      with TestPipeline() as p:
        _ = p | iobase.Read(_OffsetSrc())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
