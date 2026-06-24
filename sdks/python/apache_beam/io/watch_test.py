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

"""Tests for the Watch transform."""

import collections
import hashlib
import unittest

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import Watch
from apache_beam.io.watch import _GrowthRestrictionTracker
from apache_beam.io.watch import _GrowthStateCoder
from apache_beam.io.watch import _NonPollingGrowthState
from apache_beam.io.watch import _PollingGrowthState
from apache_beam.io.watch import after_total_of
from apache_beam.io.watch import never
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


def _ts(value, timestamp):
  return TimestampedValue(value, Timestamp(timestamp))


def _new_tracker(restriction, poll_fn, now=0.0):
  return _GrowthRestrictionTracker(
      restriction, poll_fn, StrUtf8Coder(), never(), lambda: now)


def _initial_polling(termination=None, now=Timestamp(0)):
  termination = termination or never()
  return _PollingGrowthState(
      collections.OrderedDict(), None, termination.for_new_input(now, 'input'))


def _horizon_tracker(restriction, poll_fn, horizon, now=0.0):
  return _GrowthRestrictionTracker(
      restriction,
      poll_fn,
      StrUtf8Coder(),
      never(), lambda: now,
      Duration(horizon))


def _hash(value):
  return hashlib.blake2b(StrUtf8Coder().encode(value), digest_size=16).digest()


class PollResultTest(unittest.TestCase):
  def test_normalize_stamps_one_processing_time_when_timestamp_none(self):
    before = Timestamp.now()
    result = PollResult.incomplete(['a', 'b'])
    after = Timestamp.now()
    # Raw outputs share a single processing-time stamp (no per-output jitter).
    stamps = {o.timestamp for o in result.outputs}
    self.assertEqual(1, len(stamps))
    ts = stamps.pop()
    self.assertTrue(before <= ts <= after)

  def test_normalize_preserves_timestamped_and_applies_explicit_default(self):
    result = PollResult.incomplete([_ts('a', 1), 'b'], timestamp=7)
    by_value = {o.value: o.timestamp for o in result.outputs}
    self.assertEqual(Timestamp(1), by_value['a'])  # TimestampedValue preserved
    self.assertEqual(Timestamp(7), by_value['b'])  # raw stamped with default

  def test_complete_releases_watermark_to_max(self):
    self.assertEqual(
        MAX_TIMESTAMP, PollResult.complete([_ts('a', 1)]).watermark)
    self.assertTrue(PollResult.complete([]).is_complete)

  def test_with_watermark_overrides(self):
    self.assertEqual(
        Timestamp(0),
        PollResult.incomplete([_ts('a', 9)]).with_watermark(0).watermark)


class GrowthStateCoderTest(unittest.TestCase):
  def test_polling_round_trip_preserves_resume_state(self):
    termination = after_total_of(Duration(30))
    coder = _GrowthStateCoder(StrUtf8Coder(), termination)
    completed = collections.OrderedDict([
        (b'a' * 16, Timestamp(1)),
        (b'b' * 16, Timestamp(2)),
        (b'c' * 16, Timestamp(3)),
    ])
    termination_state = termination.for_new_input(Timestamp(7), 'input')
    state = _PollingGrowthState(completed, Timestamp(5), termination_state)
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(list(completed.items()), list(decoded.completed.items()))
    self.assertEqual(Timestamp(5), decoded.poll_watermark)
    self.assertEqual(termination_state, decoded.termination_state)

  def test_non_polling_round_trip_preserves_pending_outputs(self):
    coder = _GrowthStateCoder(StrUtf8Coder(), never())
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    state = _NonPollingGrowthState(pending)
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(MAX_TIMESTAMP, decoded.pending.watermark)
    self.assertEqual([('a', Timestamp(1)), ('b', Timestamp(2))],
                     [(o.value, o.timestamp) for o in decoded.pending.outputs])


class GrowthTrackerTest(unittest.TestCase):
  def test_poll_claims_dedups_and_checkpoints(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1), _ts('a', 1), _ts('b', 2)])

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    self.assertTrue(tracker.try_claim(holder))
    work = holder[1]
    kind, outputs, watermark, stop = work[0], work[1], work[2], work[3]
    self.assertEqual('poll', kind)
    self.assertEqual(['a', 'b'], [o.value for o in outputs])
    self.assertFalse(stop)
    self.assertEqual(Timestamp(1), watermark)
    self.assertFalse(tracker.is_bounded())

    primary, residual = tracker.try_split(0)
    self.assertIsInstance(primary, _NonPollingGrowthState)
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(2, len(residual.completed))
    self.assertTrue(tracker.check_done())

    def explicit_watermark_poll(unused_element):
      return PollResult.incomplete([_ts('c', 3)]).with_watermark(5)

    explicit_tracker = _new_tracker(_initial_polling(), explicit_watermark_poll)
    holder = ['input', None]
    self.assertTrue(explicit_tracker.try_claim(holder))
    self.assertEqual(Timestamp(5), holder[1][2])
    _, residual = explicit_tracker.try_split(0)
    self.assertEqual(Timestamp(5), residual.poll_watermark)

  def test_second_round_repolls_and_dedups_against_completed(self):
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 1), _ts('b', 2)])
      return PollResult.incomplete([_ts('a', 1), _ts('c', 3)])

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual = tracker.try_split(0)

    resumed = _new_tracker(residual, poll)
    holder = ['input', None]
    self.assertTrue(resumed.try_claim(holder))
    outputs = holder[1][1]
    self.assertEqual(2, len(polls))
    self.assertEqual(['c'], [o.value for o in outputs])

  def test_termination_condition_sets_stop(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1)])

    termination = after_total_of(10)
    for now, expected_stop in [(10.0, False), (11.0, True)]:
      with self.subTest(now=now):
        tracker = _GrowthRestrictionTracker(
            _initial_polling(termination, Timestamp(0)),
            poll,
            StrUtf8Coder(),
            termination, lambda now=now: now)
        holder = ['input', None]
        self.assertTrue(tracker.try_claim(holder))
        self.assertEqual(expected_stop, holder[1][3])

  def test_non_polling_replays(self):
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    tracker = _new_tracker(_NonPollingGrowthState(pending), lambda e: None)
    holder = ['input', None]
    self.assertTrue(tracker.try_claim(holder))
    work = holder[1]
    kind, replayed = work[0], work[1]
    self.assertEqual('replay', kind)
    self.assertEqual(['a', 'b'], [o.value for o in replayed.outputs])
    self.assertTrue(tracker.check_done())

  def test_terminal_split_residual_is_empty_for_all_stop_causes(self):
    termination = after_total_of(Duration(10))
    cases = [
        (
            'reached_max',
            never(),
            _initial_polling(), lambda element: PollResult(
                (TimestampedValue('a', MAX_TIMESTAMP), ), None),
            0.0),
        (
            'complete',
            never(),
            _initial_polling(),
            lambda element: PollResult.complete([_ts('a', 1)]),
            0.0),
        (
            'after_total_of',
            termination,
            _initial_polling(termination, Timestamp(0)),
            lambda element: PollResult.incomplete([_ts('a', 1)]),
            100.0),
    ]
    for name, condition, restriction, poll_fn, now in cases:
      with self.subTest(name=name):
        tracker = _GrowthRestrictionTracker(
            restriction,
            poll_fn,
            StrUtf8Coder(),
            condition, lambda now=now: now)
        holder = ['input', None]
        self.assertTrue(tracker.try_claim(holder))
        self.assertTrue(holder[1][3])
        _, residual = tracker.try_split(0)
        self.assertIsInstance(residual, _NonPollingGrowthState)
        self.assertEqual((), residual.pending.outputs)

  def test_wrapper_chain_defers_merged_residual(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1), _ts('b', 2)])

    threadsafe = ThreadsafeRestrictionTracker(
        _new_tracker(_initial_polling(), poll))
    view = RestrictionTrackerView(threadsafe)
    holder = ['input', None]
    self.assertTrue(view.try_claim(holder))
    self.assertEqual('poll', holder[1][0])
    view.defer_remainder(Duration(5))
    residual, _ = threadsafe.deferred_status()
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(2, len(residual.completed))

  def test_out_of_order_new_output_infers_its_earlier_event_time(self):
    # Round 1 surfaces a@10; round 2 surfaces a brand-new b@5 (earlier). The
    # round-2 inferred watermark is b's own time; the process() estimator's
    # monotonic guard is what leaves b late. This locks in the
    # reference-consistent behavior the design documents and warns about.
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 10)])
      return PollResult.incomplete([_ts('b', 5)])

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    tracker.try_claim(holder)
    self.assertEqual(Timestamp(10), holder[1][2])
    _, residual = tracker.try_split(0)

    resumed = _new_tracker(residual, poll)
    holder = ['input', None]
    self.assertTrue(resumed.try_claim(holder))
    self.assertEqual(['b'], [o.value for o in holder[1][1]])
    self.assertEqual(Timestamp(5), holder[1][2])

  def test_explicit_watermark_holds_below_output_time(self):
    # An explicit watermark below the output's own event time is honored, so a
    # later earlier-timestamped output stays on time (the out-of-order-safe
    # path).
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 10)]).with_watermark(0)

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    tracker.try_claim(holder)
    self.assertEqual(Timestamp(0), holder[1][2])
    _, residual = tracker.try_split(0)
    self.assertEqual(Timestamp(0), residual.poll_watermark)

  def test_idle_round_reuses_completed_map_object(self):
    # A round that discovers nothing (and no dedup horizon) must reuse the
    # parent dedup map rather than copying it O(N).
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 1)])
      return PollResult.incomplete([])

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual1 = tracker.try_split(0)

    resumed = _new_tracker(residual1, poll)
    holder = ['input', None]
    resumed.try_claim(holder)
    _, residual2 = resumed.try_split(0)
    self.assertIs(residual1.completed, residual2.completed)

  def test_dedup_horizon_evicts_outputs_behind_the_watermark(self):
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 10), _ts('b', 20)])
      return PollResult.incomplete([_ts('c', 30)])

    tracker = _horizon_tracker(_initial_polling(), poll, horizon=5)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual1 = tracker.try_split(0)
    self.assertEqual(2, len(residual1.completed))  # nothing stale yet

    resumed = _horizon_tracker(residual1, poll, horizon=5)
    holder = ['input', None]
    resumed.try_claim(holder)
    _, residual2 = resumed.try_split(0)
    # Watermark advanced to 30; a@10 and b@20 are older than 30-5=25, evicted.
    self.assertEqual([_hash('c')], list(residual2.completed))
    self.assertEqual(Timestamp(30), residual2.completed[_hash('c')])

  def test_dedup_horizon_unset_retains_all_hashes(self):
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 10), _ts('b', 20)])
      return PollResult.incomplete([_ts('c', 30)])

    tracker = _new_tracker(_initial_polling(), poll)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual1 = tracker.try_split(0)
    resumed = _new_tracker(residual1, poll)
    holder = ['input', None]
    resumed.try_claim(holder)
    _, residual2 = resumed.try_split(0)
    self.assertEqual(3, len(residual2.completed))

  def test_dedup_horizon_still_dedups_recent_outputs(self):
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 10)])
      # 'a' re-appears but is well within the (large) horizon, so it stays in
      # the dedup set and only the genuinely-new 'b' is emitted.
      return PollResult.incomplete([_ts('a', 10), _ts('b', 12)])

    tracker = _horizon_tracker(_initial_polling(), poll, horizon=100)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual1 = tracker.try_split(0)
    resumed = _horizon_tracker(residual1, poll, horizon=100)
    holder = ['input', None]
    resumed.try_claim(holder)
    self.assertEqual(['b'], [o.value for o in holder[1][1]])

  def test_dedup_horizon_reemits_a_value_resurfaced_at_a_fresh_timestamp(self):
    # Documents the residual with_dedup_horizon hazard: a value re-returned at a
    # NEWER, at-or-after-watermark timestamp is on time, so suppression does not
    # drop it; with its entry already evicted it is seen as new and re-emitted.
    # (A re-list at the original below-horizon timestamp is suppressed instead;
    # see test_dedup_horizon_suppresses_aged_relist_and_advances_watermark.)
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('x', 10)])
      if len(polls) == 2:
        return PollResult.incomplete([_ts('y', 30)])  # advances watermark to 30
      return PollResult.incomplete([_ts('x', 40)])  # x re-surfaces, on time

    tracker = _horizon_tracker(_initial_polling(), poll, horizon=5)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual = tracker.try_split(0)

    resumed = _horizon_tracker(residual, poll, horizon=5)
    holder = ['input', None]
    resumed.try_claim(holder)  # round 2: y@30, watermark->30, evict x@10 (<25)
    _, residual = resumed.try_split(0)
    self.assertNotIn(_hash('x'), residual.completed)

    resumed2 = _horizon_tracker(residual, poll, horizon=5)
    holder = ['input', None]
    resumed2.try_claim(holder)  # round 3: x@40 re-emitted as a duplicate
    self.assertEqual(['x'], [o.value for o in holder[1][1]])

  def test_dedup_horizon_evicts_regardless_of_insertion_order(self):
    # An explicit held-back watermark lets a later round insert an
    # earlier-timestamped output after a future-timestamped one, so insertion
    # order is not event-time order. Eviction must still drop the aged-out
    # entry (a full scan, not a prefix scan).
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('future', 1000000)]).with_watermark(0)
      if len(polls) == 2:
        return PollResult.incomplete([_ts('old', 900)]).with_watermark(900)
      return PollResult.incomplete([_ts('marker', 1005)]).with_watermark(1005)

    state = _initial_polling()
    for _ in range(3):
      tracker = _horizon_tracker(state, poll, horizon=100)
      holder = ['input', None]
      tracker.try_claim(holder)
      _, state = tracker.try_split(0)
    # cutoff = 1005 - 100 = 905; old@900 is evicted even though 'future'
    # precedes it in insertion order, while future@1000000 is retained.
    self.assertNotIn(_hash('old'), state.completed)
    self.assertIn(_hash('future'), state.completed)
    self.assertIn(_hash('marker'), state.completed)

  def test_dedup_horizon_keeps_entry_exactly_at_cutoff(self):
    # Eviction is strict (< cutoff): an entry whose event time equals
    # watermark - horizon is retained, not evicted.
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 25)])
      return PollResult.incomplete([_ts('b', 30)])  # watermark->30, cutoff=25

    tracker = _horizon_tracker(_initial_polling(), poll, horizon=5)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, residual = tracker.try_split(0)
    resumed = _horizon_tracker(residual, poll, horizon=5)
    holder = ['input', None]
    resumed.try_claim(holder)
    _, residual = resumed.try_split(0)
    self.assertIn(_hash('a'), residual.completed)  # a@25 == cutoff, kept
    self.assertIn(_hash('b'), residual.completed)

  def test_dedup_horizon_suppresses_aged_relist_and_advances_watermark(self):
    # A full-relisting poll re-surfaces every output at its original event time
    # every round. Once an output ages past the horizon its hash is evicted;
    # suppression then drops the re-listed copy instead of re-emitting it, so
    # the watermark keeps advancing (no freeze) and the dedup set stays bounded.
    def poll_for(round_index):
      def poll(unused_element):
        return PollResult.incomplete(
            [_ts('f%d' % i, i) for i in range(round_index + 1)])

      return poll

    state = _initial_polling()
    emitted = collections.Counter()
    watermarks = []
    sizes = []
    for round_index in range(10):
      tracker = _horizon_tracker(state, poll_for(round_index), horizon=3)
      holder = ['input', None]
      tracker.try_claim(holder)
      for output in holder[1][1]:
        emitted[output.value] += 1
      _, state = tracker.try_split(0)
      watermarks.append(state.poll_watermark)
      sizes.append(len(state.completed))
    # Every file emitted exactly once: the aged re-lists are suppressed, not
    # re-emitted, so there is no duplicate storm.
    self.assertEqual([1] * 10, [emitted['f%d' % i] for i in range(10)])
    # The watermark advances to the final round instead of freezing at the
    # horizon: GC-only (no suppression) would pin it at horizon + 1.
    self.assertEqual(Timestamp(9), watermarks[-1])
    # Dedup state is bounded by the horizon window, not the full history.
    self.assertLessEqual(max(sizes), 5)

  def test_dedup_horizon_reports_suppressed_count(self):
    # An aged output below the cutoff is suppressed and surfaced as a count so
    # the DoFn can warn; an on-time output in the same round is still emitted.
    def poll_for(round_index):
      def poll(unused_element):
        if round_index == 0:
          return PollResult.incomplete([_ts('seed', 100)])
        return PollResult.incomplete([_ts('aged', 1), _ts('fresh', 101)])

      return poll

    tracker = _horizon_tracker(_initial_polling(), poll_for(0), horizon=5)
    holder = ['input', None]
    tracker.try_claim(holder)
    _, state = tracker.try_split(0)  # watermark now 100, cutoff 95

    resumed = _horizon_tracker(state, poll_for(1), horizon=5)
    holder = ['input', None]
    resumed.try_claim(holder)
    work = holder[1]
    self.assertEqual(['fresh'], [o.value for o in work[1]])  # aged@1 dropped
    self.assertEqual(1, work[4])  # one suppression reported


# Module-level so the poll function pickles by reference; the call counter is
# shared within the single in-memory DirectRunner process.
_POLL_CALLS = collections.defaultdict(int)


def _growing_poll(prefix):
  _POLL_CALLS[prefix] += 1
  count = _POLL_CALLS[prefix]
  outputs = [_ts('%s%d' % (prefix, i), i + 1) for i in range(count)]
  if count >= 3:
    return PollResult.complete(outputs)
  return PollResult.incomplete(outputs)


def _complete_poll(prefix):
  return PollResult.complete([_ts(prefix + 'a', 1), _ts(prefix + 'b', 2)])


def _out_of_order_poll(prefix):
  # Round 1 emits a late_after@10 (advances the watermark to 10); round 2 emits
  # early@5, which is behind the watermark and therefore late.
  _POLL_CALLS[prefix] += 1
  count = _POLL_CALLS[prefix]
  if count == 1:
    return PollResult.incomplete([_ts(prefix + 'late_after', 10)])
  return PollResult.complete([_ts(prefix + 'early', 5)])


def _aging_relist_poll(prefix):
  # Re-lists the whole growing directory every round at the files' original
  # event times, then completes. With a tight dedup horizon the oldest files
  # age out and their re-lists must be suppressed, not re-emitted.
  _POLL_CALLS[prefix] += 1
  count = _POLL_CALLS[prefix]
  outputs = [_ts('%s%d' % (prefix, i), i) for i in range(count)]
  if count >= 5:
    return PollResult.complete(outputs)
  return PollResult.incomplete(outputs)


def _windowed_group(kv, window=beam.DoFn.WindowParam):
  return ((window.start, window.end), sorted(kv[1]))


class WatchBuilderTest(unittest.TestCase):
  def test_with_dedup_horizon_rejects_negative(self):
    watch = Watch.growth_of(_complete_poll)
    with self.assertRaises(ValueError):
      watch.with_dedup_horizon(-1)
    with self.assertRaises(ValueError):
      watch.with_dedup_horizon(Duration(-0.5))

  def test_with_dedup_horizon_accepts_zero_and_duration(self):
    watch = Watch.growth_of(_complete_poll)
    self.assertEqual(Duration(0), watch.with_dedup_horizon(0)._dedup_horizon)
    self.assertEqual(
        Duration(5), watch.with_dedup_horizon(Duration(5))._dedup_horizon)


class WatchEndToEndTest(unittest.TestCase):
  def _in_memory_pipeline(self):
    return TestPipeline(
        options=PipelineOptions(direct_running_mode='in_memory'))

  def test_complete_outputs_values_and_timestamps(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch.growth_of(_complete_poll).with_poll_interval(Duration(1)))
      assert_that(
          output,
          equal_to([
              TestWindowedValue(('k:', 'k:a'), Timestamp(1), [GlobalWindow()]),
              TestWindowedValue(('k:', 'k:b'), Timestamp(2), [GlobalWindow()]),
          ]),
          reify_windows=True)

  def test_complete_advances_watermark_for_windowed_pipeline(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch.growth_of(_complete_poll).with_poll_interval(Duration(1)))
      grouped = (
          output
          | beam.WindowInto(FixedWindows(10))
          | beam.Map(lambda kv: ('all', kv[1]))
          | beam.GroupByKey()
          | beam.Map(_windowed_group))
      assert_that(
          grouped,
          equal_to([
              ((Timestamp(0), Timestamp(10)), ['k:a', 'k:b']),
          ]))

  def test_multi_round_dedups_stops_and_is_per_input(self):
    _POLL_CALLS.clear()
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['x:', 'y:'])
          | Watch.growth_of(_growing_poll).with_poll_interval(Duration(0.05)))
      assert_that(
          output,
          equal_to([('x:', 'x:0'), ('x:', 'x:1'), ('x:', 'x:2'), ('y:', 'y:0'),
                    ('y:', 'y:1'), ('y:', 'y:2')]))
    self.assertEqual(3, _POLL_CALLS['x:'])
    self.assertEqual(3, _POLL_CALLS['y:'])

  def test_out_of_order_across_rounds_warns_about_late_output(self):
    _POLL_CALLS.clear()
    with self.assertLogs(level='WARNING') as logs:
      with self._in_memory_pipeline() as p:
        output = (
            p | beam.Create(['k:'])
            | Watch.growth_of(_out_of_order_poll).with_poll_interval(
                Duration(0.05)))
        # Watch emits every new output; the lateness is a downstream concern.
        assert_that(
            output, equal_to([('k:', 'k:late_after'), ('k:', 'k:early')]))
    self.assertTrue(
        any('behind the current watermark' in line for line in logs.output),
        'expected a late-emission warning, got: %s' % logs.output)

  def test_relisting_with_dedup_horizon_suppresses_and_dedups(self):
    _POLL_CALLS.clear()
    with self.assertLogs(level='WARNING') as logs:
      with self._in_memory_pipeline() as p:
        output = (
            p | beam.Create(['k:'])
            | Watch.growth_of(_aging_relist_poll).with_poll_interval(
                Duration(0.05)).with_dedup_horizon(Duration(1)))
        # Each file is emitted exactly once even though every round re-lists the
        # whole directory: aged re-lists are suppressed, recent ones deduped.
        assert_that(output, equal_to([('k:', 'k:%d' % i) for i in range(5)]))
    self.assertTrue(
        any('more than the dedup horizon' in line for line in logs.output),
        'expected a suppression warning, got: %s' % logs.output)


if __name__ == '__main__':
  unittest.main()
