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

"""Unit tests for classes in sdf_utils.py."""

# pytype: skip-file

import time
import unittest

from apache_beam.io.concat_source_test import RangeSource
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.runners.sdf_utils import ThreadsafeWatermarkEstimator
from apache_beam.utils import timestamp


class ThreadsafeRestrictionTrackerTest(unittest.TestCase):
  def test_initialization(self):
    with self.assertRaises(ValueError):
      ThreadsafeRestrictionTracker(RangeSource(0, 1))

  def test_defer_remainder_with_wrong_time_type(self):
    threadsafe_tracker = ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    with self.assertRaises(ValueError):
      threadsafe_tracker.defer_remainder(10)

  def test_self_checkpoint_immediately(self):
    restriction_tracker = OffsetRestrictionTracker(OffsetRange(0, 10))
    threadsafe_tracker = ThreadsafeRestrictionTracker(restriction_tracker)
    threadsafe_tracker.defer_remainder()
    deferred_residual, deferred_time = threadsafe_tracker.deferred_status()
    expected_residual = OffsetRange(0, 10)
    self.assertEqual(deferred_residual, expected_residual)
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    self.assertEqual(deferred_time, 0)

  def test_defer_remainder_sets_optional_context_hook(self):
    class _HookedOffsetRestrictionTracker(OffsetRestrictionTracker):
      def __init__(self):
        super().__init__(OffsetRange(0, 10))
        self._in_defer_remainder_split = False
        self.hook_values = []
        self.try_split_hook_values = []

      def _set_defer_remainder_split(self, active):
        self._in_defer_remainder_split = active
        self.hook_values.append(active)

      def try_split(self, fraction_of_remainder):
        self.try_split_hook_values.append(self._in_defer_remainder_split)
        return super().try_split(fraction_of_remainder)

    restriction_tracker = _HookedOffsetRestrictionTracker()
    threadsafe_tracker = ThreadsafeRestrictionTracker(restriction_tracker)

    threadsafe_tracker.defer_remainder()

    self.assertEqual(restriction_tracker.hook_values, [True, False])
    self.assertEqual(restriction_tracker.try_split_hook_values, [True])

  def test_defer_remainder_ignores_non_callable_context_hook(self):
    # A user-defined tracker may collide on the hook name with a non-callable
    # attribute (e.g. an int sentinel) or expose a property that returns one.
    # Either way ``defer_remainder`` must skip the hook instead of raising
    # TypeError when it tries to call a non-callable.

    class _AttrHookOffsetRestrictionTracker(OffsetRestrictionTracker):
      _set_defer_remainder_split = True

    class _PropertyHookOffsetRestrictionTracker(OffsetRestrictionTracker):
      @property
      def _set_defer_remainder_split(self):
        return 'not-a-callable'

    for cls in (_AttrHookOffsetRestrictionTracker,
                _PropertyHookOffsetRestrictionTracker):
      restriction_tracker = cls(OffsetRange(0, 10))
      threadsafe_tracker = ThreadsafeRestrictionTracker(restriction_tracker)

      threadsafe_tracker.defer_remainder()

      deferred_residual, _ = threadsafe_tracker.deferred_status()
      self.assertEqual(deferred_residual, OffsetRange(0, 10))

  def test_self_checkpoint_with_relative_time(self):
    threadsafe_tracker = ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    threadsafe_tracker.defer_remainder(timestamp.Duration(100))
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation = 100 - 2 - some_delta
    self.assertTrue(deferred_time <= 98)

  def test_self_checkpoint_with_absolute_time(self):
    threadsafe_tracker = ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    now = timestamp.Timestamp.now()
    schedule_time = now + timestamp.Duration(100)
    self.assertTrue(isinstance(schedule_time, timestamp.Timestamp))
    threadsafe_tracker.defer_remainder(schedule_time)
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation =
    # schedule_time - the time when deferred_status is called - some_delta
    self.assertTrue(deferred_time <= 98)


class RestrictionTrackerViewTest(unittest.TestCase):
  def test_initialization(self):
    with self.assertRaises(ValueError):
      RestrictionTrackerView(OffsetRestrictionTracker(OffsetRange(0, 10)))

  def test_api_expose(self):
    threadsafe_tracker = ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    tracker_view = RestrictionTrackerView(threadsafe_tracker)
    current_restriction = tracker_view.current_restriction()
    self.assertEqual(current_restriction, OffsetRange(0, 10))
    self.assertTrue(tracker_view.try_claim(0))
    tracker_view.defer_remainder()
    deferred_remainder, deferred_watermark = (
        threadsafe_tracker.deferred_status())
    self.assertEqual(deferred_remainder, OffsetRange(1, 10))
    self.assertEqual(deferred_watermark, timestamp.Duration())

  def test_non_expose_apis(self):
    threadsafe_tracker = ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    tracker_view = RestrictionTrackerView(threadsafe_tracker)
    with self.assertRaises(AttributeError):
      tracker_view.check_done()
    with self.assertRaises(AttributeError):
      tracker_view.current_progress()
    with self.assertRaises(AttributeError):
      tracker_view.try_split()
    with self.assertRaises(AttributeError):
      tracker_view.deferred_status()


class ThreadsafeWatermarkEstimatorTest(unittest.TestCase):
  def test_initialization(self):
    with self.assertRaises(ValueError):
      ThreadsafeWatermarkEstimator(None)

  def test_get_estimator_state(self):
    estimator = ThreadsafeWatermarkEstimator(ManualWatermarkEstimator(None))
    self.assertIsNone(estimator.get_estimator_state())
    estimator.set_watermark(timestamp.Timestamp(10))
    self.assertEqual(estimator.get_estimator_state(), timestamp.Timestamp(10))

  def test_track_timestamp(self):
    estimator = ThreadsafeWatermarkEstimator(ManualWatermarkEstimator(None))
    estimator.observe_timestamp(timestamp.Timestamp(10))
    self.assertIsNone(estimator.current_watermark())
    estimator.set_watermark(timestamp.Timestamp(20))
    self.assertEqual(estimator.current_watermark(), timestamp.Timestamp(20))

  def test_non_exsited_attr(self):
    estimator = ThreadsafeWatermarkEstimator(ManualWatermarkEstimator(None))
    with self.assertRaises(AttributeError):
      estimator.non_existed_call()


if __name__ == '__main__':
  unittest.main()
