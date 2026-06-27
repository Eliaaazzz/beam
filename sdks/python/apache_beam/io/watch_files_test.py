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

"""Tests for WatchForNewFiles."""

import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io.watch import after_total_of
from apache_beam.io.watch_files import ReadFromTextContinuously
from apache_beam.io.watch_files import WatchForNewFiles
from apache_beam.io.watch_files import _match_files
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


def _make_files(directory, names_and_mtimes):
  pattern = os.path.join(directory, '*.txt')
  for name, mtime in names_and_mtimes:
    path = os.path.join(directory, name)
    with open(path, 'w', encoding='utf-8') as handle:
      handle.write('x')
    os.utime(path, (mtime, mtime))
  return pattern


class MatchFilesPollTest(unittest.TestCase):
  def test_poll_stamps_each_file_by_modification_time(self):
    with tempfile.TemporaryDirectory() as directory:
      pattern = _make_files(
          directory, [('a.txt', 100), ('b.txt', 101), ('c.txt', 102)])
      result = _match_files(pattern)
      by_name = {
          os.path.basename(o.value.path): o.timestamp
          for o in result.outputs
      }
      self.assertEqual({'a.txt', 'b.txt', 'c.txt'}, set(by_name))
      self.assertEqual(Timestamp(100), by_name['a.txt'])
      self.assertEqual(Timestamp(102), by_name['c.txt'])
      self.assertFalse(result.is_complete)  # keeps polling

  def test_poll_on_empty_location_yields_nothing(self):
    with tempfile.TemporaryDirectory() as directory:
      result = _match_files(os.path.join(directory, '*.txt'))
      self.assertEqual((), result.outputs)


class WatchForNewFilesTest(unittest.TestCase):
  def _in_memory_pipeline(self):
    return TestPipeline(
        options=PipelineOptions(direct_running_mode='in_memory'))

  def test_emits_each_matching_file_once(self):
    with tempfile.TemporaryDirectory() as directory:
      pattern = _make_files(
          directory, [('a.txt', 100), ('b.txt', 101), ('c.txt', 102)])
      with self._in_memory_pipeline() as p:
        names = (
            p
            | WatchForNewFiles(
                pattern,
                interval=0.05,
                termination=after_total_of(Duration(0.4)))
            | beam.Map(lambda metadata: os.path.basename(metadata.path)))
        # Re-globbed every poll round, but the modification-time cursor emits
        # each file exactly once.
        assert_that(names, equal_to(['a.txt', 'b.txt', 'c.txt']))

  def test_mtime_cursor_emits_each_matching_file_once(self):
    with tempfile.TemporaryDirectory() as directory:
      pattern = _make_files(
          directory, [('a.txt', 100), ('b.txt', 101), ('c.txt', 102)])
      with self._in_memory_pipeline() as p:
        names = (
            p
            | WatchForNewFiles(
                pattern,
                interval=0.05,
                termination=after_total_of(Duration(0.4)),
                dedup='mtime')
            | beam.Map(lambda metadata: os.path.basename(metadata.path)))
        assert_that(names, equal_to(['a.txt', 'b.txt', 'c.txt']))

  def test_mtime_cursor_emits_files_sharing_a_modification_time(self):
    with tempfile.TemporaryDirectory() as directory:
      # Three files written at the same coarse modification time still each
      # emit once under the cursor's same-timestamp cohort.
      pattern = _make_files(
          directory, [('a.txt', 100), ('b.txt', 100), ('c.txt', 100)])
      with self._in_memory_pipeline() as p:
        names = (
            p
            | WatchForNewFiles(
                pattern,
                interval=0.05,
                termination=after_total_of(Duration(0.4)),
                dedup='mtime')
            | beam.Map(lambda metadata: os.path.basename(metadata.path)))
        assert_that(names, equal_to(['a.txt', 'b.txt', 'c.txt']))

  def test_read_lines_of_each_new_file(self):
    with tempfile.TemporaryDirectory() as directory:
      for i, (name, mtime) in enumerate([('a.txt', 100), ('b.txt', 101)]):
        path = os.path.join(directory, name)
        with open(path, 'w', encoding='utf-8') as handle:
          handle.write('f%d-line1\nf%d-line2\n' % (i, i))
        os.utime(path, (mtime, mtime))
      pattern = os.path.join(directory, '*.txt')
      with self._in_memory_pipeline() as p:
        lines = (
            p
            | ReadFromTextContinuously(
                pattern,
                interval=0.05,
                termination=after_total_of(Duration(0.4))))
        assert_that(
            lines, equal_to(['f0-line1', 'f0-line2', 'f1-line1', 'f1-line2']))


if __name__ == '__main__':
  unittest.main()
