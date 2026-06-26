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

"""Scalable continuous file discovery built on the Watch transform.

:class:`WatchForNewFiles` polls a file pattern on an interval and emits each
newly appeared file's :class:`~apache_beam.io.filesystem.FileMetadata`. It
dedups by a high-water-mark modification-time cursor, so the per-pattern state
is a single timestamp (O(1)) regardless of how many files match.

This is the scalable alternative to :class:`apache_beam.io.fileio.
MatchContinuously`, whose per-file keyed state grows with the number of distinct
files. The unbounded dedup state is what issue #18459 reports causing a Dataflow
``ValueState`` size-limit failure, and what ``MatchContinuously`` documents as
"scales poorly... requires storing file ids in memory".

Because dedup is by the modification time the cursor has already passed, the
state never grows and survives a restart through the Watch restriction state, so
files are not reprocessed. A file is emitted when its modification time first
exceeds the cursor, so a file rewritten with a newer modification time is
re-emitted.

PRECONDITION: new files must appear with strictly increasing modification times.
This holds for object stores whose object update time is effectively unique
(GCS/S3 use sub-second precision). On filesystems with coarse (for example
one-second) modification-time granularity, two files created within the same
tick share a timestamp and only the first is emitted; use the exact
``fileio.MatchContinuously`` for those sources.

Example::

    files = (
        pipeline
        | WatchForNewFiles('gs://bucket/path/*.json', interval=30)
        | beam.io.fileio.ReadMatches())

This API is experimental and may change in backwards-incompatible ways.
"""

import logging

import apache_beam as beam
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import TerminationCondition
from apache_beam.io.watch import Watch
from apache_beam.io.watch import never
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = ['WatchForNewFiles']

_LOGGER = logging.getLogger(__name__)


def _match_files(file_pattern):
  """Polls a file pattern, returning each matching file stamped with its
  modification time so the Watch timestamp cursor dedups by that time.

  A pattern that matches nothing yet returns no outputs (and keeps polling), so
  watching an initially-empty location is supported.
  """
  try:
    results = FileSystems.match([file_pattern])
  except BeamIOError:
    return PollResult.incomplete([])
  metadata = results[0].metadata_list if results else []
  return PollResult.incomplete([
      TimestampedValue(m, Timestamp(m.last_updated_in_seconds))
      for m in metadata
  ])


class WatchForNewFiles(beam.PTransform):
  """Continuously watches ``file_pattern`` and emits each new file's
  :class:`~apache_beam.io.filesystem.FileMetadata`, deduped by an O(1)
  modification-time cursor. See the module docstring for the ordering
  precondition.
  """
  def __init__(
      self,
      file_pattern: str,
      interval=60.0,
      termination: TerminationCondition = None):
    """Initializes the transform.

    Args:
      file_pattern: the file glob to watch.
      interval: seconds (or a :class:`Duration`) between polls.
      termination: a per-input :class:`TerminationCondition`; defaults to
        watching forever (:func:`never`).
    """
    super().__init__()
    self._file_pattern = file_pattern
    self._interval = (
        interval if isinstance(interval, Duration) else Duration(interval))
    self._termination = termination or never()

  def expand(self, pbegin):
    watch = (
        Watch.growth_of(_match_files).with_poll_interval(
            self._interval).with_timestamp_cursor().with_termination_per_input(
                self._termination))
    return (
        pbegin
        | 'Pattern' >> beam.Create([self._file_pattern])
        | 'Watch' >> watch
        | 'ExtractMetadata' >> beam.Map(lambda pattern_file: pattern_file[1]))
