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
offers two dedup modes:

* ``dedup='path'`` (default): exact dedup by file path. Each path is emitted
  once; the per-pattern state holds the set of seen paths, like
  :class:`apache_beam.io.fileio.MatchContinuously`.
* ``dedup='mtime'``: dedup by a high-water-mark modification-time cursor, so the
  per-pattern state is one timestamp plus the bounded cohort of files sharing
  that timestamp (O(1)) regardless of how many files match.

The cursor mode is the scalable alternative to ``MatchContinuously``, whose
per-file keyed state grows with the number of distinct files. That unbounded
dedup state is what issue #18459 reports causing a Dataflow ``ValueState``
size-limit failure, and what ``MatchContinuously`` documents as "scales
poorly... requires storing file ids in memory".

In cursor mode a file is emitted when its modification time first reaches or
passes the cursor; files sharing the cursor's exact instant are each emitted
once. The state survives a restart through the Watch restriction state, so files
are not reprocessed.

CURSOR PRECONDITION: a file's modification time must never fall below the
high-water mark of an earlier poll. This holds when files only ever appear with
newer modification times. A file surfaced with a modification time below the
cursor (a backfill or clock skew) is treated as already seen and dropped; use
the default ``dedup='path'`` or ``fileio.MatchContinuously`` for those sources.

Example::

    files = (
        pipeline
        | WatchForNewFiles('gs://bucket/path/*.json', interval=30)
        | beam.io.fileio.ReadMatches())

This API is experimental and may change in backwards-incompatible ways.
"""

import logging

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.textio import ReadAllFromText
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import TerminationCondition
from apache_beam.io.watch import Watch
from apache_beam.io.watch import never
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = ['WatchForNewFiles', 'ReadFromTextContinuously']

_DEDUP_MODES = ('path', 'mtime')

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
  :class:`~apache_beam.io.filesystem.FileMetadata`.

  Dedups by file path (``dedup='path'``, the default) or by an O(1)
  modification-time cursor (``dedup='mtime'``). See the module docstring for the
  cursor precondition.
  """
  def __init__(
      self,
      file_pattern: str,
      interval=60.0,
      termination: TerminationCondition = None,
      dedup: str = 'path'):
    """Initializes the transform.

    Args:
      file_pattern: the file glob to watch.
      interval: seconds (or a :class:`Duration`) between polls.
      termination: a per-input :class:`TerminationCondition`; defaults to
        watching forever (:func:`never`).
      dedup: ``'path'`` (default) for exact dedup by file path, or ``'mtime'``
        for the O(1) modification-time cursor.
    """
    super().__init__()
    if dedup not in _DEDUP_MODES:
      raise ValueError(
          'dedup must be one of %s, got %r' % (_DEDUP_MODES, dedup))
    self._file_pattern = file_pattern
    self._interval = (
        interval if isinstance(interval, Duration) else Duration(interval))
    self._termination = termination or never()
    self._dedup = dedup

  def expand(self, pbegin):
    watch = (
        Watch.growth_of(_match_files).with_poll_interval(
            self._interval).with_dedup_key(
                lambda metadata: metadata.path,
                StrUtf8Coder()).with_termination_per_input(self._termination))
    if self._dedup == 'mtime':
      watch = watch.with_timestamp_cursor()
    return (
        pbegin
        | 'Pattern' >> beam.Create([self._file_pattern])
        | 'Watch' >> watch
        | 'ExtractMetadata' >> beam.Map(lambda pattern_file: pattern_file[1]))


class ReadFromTextContinuously(beam.PTransform):
  """Continuously watches ``file_pattern`` and emits the lines of each new file.

  Composes :class:`WatchForNewFiles` with
  :class:`~apache_beam.io.textio.ReadAllFromText`, so new files are read as they
  appear. With ``dedup='mtime'`` discovery avoids the unbounded per-file dedup
  state of :class:`apache_beam.io.fileio.MatchContinuously`. See
  :class:`WatchForNewFiles` for the dedup modes and the cursor precondition.
  """
  def __init__(
      self,
      file_pattern: str,
      interval=60.0,
      termination: TerminationCondition = None,
      dedup: str = 'path'):
    super().__init__()
    self._file_pattern = file_pattern
    self._interval = interval
    self._termination = termination
    self._dedup = dedup

  def expand(self, pbegin):
    return (
        pbegin
        | 'WatchFiles' >> WatchForNewFiles(
            self._file_pattern, self._interval, self._termination, self._dedup)
        | 'Paths' >> beam.Map(lambda metadata: metadata.path)
        | 'ReadText' >> ReadAllFromText())
