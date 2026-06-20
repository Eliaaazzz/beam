"""Rate-limited infinite counting source for the Dataflow soak job.

Lives in a real module (shipped to workers via --extra_package) so pickle
resolves the classes by reference on Dataflow workers; classes defined in
__main__ fail to unpickle there.
"""

import logging
import time

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


class TickCheckpointMark(CheckpointMark):
  def __init__(self, last_index, anchor_time):
    self.last_index = last_index
    self.anchor_time = anchor_time  # schedule survives resume via the mark


class TickReader(UnboundedReader):
  """Record ``i`` becomes available at ``anchor + i / rate``; never EOF."""
  def __init__(self, source, start_index, anchor_time):
    self._s = source
    self._next = start_index
    self._anchor = anchor_time
    self._current = None

  def _available_at(self, i):
    return self._anchor + i / self._s.rate_per_sec

  def start(self):
    return self.advance()

  def advance(self):
    if time.time() < self._available_at(self._next):
      return False  # not yet available; the wrapper defers and resumes
    self._current, self._next = self._next, self._next + 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(self._available_at(self._current))

  def get_watermark(self):
    if self._current is None:
      return MIN_TIMESTAMP
    return Timestamp(self._available_at(self._current))

  def get_checkpoint_mark(self):
    return TickCheckpointMark(self._next - 1, self._anchor)


class TickSource(UnboundedSource):
  """Counts 0, 1, 2, ... forever at ``rate_per_sec``; no stop count."""
  def __init__(self, rate_per_sec):
    self.rate_per_sec = rate_per_sec

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    if checkpoint_mark is None:
      return TickReader(self, 0, time.time())
    return TickReader(
        self, checkpoint_mark.last_index + 1, checkpoint_mark.anchor_time)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class CountCheckpointMark(CheckpointMark):
  def __init__(self, last_index):
    self.last_index = last_index


class CountReader(UnboundedReader):
  """Always has a next record; counts up forever unless ``stop_at`` is set."""
  def __init__(self, source, start_index):
    self._s = source
    self._next = start_index
    self._current = None
    self._current_ts = None

  def start(self):
    return self.advance()

  def advance(self):
    if self._s.stop_at is not None and self._next >= self._s.stop_at:
      return False  # exhausted -> get_watermark reports MAX_TIMESTAMP
    self._current, self._next = self._next, self._next + 1
    self._current_ts = Timestamp(time.time())
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return self._current_ts

  def get_watermark(self):
    if self._s.stop_at is not None and self._next >= self._s.stop_at:
      return MAX_TIMESTAMP
    if self._current_ts is None:
      return MIN_TIMESTAMP
    return self._current_ts

  def get_checkpoint_mark(self):
    return CountCheckpointMark(self._next - 1)


class CountSource(UnboundedSource):
  """Counts 0, 1, 2, ... as fast as the runner reads; no availability gating.

  The reader never reports "no data now", so the wrapper only checkpoints at
  its own per-bundle caps (max_records_per_bundle / max_read_time_seconds).
  ``stop_at`` bounds the count for local smoke runs; None reads to infinity.
  """
  def __init__(self, stop_at=None):
    self.stop_at = stop_at

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    if checkpoint_mark is None:
      return CountReader(self, 0)
    return CountReader(self, checkpoint_mark.last_index + 1)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class LogProgress(beam.DoFn):
  """Logs every Nth element. Fused with the read, so progress shows in worker
  logs even when the watermark (and thus any windowed output) is stuck."""
  def __init__(self, every):
    self._every = every

  def process(self, element):
    if element % self._every == 0:
      logging.info('count progress element=%d wall=%.3f', element, time.time())
    yield element


class LogWindowCount(beam.DoFn):
  def process(self, kv, window=beam.DoFn.WindowParam):
    _, count = kv
    logging.info(
        'tick window [%s, %s) count=%d',
        window.start.to_utc_datetime(),
        window.end.to_utc_datetime(),
        count)
    yield count
