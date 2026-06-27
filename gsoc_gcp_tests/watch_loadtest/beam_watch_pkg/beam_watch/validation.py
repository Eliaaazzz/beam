"""Load-test poll and result writer for the Dataflow Watch validation.

Importable (not __main__) so they pickle by reference and run on Dataflow
workers.
"""
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from beam_watch.watch import PollResult

BATCH = 15000  # new outputs produced per poll round per input (high cardinality).


def load_poll(key):
  """High-volume incremental poll: each round emits BATCH unique outputs at
  strictly increasing event times derived from the wall clock.

  With ``with_timestamp_cursor`` the cursor keeps one timestamp and the state is
  O(1). With the default hash dedup the completed set grows by BATCH every
  round, so every checkpoint re-serializes a larger and larger state.
  """
  now_us = Timestamp.now().micros
  return PollResult.incomplete([
      TimestampedValue(
          '%s-%d-%d' % (key, now_us, i), Timestamp(micros=now_us + i))
      for i in range(BATCH)
  ])


class WriteKeyCount(beam.DoFn):
  """Writes one GCS file per key with the total outputs it processed in the
  wall-clock budget. A mode that cannot keep up with its growing checkpoint
  state completes fewer poll rounds and so emits fewer outputs."""
  def __init__(self, output_prefix):
    self._output_prefix = output_prefix

  def process(self, element):
    key, count = element
    path = '%s/result-%s.txt' % (self._output_prefix, key)
    line = 'key=%s count=%d\n' % (key, count)
    with FileSystems.create(path) as handle:
      handle.write(line.encode('utf-8'))
    yield line.strip()
