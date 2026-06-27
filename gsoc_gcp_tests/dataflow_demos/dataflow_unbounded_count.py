"""Submit an indefinitely running hot UnboundedSource counting job to Dataflow.

The source (``CountSource`` in soak_ticks) always has a next record, so the
read never waits for data and never reaches EOF: it counts 0, 1, 2, ... to
infinity as fast as the runner pulls. The wrapper therefore only checkpoints
at its own per-bundle caps (10k records / 10s by default) via
``defer_remainder()`` with no resume delay. This isolates the self-checkpoint
resume path from the poll-delay path that the rate-limited TickSource soak
exercises.

Two progress signals land in worker logs:
  - ``LogProgress`` is fused with the read and logs every Nth element, so read
    liveness is visible even if the watermark is stuck.
  - ``LogWindowCount`` logs a per-minute windowed count, which only fires when
    the watermark advances past each window.

The source classes live in the ``soak_ticks`` module (soak_pkg/), shipped to
workers via --extra_package; classes defined in __main__ fail to unpickle on
Dataflow workers.

Submits asynchronously (no wait_until_finish) and prints the job id. A local
smoke run bounds the count instead:

    python dataflow_unbounded_count.py --stop_at=50000 --log_every=10000

Run from sdks/python (or with PYTHONPATH pointing there) so the branch SDK
with apache_beam.io.unbounded_source is importable:

    python dataflow_unbounded_count.py \
        --runner=DataflowRunner --project=... --region=... --streaming \
        --extra_package=<path>/beam_soak_ticks-0.0.2.tar.gz ...
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'soak_pkg'))

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

from soak_ticks import CountSource
from soak_ticks import LogProgress
from soak_ticks import LogWindowCount

from apache_beam.io.unbounded_source import ReadFromUnboundedSource


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--stop_at', type=int, default=None)
  parser.add_argument('--log_every', type=int, default=100000)
  known, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=options)
  _ = (
      p
      | 'ReadCountForever' >> ReadFromUnboundedSource(
          CountSource(stop_at=known.stop_at))
      | 'LogEveryN' >> beam.ParDo(LogProgress(known.log_every))
      | 'WindowPerMinute' >> beam.WindowInto(FixedWindows(60))
      | 'KeyAll' >> beam.Map(lambda v: ('count', v))
      | 'CountPerWindow' >> beam.combiners.Count.PerKey()
      | 'LogCount' >> beam.ParDo(LogWindowCount()))

  result = p.run()
  if known.stop_at is not None:
    result.wait_until_finish()
    return
  job_id = result.job_id() if hasattr(result, 'job_id') else None
  print('SUBMITTED job_id=%s' % job_id)
  opts = options.get_all_options()
  print(
      'console: https://console.cloud.google.com/dataflow/jobs/%s/'
      '%s?project=%s' % (opts['region'], job_id, opts['project']))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
