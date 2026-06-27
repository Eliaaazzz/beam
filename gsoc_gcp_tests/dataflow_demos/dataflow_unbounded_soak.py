"""Submit one indefinitely running UnboundedSource job to Dataflow.

The source has no stop count: ``advance()`` always has a next record once its
wall-clock availability time passes, so the read never reaches EOF and the job
runs until it is cancelled. Elements carry event-time timestamps; a one-minute
fixed window count is logged so watermark progress and element flow are
visible in the worker logs.

The source classes live in the ``soak_ticks`` module (soak_pkg/), shipped to
workers via --extra_package; classes defined in __main__ fail to unpickle on
Dataflow workers (plain-pickle coder paths resolve them by reference).

Submits asynchronously (no wait_until_finish) and prints the job id.

Run from sdks/python (or with PYTHONPATH pointing there) so the branch SDK
with apache_beam.io.unbounded_source is importable:

    python dataflow_unbounded_soak.py --rate=25 --poll_interval=1 \
        --runner=DataflowRunner --project=... --region=... --streaming \
        --extra_package=<path>/beam-soak-ticks-0.0.1.tar.gz ...
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'soak_pkg'))

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

from soak_ticks import LogWindowCount
from soak_ticks import TickSource

from apache_beam.io.unbounded_source import ReadFromUnboundedSource


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--rate', type=float, default=25.0)
  parser.add_argument('--poll_interval', type=float, default=1.0)
  known, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=options)
  _ = (
      p
      | 'ReadForever' >> ReadFromUnboundedSource(
          TickSource(known.rate), poll_interval=known.poll_interval)
      | 'WindowPerMinute' >> beam.WindowInto(FixedWindows(60))
      | 'KeyAll' >> beam.Map(lambda v: ('ticks', v))
      | 'CountPerWindow' >> beam.combiners.Count.PerKey()
      | 'LogCount' >> beam.ParDo(LogWindowCount()))

  result = p.run()
  job_id = result.job_id() if hasattr(result, 'job_id') else None
  print('SUBMITTED job_id=%s' % job_id)
  opts = options.get_all_options()
  print(
      'console: https://console.cloud.google.com/dataflow/jobs/%s/'
      '%s?project=%s' % (opts['region'], job_id, opts['project']))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
