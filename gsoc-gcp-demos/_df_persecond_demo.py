"""Submit the real-time per-second UnboundedSource demo to Dataflow.

All Dataflow options are baked in so the command stays short:
  python _df_persecond_demo.py --poll-interval 1      smooth per-second
  python _df_persecond_demo.py --poll-interval 20     bursts every 20s (shows poll_interval)

Reuses the committed demo's StatsCombineFn / LogStatsDoFn and the real
RealTimeCountingSource + ReadFromUnboundedSource wrapper. Only --rate,
--poll-interval and --window-size are knobs; everything else is fixed below.
"""

import argparse
import logging
import sys
import time

import apache_beam as beam
from apache_beam.examples.unbounded_counting_demo import LogStatsDoFn
from apache_beam.examples.unbounded_counting_demo import StatsCombineFn
from apache_beam.examples.unbounded_counting_source import RealTimeCountingSource
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

ROOT = r"C:\Users\Aufb\Desktop\beam"


def run():
  parser = argparse.ArgumentParser()
  parser.add_argument('--rate', type=float, default=25.0)
  parser.add_argument('--poll-interval', type=float, default=1.0)
  parser.add_argument('--window-size', type=int, default=1)
  args = parser.parse_args()

  job_name = "elia-persecond-" + time.strftime("%m%d%H%M%S")
  df_opts = [
      '--runner=DataflowRunner',
      '--project=apache-beam-testing',
      '--region=us-east1',
      '--temp_location=gs://io-performance-temp/temp',
      '--staging_location=gs://io-performance-temp/staging',
      '--sdk_location=' + ROOT + r'\sdks\python\dist\apache_beam-2.75.0.dev0.tar.gz',
      '--sdk_container_image=apache/beam_python3.11_sdk:2.74.0',
      '--streaming',
      '--enable_streaming_engine',
      '--worker_machine_type=e2-standard-2',
      '--num_workers=1',
      '--max_num_workers=1',
      '--experiments=use_runner_v2',
      '--job_name=' + job_name,
  ]

  p = beam.Pipeline(options=PipelineOptions(df_opts))
  _ = (
      p
      | 'ReadFromUnbounded' >> ReadFromUnboundedSource(
          RealTimeCountingSource(sys.maxsize, rate_per_sec=args.rate),
          poll_interval=args.poll_interval)
      | 'WindowPerSecond' >> beam.WindowInto(FixedWindows(args.window_size))
      | 'CombineStats' >> beam.CombineGlobally(StatsCombineFn()).without_defaults()
      | 'LogWindowStats' >> beam.ParDo(LogStatsDoFn()))

  result = p.run()  # async submit, does not block
  print("\n==================== SUBMITTED ====================")
  print("job_name      :", job_name)
  print("poll_interval :", args.poll_interval, "s   window:", args.window_size,
        "s   rate:", args.rate, "/s")
  try:
    jid = result.job_id()
    print("job_id        :", jid)
    print("logs page     : https://console.cloud.google.com/dataflow/jobs/"
          "us-east1/%s?project=apache-beam-testing" % jid)
    print("cancel with   : gcloud dataflow jobs cancel %s "
          "--region=us-east1 --project=apache-beam-testing" % jid)
  except Exception:
    print("(see 'Created job with id' above for the job id)")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
