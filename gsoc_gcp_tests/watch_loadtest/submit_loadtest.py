"""Submit a heavy Watch load test to Dataflow Runner v2.

  python submit_loadtest.py cursor    # O(1) high-water-mark dedup
  python submit_loadtest.py default   # exact hash dedup (unbounded state)

Few inputs each emit a large batch every second, so the default dedup set grows
fast and every checkpoint re-serializes it; the cursor keeps one timestamp. The
per-key output count over a fixed wall-clock budget shows whether a mode keeps
up: a mode throttled by its growing checkpoint state completes fewer rounds.
"""
import sys
import time

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils.timestamp import Duration

from beam_watch.validation import WriteKeyCount, load_poll
from beam_watch.watch import Watch, after_total_of

SDIST = 'beam_watch_pkg/dist/beam_watch-0.1.0.tar.gz'
KEYS = 4           # few inputs, so each accumulates a large per-key state
DURATION = 240     # seconds each input polls before terminating
RESULTS = 'gs://io-performance-temp/watch-load/results'


def main():
  mode = sys.argv[1] if len(sys.argv) > 1 else 'cursor'
  job_name = 'watch-heavy-%s-%d' % (mode, int(time.time()))
  out_prefix = '%s/%s' % (RESULTS, job_name)
  options = PipelineOptions([
      '--runner=DataflowRunner',
      '--project=apache-beam-testing',
      '--region=us-east1',
      '--job_name=' + job_name,
      '--staging_location=gs://io-performance-temp/watch-load/staging',
      '--temp_location=gs://io-performance-temp/watch-load/temp',
      '--experiments=use_runner_v2',
      '--extra_packages=' + SDIST,
      '--streaming',
      '--num_workers=4',
      '--max_num_workers=4',
  ])
  p = beam.Pipeline(options=options)
  watch = Watch.growth_of(load_poll).with_poll_interval(
      Duration(1)).with_output_coder(StrUtf8Coder()).with_termination_per_input(
          after_total_of(Duration(DURATION)))
  if mode == 'cursor':
    watch = watch.with_timestamp_cursor()
  keys = ['key-%d' % i for i in range(KEYS)]
  _ = (
      p | 'keys' >> beam.Create(keys)
      | 'watch' >> watch
      | 'global_window' >> beam.WindowInto(
          GlobalWindows(),
          trigger=AfterWatermark(),
          accumulation_mode=AccumulationMode.DISCARDING)
      | 'count_per_key' >> beam.combiners.Count.PerKey()
      | 'write' >> beam.ParDo(WriteKeyCount(out_prefix)))
  result = p.run()
  print('JOB_NAME', job_name)
  print('JOB_ID', result.job_id())
  print('RESULT_PREFIX', out_prefix)
  print('MODE', mode)


if __name__ == '__main__':
  main()
