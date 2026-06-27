"""Submit the Watch dedup-horizon validation to Dataflow Runner v2.

Stock Beam 2.74.0 client + worker; the Watch transform ships as the beam_watch
extra package. Each round re-lists a perpetual old output; with the suppression
fix it is emitted once per seed, with the GC-only bug it would re-emit every
round. Per-seed counts are written to GCS when the watermark reaches MAX (every
seed has terminated), so the result is durable and unambiguous.
"""
import time

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils.timestamp import Duration

from beam_watch.validation import WriteSeedCounts, growing_poll
from beam_watch.watch import Watch, after_total_of

SDIST = 'beam_watch_pkg/dist/beam_watch-0.1.0.tar.gz'
SEEDS = ['seed-0', 'seed-1', 'seed-2']
RESULTS = 'gs://io-performance-temp/watch-val/results'


def main():
  job_name = 'watch-val-%d' % int(time.time())
  out_prefix = '%s/%s' % (RESULTS, job_name)
  options = PipelineOptions([
      '--runner=DataflowRunner',
      '--project=apache-beam-testing',
      '--region=us-east1',
      '--job_name=' + job_name,
      '--staging_location=gs://io-performance-temp/watch-val/staging',
      '--temp_location=gs://io-performance-temp/watch-val/temp',
      '--experiments=use_runner_v2',
      '--extra_packages=' + SDIST,
      '--streaming',
      '--num_workers=1',
      '--max_num_workers=2',
  ])
  p = beam.Pipeline(options=options)
  _ = (
      p | 'seeds' >> beam.Create(SEEDS)
      | 'watch' >> Watch.growth_of(growing_poll).with_poll_interval(
          Duration(10)).with_dedup_horizon(Duration(15)).with_output_coder(
              StrUtf8Coder()).with_termination_per_input(
                  after_total_of(Duration(90)))
      | 'global_window' >> beam.WindowInto(
          GlobalWindows(),
          trigger=AfterWatermark(),
          accumulation_mode=AccumulationMode.DISCARDING)
      | 'group_by_seed' >> beam.GroupByKey()
      | 'write_counts' >> beam.ParDo(WriteSeedCounts(out_prefix)))
  result = p.run()
  print('JOB_NAME', job_name)
  print('JOB_ID', result.job_id())
  print('RESULT_PREFIX', out_prefix)


if __name__ == '__main__':
  main()
