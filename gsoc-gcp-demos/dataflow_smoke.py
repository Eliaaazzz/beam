"""Minimal Dataflow smoke pipeline: beam.Create -> square -> WriteToText (GCS).

Confirms the SDK can submit to Dataflow and read/write GCS. Run AFTER
`gcloud auth login` and confirming access to the target project.

Example (fill in your project + a bucket you can write to):

    python dataflow_smoke.py \
        --runner=DataflowRunner \
        --project=apache-beam-testing \
        --region=us-central1 \
        --temp_location=gs://io-performance-temp/tmp \
        --staging_location=gs://io-performance-temp/staging \
        --output=gs://io-performance-temp/smoke/out \
        --job_name=unbounded-smoke

Notes:
  * Python uses --temp_location (snake_case), not Java's --tempLocation.
  * DataflowRunner needs --project and a writable --staging_location/--temp_location.
  * Run DirectRunner first to sanity-check with no cloud:
        python dataflow_smoke.py --output=./smoke_out
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output', required=True, help='Output path prefix (local or gs://).')
  known, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  # Ship the main session so any local defs are available on workers.
  options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=options) as p:
    (
        p
        | 'Create' >> beam.Create(range(1, 21))
        | 'Square' >> beam.Map(lambda x: '%d -> %d' % (x, x * x))
        | 'Write' >> beam.io.WriteToText(known.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
