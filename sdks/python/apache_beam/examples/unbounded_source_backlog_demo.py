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

"""Measures UnboundedSource wrapper behavior with and without a backlog.

Runs the same ``UnboundedSource -> ParDo`` pipeline twice: once with every
record available up front (backlog) and once with records arriving at a fixed
rate (no backlog). Prints a side-by-side report of throughput, bundle size,
checkpoint frequency, and finalization count.

A reader with data available keeps claiming until ``max_records_per_bundle``
or ``max_read_time_seconds`` cuts a checkpoint. A reader with no data cuts a
checkpoint immediately and resumes after ``poll_interval``. A deep backlog
yields few large bundles. A trickling source yields one small bundle per poll.

DirectRunner/FnApiRunner ignores the resume delay, so the no-backlog run
checkpoints more often than ``poll_interval`` suggests; keep
``--total_elements`` modest there (large totals can hit a CPython recursion
limit in its per-sub-bundle metrics consolidation).

Usage (DirectRunner, from sdks/python):

    python -m apache_beam.examples.unbounded_source_backlog_demo \\
        --total_elements=100 --rate=50 --poll_interval=0.25

Flink local with LOOPBACK (the resume delay is honored there):

    python -m apache_beam.examples.unbounded_source_backlog_demo \\
        --total_elements=400 --rate=50 --poll_interval=0.25 \\
        --runner=FlinkRunner --environment_type=LOOPBACK \\
        --flink_job_server_jar=<path-to-job-server-shadowJar> \\
        --parallelism=1 --streaming
"""

# pytype: skip-file

import argparse
import os
import tempfile
import time

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

NAMESPACE = 'unbounded_demo'


class DemoCheckpointMark(CheckpointMark):
  """Mark for ``DemoSource``; records each finalization to a file.

  finalize_checkpoint runs after the bundle's state sampler stops, so Beam
  metrics recorded there are dropped. Appending to a file also survives the
  by-value pickling of ``__main__`` classes under direct script execution.
  """
  def __init__(self, last_index, anchor_time, finalize_path):
    self.last_index = last_index
    # The availability schedule survives resume via the mark.
    self.anchor_time = anchor_time
    self.finalize_path = finalize_path

  def finalize_checkpoint(self):
    with open(self.finalize_path, 'a', encoding='utf-8') as f:
      f.write('%f,%d\n' % (time.time(), self.last_index))


class DemoReader(UnboundedReader):
  """Record ``i`` is available at ``anchor + max(0, i - backlog) / rate``."""
  def __init__(self, source, start_index, anchor_time):
    self._s = source
    self._next = start_index
    self._anchor = anchor_time
    self._current = None
    self._checkpoints = Metrics.counter(NAMESPACE, 'checkpoints_created')

  def _available_at(self, i):
    late = max(0, i - self._s.backlog_prefill)
    return self._anchor + late / self._s.rate_per_sec

  def start(self):
    return self.advance()

  def advance(self):
    if self._next >= self._s.total_elements:
      return False  # Exhausted; the watermark reports MAX_TIMESTAMP.
    if time.time() < self._available_at(self._next):
      return False  # Available later; the wrapper defers with poll_interval.
    self._current, self._next = self._next, self._next + 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(self._available_at(self._current))

  def get_watermark(self):
    if self._next >= self._s.total_elements:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return Timestamp(self._available_at(self._current))

  def get_checkpoint_mark(self):
    # Counts every checkpoint cut: poll deferrals, cap deferrals, and EOF.
    self._checkpoints.inc()
    return DemoCheckpointMark(
        self._next - 1, self._anchor, self._s.finalize_path)


class DemoSource(UnboundedSource):
  """Emits ``range(total_elements)`` on the reader's availability schedule."""
  def __init__(
      self, total_elements, backlog_prefill, rate_per_sec, finalize_path):
    self.total_elements = total_elements
    self.backlog_prefill = backlog_prefill
    self.rate_per_sec = rate_per_sec
    self.finalize_path = finalize_path

  def split(self, desired_num_splits, options=None):
    return [self]  # A single shard keeps the per-bundle stats readable.

  def create_reader(self, options, checkpoint_mark):
    if checkpoint_mark is None:
      return DemoReader(self, 0, time.time())
    return DemoReader(
        self, checkpoint_mark.last_index + 1, checkpoint_mark.anchor_time)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class BundleStats(beam.DoFn):
  """Fused with the read; its bundles match the wrapper's checkpoint cycles."""
  def __init__(self):
    self.elements = Metrics.counter(NAMESPACE, 'elements')
    self.bundles = Metrics.counter(NAMESPACE, 'bundles')
    self.bundle_size = Metrics.distribution(NAMESPACE, 'bundle_size')
    self.bundle_millis = Metrics.distribution(NAMESPACE, 'bundle_millis')
    self.runtime_ms = Metrics.distribution(NAMESPACE, 'runtime_ms')
    self.latency_ms = Metrics.distribution(NAMESPACE, 'latency_ms')

  def start_bundle(self):
    self._n = 0
    self._t0 = time.time()
    self.bundles.inc()
    self.runtime_ms.update(int(self._t0 * 1000))

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    self._n += 1
    self.elements.inc()
    self.latency_ms.update(int(time.time() * 1000) - timestamp.micros // 1000)
    yield element

  def finish_bundle(self):
    now = time.time()
    self.bundle_size.update(self._n)
    self.bundle_millis.update(int((now - self._t0) * 1000))
    self.runtime_ms.update(int(now * 1000))


def run_once(label, total, backlog, knobs, pipeline_args):
  fd, finalize_path = tempfile.mkstemp(prefix='unbounded_demo_finalize_')
  os.close(fd)
  try:
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    _ = (
        p
        | 'Read' >> ReadFromUnboundedSource(
            DemoSource(total, backlog, knobs.rate, finalize_path),
            poll_interval=knobs.poll_interval,
            max_records_per_bundle=knobs.max_records_per_bundle,
            max_read_time_seconds=knobs.max_read_time_seconds)
        | 'BundleStats' >> beam.ParDo(BundleStats()))
    wall_start = time.time()
    result = p.run()
    result.wait_until_finish()
    wall = time.time() - wall_start

    q = result.metrics().query(MetricsFilter().with_namespace(NAMESPACE))
    counters = {c.key.metric.name: c.result for c in q['counters']}
    dists = {d.key.metric.name: d.result for d in q['distributions']}
    with open(finalize_path, encoding='utf-8') as f:
      finalized = sum(1 for _ in f)
  finally:
    os.remove(finalize_path)

  elements = counters.get('elements', 0)
  # Worker-side active span; wall time includes pipeline startup.
  rt = dists.get('runtime_ms')
  span = ((rt.max - rt.min) /
          1000.0 if rt and rt.count and rt.max > rt.min else wall)
  bs = dists.get('bundle_size')
  return {
      'label': label,
      'wall_secs': round(wall, 2),
      'elements': elements,
      'throughput_eps': round(elements / max(span, 1e-9), 1),
      'bundles': counters.get('bundles', 0),
      'bundle_size_mean': round(bs.mean, 2) if bs and bs.count else None,
      'bundle_size_max': bs.max if bs and bs.count else None,
      'checkpoints': counters.get('checkpoints_created', 0),
      'checkpoints_per_sec': round(
          counters.get('checkpoints_created', 0) / max(wall, 1e-9), 2),
      'finalize_calls': finalized,
      'latency_ms_mean': (
          round(dists['latency_ms'].mean, 1)
          if 'latency_ms' in dists and dists['latency_ms'].count else None),
  }


def main(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--total_elements', type=int, default=100)
  parser.add_argument(
      '--rate', type=float, default=50.0, help='Arrival rate, records/sec.')
  parser.add_argument('--poll_interval', type=float, default=0.25)
  parser.add_argument('--max_records_per_bundle', type=int, default=10000)
  parser.add_argument('--max_read_time_seconds', type=float, default=10.0)
  knobs, pipeline_args = parser.parse_known_args(argv)

  reports = [
      run_once(
          'BACKLOG (all prefilled)',
          knobs.total_elements,
          knobs.total_elements,
          knobs,
          pipeline_args),
      run_once(
          'NO BACKLOG (%.0f rec/s)' % knobs.rate,
          knobs.total_elements,
          0,
          knobs,
          pipeline_args),
  ]
  keys = [
      'wall_secs',
      'elements',
      'throughput_eps',
      'bundles',
      'bundle_size_mean',
      'bundle_size_max',
      'checkpoints',
      'checkpoints_per_sec',
      'finalize_calls',
      'latency_ms_mean',
  ]
  print(
      '\n%-22s %24s %24s' %
      ('metric', reports[0]['label'], reports[1]['label']))
  for k in keys:
    print('%-22s %24s %24s' % (k, reports[0][k], reports[1][k]))


if __name__ == '__main__':
  main()
