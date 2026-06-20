"""Local playground for the real-time UnboundedSource and poll_interval.

Drives the real RealTimeCountingSource reader and prints each emitted element.
When the reader catches up to the wall clock and reports no data, it waits
``--poll-interval`` seconds before resuming, the same way the SDF wrapper
defers on Dataflow / Flink. A small poll_interval gives a fast, near-continuous
refresh; a larger one gives visible bursts every poll_interval seconds.

Note: the local DirectRunner ignores the real wrapper's resume delay, so this
script mimics that defer locally for visualization. The real wrapper honoring
poll_interval is exercised on Dataflow.
"""

import argparse
import ctypes
import sys
import time

try:
  ctypes.windll.kernel32.SetConsoleTitleW("BeamLiveStream")
except Exception:
  pass

from apache_beam.examples.unbounded_counting_source import RealTimeCountingSource

parser = argparse.ArgumentParser()
parser.add_argument('--rate', type=float, default=5.0, help='elements per second')
parser.add_argument(
    '--poll-interval',
    type=float,
    default=0.2,
    help='seconds to wait after catching up before resuming (mimics the wrapper defer)')
args = parser.parse_args()

src = RealTimeCountingSource(sys.maxsize, rate_per_sec=args.rate)
reader = src.create_reader(None, None)

print("Apache Beam  -  Python UnboundedSource   (RealTimeCountingSource)")
print("rate = %g elements/sec    poll_interval = %g s    (infinite; Ctrl+C to stop)"
      % (args.rate, args.poll_interval))
print("-" * 84)


def emit():
  idx = reader.get_current()
  ts = reader.get_current_timestamp()
  now = time.time()
  wall = time.strftime("%H:%M:%S", time.gmtime(now)) + ".%03d" % int((now % 1) * 1000)
  ev = ts.to_utc_datetime().strftime("%H:%M:%S.%f")[:-3]
  print(
      "wall(UTC)=%sZ   count=%-7d  event_time=%sZ   lag=%+.2fs" %
      (wall, idx, ev, now - float(ts)),
      flush=True)


if reader.start():
  emit()
while True:
  if reader.advance():
    emit()
  else:
    if args.poll_interval >= 0.5:
      print("  ... caught up to wall clock, deferring %.1fs (poll_interval) ..."
            % args.poll_interval, flush=True)
    time.sleep(args.poll_interval)
