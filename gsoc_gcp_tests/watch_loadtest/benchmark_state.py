"""Quantify the scalability gain of with_dedup_horizon vs the unbounded default.

Simulates continuous file discovery: a directory that grows by B files per poll
and is fully re-listed every round. Measures the per-input dedup state (entries)
and the per-checkpoint encoded size (bytes, re-serialized every poll) over time,
for the unbounded default vs a bounded horizon.
"""
import collections

from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Duration, Timestamp

from beam_watch.watch import (PollResult, _GrowthRestrictionTracker,
                              _GrowthStateCoder, _PollingGrowthState, never)

B = 50        # new files discovered per poll round
ROUNDS = 80   # poll rounds
HORIZON = 10 * B  # keep ~10 rounds of event time


def _initial():
  return _PollingGrowthState(
      collections.OrderedDict(), None, never().for_new_input(Timestamp(0), 'd'))


def relist_poll(r):
  # Full re-list of a directory grown to (r+1)*B files, each at event time = idx.
  def poll(_):
    return PollResult.incomplete(
        [TimestampedValue('file-%d' % i, Timestamp(i)) for i in range((r + 1) * B)])
  return poll


def run(horizon):
  coder = _GrowthStateCoder(StrUtf8Coder(), never())
  state = _initial()
  h = None if horizon is None else Duration(horizon)
  out = {}
  for r in range(ROUNDS):
    tr = _GrowthRestrictionTracker(
        state, relist_poll(r), StrUtf8Coder(), never(), lambda: 0.0, h)
    holder = ['d', None]
    tr.try_claim(holder)
    _, state = tr.try_split(0)
    if isinstance(state, _PollingGrowthState):
      out[r + 1] = (len(state.completed), len(coder.encode(state)))
  return out


default = run(None)
bounded = run(HORIZON)

print('Continuous discovery: +%d files/round, full re-list each round, %d rounds'
      % (B, ROUNDS))
print('Horizon = %ds (~10 rounds of event time)\n' % HORIZON)
print('%6s | %-22s | %-22s | %s' %
      ('round', 'default (entries/bytes)', 'horizon (entries/bytes)', 'bytes saved'))
for r in (10, 20, 40, 80):
  de, db = default[r]
  he, hb = bounded[r]
  print('%6d | %8d / %9d | %8d / %9d | %5.1fx (%d -> %d B)' %
        (r, de, db, he, hb, db / hb, db, hb))

dtot = sum(b for _, b in default.values())
btot = sum(b for _, b in bounded.values())
print('\nCumulative bytes serialized across all %d checkpoints:' % ROUNDS)
print('  default: %d B   horizon: %d B   (%.1fx less I/O over the run)'
      % (dtot, btot, dtot / btot))
print('\nDefault entries grow without bound (= every distinct file ever seen);')
print('horizon stays flat at the sliding window. The gap widens forever as the')
print('job runs longer.')
