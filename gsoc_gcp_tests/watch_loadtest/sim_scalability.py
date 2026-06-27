"""Empirical probe of the 776af22 Watch dedup behavior.

Drives the restriction tracker round-by-round (as the unit tests do) to measure
two things the green tests do not surface:

  1. Default path: per-key `completed` growth and per-checkpoint encoded bytes
     for a continuous-discovery (relisting) source.  Is memory + serialization
     really still O(N) every round by default?

  2. with_dedup_horizon on a FULL-RELISTING source (the transform's headline
     file-discovery use case): how many times is each output emitted, and does
     the watermark advance?
"""
import collections

from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import _GrowthRestrictionTracker
from apache_beam.io.watch import _GrowthStateCoder
from apache_beam.io.watch import _PollingGrowthState
from apache_beam.io.watch import never
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Duration, Timestamp


def _ts(v, t):
  return TimestampedValue(v, Timestamp(t))


def _initial():
  return _PollingGrowthState(
      collections.OrderedDict(), None, never().for_new_input(Timestamp(0), 'in'))


def _tracker(state, poll, horizon=None):
  h = None if horizon is None else Duration(horizon)
  return _GrowthRestrictionTracker(
      state, poll, StrUtf8Coder(), never(), lambda: 0.0, h)


def drive(poll_factory, rounds, horizon=None):
  """Runs `rounds` poll rounds, threading the residual. Returns per-round
  (emitted_values, completed_size, encoded_bytes, residual_watermark)."""
  coder = _GrowthStateCoder(StrUtf8Coder(), never())
  state = _initial()
  log = []
  for r in range(rounds):
    tracker = _tracker(state, poll_factory(r), horizon)
    holder = ['in', None]
    tracker.try_claim(holder)
    emitted = [o.value for o in holder[1][1]]
    _, residual = tracker.try_split(0)
    if isinstance(residual, _PollingGrowthState):
      size = len(residual.completed)
      nbytes = len(coder.encode(residual))
      wm = residual.poll_watermark
    else:
      size, nbytes, wm = -1, -1, None
    log.append((emitted, size, nbytes, wm))
    state = residual
  return log


# A continuous-discovery source that RELISTS the whole growing directory each
# round: round r returns files f0..fr, each stamped at its creation round.
def relist_factory(r):
  def poll(_):
    return PollResult.incomplete([_ts('f%d' % i, i) for i in range(r + 1)])
  return poll


print('=' * 70)
print('PART 1 — DEFAULT (no horizon): does state stay O(N) every round?')
print('=' * 70)
log = drive(relist_factory, rounds=12, horizon=None)
print('%5s %9s %14s %10s' % ('round', 'completed', 'encoded_bytes', 'watermark'))
for r, (emitted, size, nbytes, wm) in enumerate(log):
  print('%5d %9d %14d %10s' % (r, size, nbytes, wm))
print('-> completed grows by 1/round (unbounded); encoded bytes grow O(N) on')
print('   EVERY checkpoint, including idle rounds. The idle-round map-reuse')
print('   optimization saves the in-memory copy, NOT the per-checkpoint encode.')

print()
print('=' * 70)
print('PART 2 — with_dedup_horizon on the SAME full-relisting source')
print('=' * 70)
emit_counts = collections.Counter()
log = drive(relist_factory, rounds=12, horizon=3)
print('%5s %10s %-32s' % ('round', 'watermark', 'emitted_this_round'))
for r, (emitted, size, nbytes, wm) in enumerate(log):
  for v in emitted:
    emit_counts[v] += 1
  print('%5d %10s %-32s' % (r, wm, emitted))
print()
print('Per-value total emission count (1 == correct exactly-once):')
for v in sorted(emit_counts, key=lambda s: int(s[1:])):
  flag = '' if emit_counts[v] == 1 else '   <-- DUPLICATED %dx' % emit_counts[v]
  print('  %-5s emitted %2d time(s)%s' % (v, emit_counts[v], flag))
dupes = sum(c - 1 for c in emit_counts.values() if c > 1)
print()
print('-> total DUPLICATE emissions: %d over 12 rounds' % dupes)
print('   and note whether the watermark ever advances past the horizon.')
