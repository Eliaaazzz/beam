# Demoing the Python UnboundedSource wrapper to the mentor

Audience: Yi Hu (GitHub `Abacn`), Apache Beam committer and GSoC mentor.
Deliverable under demo: D1, the Python `UnboundedSource` SDF wrapper (PR #38724)
plus the portable ValidatesRunner test (PR #38892).

The goal is to convince a committer that the source is correct, behaves like a
real streaming source, and meets the operational expectations the mentor stated.
Every number below is captured from an actual run on the date shown, and every
claim is scoped to what the code actually proves.

## 1. What the mentor asked to see

Operational characteristics, from mentor chat:
> A UnboundedSource then with a ParDo, check throughput, bundle size,
> checkpointing frequency, etc, when there are Backlog or not.

Real-time counting demo:
- Element event times must track the current wall clock, not a fixed historical
  base.
- The stream must be an ever-growing infinite counter, not a constant per-window
  value.
- Use a counting source, not the bespoke rate-limited tick source.
- Per fixed window, report count, min, and max.

## 2. Why this demo is shaped this way

Beam's own streaming examples set the bar. `streaming_wordcount.py`,
`complete/game/leader_board.py`, and `complete/game/game_stats.py` are the live
streaming examples: they read an unbounded source, apply event-time windows, and
emit per-window output as the watermark advances. `complete/game/user_score.py`
and `hourly_team_score.py` are batch examples and are not the model here. The
real-time counting demo is the minimal streaming form: unbounded source,
event-time `FixedWindows`, windowed aggregate.

The concept a demo must convey is that the watermark is the progress of
event-time completeness as processing time advances, and event-time windows fire
as the watermark crosses their end. The climbing per-window max is the visible
signal that event time is advancing and the stream never ends.

Scope note on TestStream. TestStream is the Beam-native way to test windowing,
triggers, and late data deterministically, but it is itself a synthetic source,
so it would replace the source under test. It cannot demonstrate this custom
`UnboundedSource`. The source is demonstrated by a live run on a real runner, by
unit tests, and by the ValidatesRunner suite.

Normal usage note. End users write `pcoll = p | beam.io.Read(MySource())`. The
`poll_interval`, `max_records_per_bundle`, and `max_read_time_seconds` arguments
used below are wrapper control knobs exposed for the demo so the backlog and
checkpoint behavior is observable; they are not something user code normally
sets.

## 3. The demo, in four layers

### Layer 1: live real-time streaming run (the headline)

Asset: `apache_beam/examples/unbounded_counting_demo.py`, live on Dataflow as job
`elia-unboundedsource-counting-0613b-east1`
(`2026-06-13_06_12_04-12041425630314217811`, us-east1, Streaming Engine, Runner
v2, one `e2-standard-2` worker).

Reproducible multi-window local run (DirectRunner, 2026-06-14, terminates after
1000 elements):
```
python -m apache_beam.examples.unbounded_counting_demo \
    --count=1000 --rate=25 --poll-interval=1.0
```
Captured output:
```
Window [03:38:40, 03:38:50) Count=186, Min=0,   Max=185   partial first window
Window [03:38:50, 03:39:00) Count=250, Min=186, Max=435   full
Window [03:39:00, 03:39:10) Count=250, Min=436, Max=685   full
Window [03:39:10, 03:39:20) Count=250, Min=686, Max=935   full
Window [03:39:20, 03:39:30) Count=64,  Min=936, Max=999   partial last (count exhausted)
```
What it proves: event times track the wall clock (current timestamps), the
counter is unbounded (Min and Max climb strictly, 0 through 999), it is a
counting source, and each event-time window fires once as the watermark advances.
For complete windows after startup, Count equals rate times the ten-second
window (250 = 25 x 10). The first and last windows are partial because the run
starts mid-window and stops at 1000.

### Layer 2: operational characteristics under backlog vs no backlog

Asset: `apache_beam/examples/unbounded_source_backlog_demo.py` reading
`InstrumentedCountingSource`, which logs `CHECKPOINT` from `get_checkpoint_mark`
and `FINALIZE` from the mark's `finalize_checkpoint`, plus a `THROUGHPUT` line
from the downstream `ParDo`. The checkpoint and finalize frequency, the
checkpoint bundle size (`bundle_records`, measured at the source not at a fused
downstream bundle), and the throughput are therefore all reproducible from
worker logs on any runner. Run one mode per invocation:

```
python -m apache_beam.examples.unbounded_source_backlog_demo \
    --mode=backlog --count=10000 --rate=50000 --max-records-per-bundle=2000
python -m apache_beam.examples.unbounded_source_backlog_demo \
    --mode=no_backlog --count=40 --rate=50 --poll-interval=0.25
```

Captured (DirectRunner, 2026-06-14):

```
BACKLOG     CHECKPOINT mode=backlog    bundle_records=2000   (5 of these for 10000)
            FINALIZE per checkpoint
            THROUGHPUT eps ~= 130000 (runner-bound, not source-bound)
NO_BACKLOG  CHECKPOINT mode=no_backlog bundle_records=0 or 1 (1165 of these for 40)
            FINALIZE per checkpoint
            THROUGHPUT eps ~= 50 (equals the source rate)
```

What it proves, and what answers the mentor's ask directly: bundle and
checkpoint boundaries are driven by data availability. With a backlog the reader
always has data, so the wrapper fills a bundle up to `max_records_per_bundle`
(`bundle_records=2000`) and checkpoints rarely, at high throughput. With no
backlog the reader frequently has no data, so the wrapper cuts a checkpoint via
`defer_remainder` at each availability gap, producing many tiny bundles
(`bundle_records` 0 or 1) and throughput equal to the source rate.

Runner caveat to state up front: the DirectRunner and FnApiRunner ignore the SDF
resume delay, so the no-backlog reader busy-loops into many no-data checkpoints
(1165 for 40 elements) rather than backing off by `poll_interval`; keep the
no-backlog count small there to stay under the FnApiRunner metrics-consolidation
recursion limit. On a runner that honors the delay, such as Flink LOOPBACK or
Dataflow, the no-backlog case backs off between reads and checkpoints roughly
once per element. The contrast in shape is the point, not the absolute counts.

### Layer 3: correctness and resume, demonstrated

Checkpoint and resume evidence: the no-backlog run above went through 1165
checkpoint-and-resume cycles with a matching `FINALIZE` per `CHECKPOINT` while
delivering all elements. The backlog run exercised the other cut path, the
per-bundle cap, checkpointing every `max_records_per_bundle` records. Exact
`0..N` ordering with no gaps or duplicates across resumes is asserted by the
deterministic end-to-end unit tests (`test_read_through_wrapper_delivers_all_in_order`
and `test_backlog_read_delivers_all_in_order`), not by reading logs.

Note on duplication: the demo runs show no observed loss or duplication, and unit
tests assert exact ordering. The Python wrapper does not expose a record-id
deduplication API, so exactly-once is not claimed; what is shown is correct
checkpoint and resume with no observed duplication.

Unit tests, no flaky timing: 43 wrapper tests in `unbounded_source_test.py` plus
6 source tests in `unbounded_counting_source_test.py`, covering watermark
monotonicity, checkpoint and resume, EOF to MAX watermark, at-most-once
finalization, and rate validation.

ValidatesRunner, official PostCommit on PR #38892 head `d9667e3`:
- Dataflow ValidatesRunner: PASS on 3.10 and 3.14.
- Flink ValidatesRunner: PASS on 3.10 and 3.14.
- Spark ValidatesRunner: PASS on 3.10 and 3.14.
- PreCommit Portable Python and Prism Python: PASS on 3.10 and 3.14.
- Two failing checks are unrelated to this change: `Python_ML` PreCommit on 3.13
  and a 3.13 test-results aggregate in the ML area; they do not touch the
  UnboundedSource code path.
- The portable test `test_unbounded_source_read` asserts unbounded output, the
  exact elements `[0..4]`, and EOF watermark propagation through `FixedWindows`
  plus `GroupByKey`. It does not assert a finalization side effect; finalization
  is covered by the unit tests above.
- PR #38892 is currently in a conflicting state and needs a rebase before merge;
  that is independent of the demo.

Manual Dataflow run, separate from CI: Beam's `EndToEndTest` ran 5 of 5 to
`JOB_STATE_DONE` on `TestDataflowRunner` with Runner v2 and streaming, using the
branch sdist. Job ids are recorded for each of the five tests.

### Layer 4: durability evidence (operational, separate from the counting demo)

A separate multi-day soak ran on Dataflow, job
`elia-unboundedsource-soak-0611-east1`
(`2026-06-10_08_50_12-4502727689029402648`, us-east1) for about 58 hours
(created 2026-06-10T15:50Z, cancelled 2026-06-13T01:56Z). It used the rate-limited
tick source and logged only a per-minute count, which is constant by design, so
it is operational durability evidence for the wrapper's continuous defer and
resume path, not part of the counting demo. It shows the wrapper holds up under
days of continuous self-checkpointing without the job failing.

## 4. Scope guardrail

The source emits strictly in index order, so its watermark is perfect and there
is no out-of-order or late data. The demo therefore does not show late panes,
speculative panes, or complex triggers. Those are downstream windowing features
owned by core Beam, not by this source. Keeping the demo scoped to read, event
time, watermark, checkpoint and resume, finalization, and backlog behavior is
itself a signal that the D1 boundary is understood.

## 5. How to deliver it

The mentor interacts asynchronously on chat and the PR. Recommended package: a
short writeup or PR comment organized by the four layers, with the live job link
so the mentor can open the running windows directly, the captured backlog table,
the multi-window output, the test counts, the ValidatesRunner check links, and
the soak job id and duration. Offer a short live walkthrough as a follow-up.

Message style: plain text, warm and humble in tone, no em dashes, no parentheses,
no per-line format markers.
