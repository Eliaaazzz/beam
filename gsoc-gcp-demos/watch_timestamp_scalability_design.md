# Watch transform — timestamp & scalability: production-readiness design

Status: design for changes to `sdks/python/apache_beam/io/watch.py`.
Reference: `sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/Watch.java`.
Derived from a 5-agent research pass (Java reference semantics, Python
timestamp/watermark audit, Python scalability audit, Beam SDF sibling patterns,
web research). 45 findings; key citations inline.

## 1. Problem statement

The Python `Watch` transform is an unbounded splittable DoFn that, per input
element, repeatedly calls a user `PollFn`, dedups never-seen outputs by a 128-bit
hash, emits them, advances a `ManualWatermarkEstimator`, and self-checkpoints via
`defer_remainder(poll_interval)`. Two classes of problem block production
readiness.

### Problem A — timestamp/watermark correctness (must fix)

- **A1 (late-data hazard).** With no explicit poll watermark, the transform
  advances the watermark to the *minimum event time of this round's new outputs*
  (`watch.py:432-433`), made permanent by the monotonic guard
  (`_set_watermark_if_greater`, `watch.py:596-600`) and carried estimator state.
  If a *later* round surfaces a brand-new output with an *earlier* event time
  (out-of-order arrival, backfill, multi-source merge, clock skew), that output
  is emitted at its true earlier timestamp while the watermark already passed it,
  so downstream `WindowInto`+`GroupByKey` drops it as late. Reproduced: round 1
  `a@10` ⇒ wm=10; round 2 `b@5` ⇒ emitted at 5 while wm=10 ⇒ dropped.
  This is **reference-consistent with Java** (`Watch.java:940-947`, "earliest
  timestamp of new elements"). Java tolerates it because poll authors are
  expected to supply an explicit watermark for out-of-order sources.
- **A2 (processing-time leaks into event time).** `PollResult._normalize`
  (`watch.py:115-126`) stamps raw outputs with `Timestamp.now()` (wall clock)
  when `timestamp is None`, and `incomplete()/complete()` default to `None`. For
  bare-value polls, event time = ingestion time. (Note: `_normalize` hoists one
  `default_ts` per call, so there is no per-output jitter — the audit's jitter
  concern does not occur.)
- **A3 (undocumented contract).** The module does not state that the inferred
  watermark is unsafe for non-monotonic polls, nor that dedup is value-identity
  only (a re-seen value's corrected timestamp is discarded).

### Problem B — scalability (mitigate the cheap parts, document the rest)

Per input key, `_PollingGrowthState.completed` (`watch.py:232-242`) gains one
16-byte-hash → Timestamp entry for every distinct output ever emitted and is
never pruned (~200-250 B resident/output; e.g. 1M files/day × 30 days ≈ 6-7 GB
pinned for one key). On top of that footprint:

- **B1.** Full map copied every round: `merged = OrderedDict(restriction.completed)`
  (`watch.py:450`) — O(N)/round ⇒ O(N·R).
- **B2.** Full map re-serialized on every checkpoint (`_GrowthStateCoder.encode`,
  `watch.py:327-336`), and `defer_remainder` fires every round ⇒ O(N) bytes/round.
- **B3.** Whole poll result re-hashed every round (`watch.py:415-421`), hashing
  the full output value (no Java-style `outputKeyFn`; `_key_coder = output_coder`).
- **B4.** `PollFn` runs under the tracker lock (`watch.py:410`) — a slow poll
  blocks checkpoints.
- **B5.** No intra-key parallelism (`split` yields whole restriction,
  `try_split` declines dynamic splits, `restriction_size==1`).

The unbounded footprint equals Java's acknowledged TODO (#18459) and
`fileio.MatchContinuously`'s documented "scales poorly". Exact value-identity
dedup fundamentally requires remembering every value unless the poll contract is
strengthened.

**Scope of this change:** fix A1/A2/A3 and B1/B2-cheap-wins; add an opt-in bound
for the footprint (the #18459 escape hatch); document B3/B4/B5 and the residual
limit. Do not change the (reference-correct) watermark math.

## 2. Timestamp/watermark fix

Keep Java's mechanism (Manual estimator, monotonic guard, MAX sentinel) and
default (min of new outputs), but make the safe path reachable and the unsafe
default loud. Per-case policy:

| Poll returns | Watermark held this round |
|---|---|
| (a) explicit watermark `W` | `max(current, W)` — user's promise; authoritative, never inferred. |
| (b) no watermark + new outputs | `max(current, min(event time of new outputs))` — Java default; **safe only for monotonic enumerations**. |
| (c) no watermark + no new outputs | unchanged (no advance, no regress) — matches `Watch.java:964-966`. |
| (d) `complete()` | release to `MAX_TIMESTAMP`; stop polling. |

- **A1:** do not weaken the monotonic guard (that would illegally regress the
  output watermark). Out-of-order ⇒ caller supplies (a). Add a per-instance
  rate-limited warning when a new output is emitted with
  `timestamp < current_watermark`, converting silent loss into a diagnosable
  signal without changing emission semantics. Emit-then-advance ordering is kept
  (matches `unbounded_source.py:850-854`).
- **A2:** keep `Timestamp.now()` default (the "discovered-now" common case) but
  document it precisely in `incomplete()/complete()/_normalize` and the module
  docstring.
- **A3:** add a "Watermark and event-time contract" section to the module
  docstring; state the dedup value-identity semantics and the deterministic-coder
  precondition.

## 3. Scalability fix

Options for the unbounded `completed` map:

- **A. Keep exact unbounded dedup; cut wasted per-round work; document
  (Java-parity).** No API change, zero correctness risk. Removes the O(N) copy on
  idle rounds. Encode stays O(N) (dictated by the restriction-coder contract).
- **B. Time-bounded dedup (`with_dedup_horizon`).** Evict entries first-seen
  below `watermark - horizon`. Safe **only if the poll never re-returns an
  output once that output has aged out of the horizon — at any timestamp**
  (cursor / time-windowed enumeration). Note this is *stronger* than "never
  return below the watermark": a re-returned value at a *newer* timestamp would
  be re-emitted as a silent on-time duplicate (caught in Codex review). It is
  therefore unsafe for full-relisting polls and must be documented as such.
  Mirrors Java's unimplemented TODO (`Watch.java:992`, #18459). Bounds footprint
  to the horizon window. Opt-in, off by default; `horizon` validated
  non-negative.
- **C. User cursor / `since` token.** Exact and ≈O(1) state, but a `PollFn`
  signature change. Larger API surface; deferred.
- **D. Bloom filter.** Bounded memory but false-positive data loss. Unacceptable
  as default.

**Recommendation:** ship **A as the default** plus **B as an opt-in
`with_dedup_horizon(Duration)`** (the #18459 escape hatch, off by default so the
default path is byte-for-byte today's exact behavior). Reuse the parent map when
a round adds nothing and no eviction applies (kills B1 on idle rounds — the
steady state). Do not attempt a delta restriction-coder redesign (too large for a
single-file change; B2 stays O(N) per the contract, but is bounded once a horizon
is set).

**B4/B5: document as known limitations, do not fix now.** Both are structural to
the SDF tracker design and to "fan out by input element" (Java is identical).
Fixing them is a redesign, not a hardening, and would balloon the review surface.
Document the user-side mitigation (bounded/timeout-safe poll; parallelism scales
with input-key count; high-per-key-growth users should prefer cursor/notification
sources, same guidance as `MatchContinuously`).

## 4. Implementation plan (single file, surgical)

1. Module + `PollResult` docstrings — event-time/watermark contract (A2/A3).
2. `process` — throttled late-emission warning before each yield (A1); keep
   emit-then-advance with a clarifying comment.
3. `try_claim` — replace the eager O(N) copy with `_build_completed`: reuse the
   parent map when nothing changed and no horizon; otherwise copy + merge delta +
   evict every entry below `watermark - horizon` (a full scan, order-independent,
   so explicit held-back watermarks can't leave stale entries un-evicted) (B1,
   B-opt-in).
4. New `Watch.with_dedup_horizon(Duration)` threaded through `_WatchGrowthDoFn`
   and `_GrowthRestrictionTracker`; default `None` ⇒ today's exact behavior.
5. Docstrings on `Watch`/`_WatchGrowthDoFn`/`_GrowthRestrictionTracker` —
   poll-under-lock, per-key parallelism, dedup-state growth & the horizon escape.

No change to `_HashCode128Coder`, `_TimestampedValueCoder`, termination
conditions, `try_split`, `truncate`, `is_bounded`, the replay path, or the
watermark math — those match the reference.

## 5. Test plan (watch_test.py)

- Out-of-order across rounds: round1 `a@10`, round2 `b@5` ⇒ `b` claimed/emitted
  at 5, inferred watermark 5 < prior 10 (locks current reference behavior).
- Late-emission warning fires (throttled) for the out-of-order case end-to-end.
- Explicit watermark makes the out-of-order case safe (`with_watermark`).
- No-new-outputs holds the watermark (case c).
- `_normalize` uses processing time when `timestamp is None` (single hoisted ts).
- `completed` coder round-trip preserves insertion order (existing; keep).
- Idle round reuses the parent `completed` object (identity check ⇒ no O(N) copy).
- `with_dedup_horizon` evicts stale hashes once the watermark advances; default
  `None` evicts nothing; above-horizon outputs still dedup.

## 6. Anticipated review objections (and pre-emptions)

1. "You only log, you didn't fix late data." — A1 is reference-correct; the only
   watermark-safe fix for out-of-order data is an explicit watermark, now the
   documented contract + tested. Changing the heuristic would stall every
   monotonic source's watermark — a worse regression.
2. "Processing-time default is a footgun." — intended "discovered-now" semantic
   (matches Java `addTimestamp`); now documented with a one-line opt-out
   (`TimestampedValue`/`timestamp=`).
3. "Dedup map still unbounded by default." — exact dedup requires it; Java is
   identical (#18459). We remove the pure-waste per-round copy and ship the safe
   opt-in GC that #18459 asks for; the residual limit is documented.
4. "`with_dedup_horizon` can re-emit." — off by default; the docstring states
   the strong precondition (an aged-out output is never re-returned, at any
   timestamp) and explicitly calls out full-relisting polls as unsafe; evicts
   only first-seen-strictly-below-cutoff using the per-entry timestamp Java
   already stores for this purpose; `horizon` is validated non-negative. A
   regression test documents the re-emission-after-eviction boundary so the
   contract is enforced by test, not just prose.
5. "Poll under lock can hang the bundle." — structural, present in Java;
   documented with the bounded/timeout-safe-poll mitigation.
6. "No intra-key parallelism." — Java is identical; parallelism is by input-key
   count; documented with cursor/notification guidance.
