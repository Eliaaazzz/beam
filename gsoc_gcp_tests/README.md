# GSoC GCP test scripts

Dataflow load tests and demos backing the Python Watch transform and the
continuous file source. These run against Dataflow Runner v2 and are kept here
for reference; absolute paths from the original runs were made relative, so set
your own project, region, and GCS buckets before running.

## watch_loadtest/

Scalability comparison of the two Watch dedup modes.

- `submit_loadtest.py` — submits two jobs to Dataflow: `cursor` (O(1)
  high-water-mark dedup) and `default` (exact hash dedup, unbounded state). A
  few inputs each emit a large batch every second, so the default dedup set
  grows fast while the cursor keeps a single timestamp.
- `submit_validation.py` — correctness run: each input is fully re-listed every
  round; asserts each output is emitted exactly once.
- `benchmark_state.py`, `sim_scalability.py` — local simulations quantifying the
  per-input dedup state and per-checkpoint encoded size over time.
- `monitor_load.sh`, `monitor_heavy.sh`, `monitor_heavy2.sh` — poll both jobs'
  state and fetch the per-key result counts from GCS.
- `beam_watch_pkg/` — the standalone `beam_watch` package staged onto Dataflow
  workers via `--extra_package` (build the sdist, then point `SDIST` at it).

## dataflow_demos/

UnboundedSource and streaming demos.

- `dataflow_unbounded_count.py`, `dataflow_unbounded_soak.py` — indefinitely
  running UnboundedSource jobs exercising the self-checkpoint resume path.
- `dataflow_smoke.py` — minimal submit/read/write smoke test.
- `unbounded_source_backlog_demo.py`, `_df_persecond_demo.py`,
  `live_stream_demo.py` — backlog and per-second throughput demos.
- `run_df_*.ps1`, `monitor_df_count.ps1`, `tail_demo_logs.ps1` — submit and
  monitor wrappers.
