#!/bin/bash
CJOB=2026-06-24_08_45_05-17501676526076065162
DJOB=2026-06-24_08_45_23-5060488636914380336
CPRE=gs://io-performance-temp/watch-load/results/watch-heavy-cursor-1782315890
DPRE=gs://io-performance-temp/watch-load/results/watch-heavy-default-1782315908
R=us-east1
P=apache-beam-testing
PY="C:/Users/Aufb/Desktop/beam/dfvenv311/Scripts/python.exe"

info() { gcloud dataflow jobs describe "$1" --region=$R --project=$P --format="value(currentState,createTime,currentStateTime)" 2>/dev/null | tr '\t' ' '; }
terminal() { case "$1" in JOB_STATE_DONE|JOB_STATE_FAILED|JOB_STATE_CANCELLED|JOB_STATE_DRAINED|JOB_STATE_UPDATED) return 0;; *) return 1;; esac; }

for i in $(seq 1 20); do
  sleep 55
  ci=$(info $CJOB); di=$(info $DJOB)
  cs=$(echo "$ci" | awk '{print $1}'); ds=$(echo "$di" | awk '{print $1}')
  echo "[iter $i $(date +%H:%M:%S)] cursor: $ci | default: $di"
  if terminal "$cs" && terminal "$ds"; then echo "both terminal"; break; fi
done

echo "=== per-key output counts (GCS) ==="
TMP="./heavy2res"
rm -rf "$TMP"; mkdir -p "$TMP/cursor" "$TMP/default"
gcloud storage cp "$CPRE/result-*.txt" "$TMP/cursor/" 2>/dev/null
gcloud storage cp "$DPRE/result-*.txt" "$TMP/default/" 2>/dev/null
"$PY" - "$TMP" <<'PYEOF'
import glob, os, sys
base = sys.argv[1]
for mode in ('cursor', 'default'):
  files = sorted(glob.glob(os.path.join(base, mode, 'result-*.txt')))
  counts = []
  for f in files:
    for line in open(f):
      kv = dict(p.split('=') for p in line.split() if '=' in p)
      if 'count' in kv:
        counts.append(int(kv['count']))
  print('%-8s keys_written=%d total_outputs=%d per_key=%s'
        % (mode, len(files), sum(counts), counts))
PYEOF
echo "=== final states ==="
echo "cursor:  $(info $CJOB)"
echo "default: $(info $DJOB)"
