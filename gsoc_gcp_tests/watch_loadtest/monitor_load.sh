#!/bin/bash
CJOB=2026-06-24_07_02_46-4858432504457834380
DJOB=2026-06-24_07_03_01-3136728607890371713
CPRE=gs://io-performance-temp/watch-load/results/watch-load-cursor-1782309751
DPRE=gs://io-performance-temp/watch-load/results/watch-load-default-1782309770
R=us-east1
P=apache-beam-testing
PY="C:/Users/Aufb/Desktop/beam/dfvenv311/Scripts/python.exe"

st() { gcloud dataflow jobs describe "$1" --region=$R --project=$P --format="value(currentState)" 2>/dev/null; }
dedup_max() {
  gcloud dataflow metrics list "$1" --region=$R --project=$P 2>/dev/null \
    | grep -iA4 "dedup_set_size" | grep -iE "max:" | head -1 | tr -d ' '
}

terminal() { case "$1" in JOB_STATE_DONE|JOB_STATE_FAILED|JOB_STATE_CANCELLED|JOB_STATE_DRAINED) return 0;; *) return 1;; esac; }

for i in $(seq 1 16); do
  sleep 55
  cs=$(st $CJOB); ds=$(st $DJOB)
  echo "[iter $i $(date +%H:%M:%S)] cursor=$cs dedup_max=[$(dedup_max $CJOB)] | default=$ds dedup_max=[$(dedup_max $DJOB)]"
  if terminal "$cs" && terminal "$ds"; then echo "both terminal"; break; fi
done

echo "=== final states: cursor=$(st $CJOB) default=$(st $DJOB) ==="
TMP="./loadres"
rm -rf "$TMP"; mkdir -p "$TMP/cursor" "$TMP/default"
gcloud storage cp "$CPRE/result-*.txt" "$TMP/cursor/" 2>/dev/null
gcloud storage cp "$DPRE/result-*.txt" "$TMP/default/" 2>/dev/null
echo "=== GCS RESULT SUMMARY ==="
"$PY" - "$TMP" <<'PYEOF'
import glob, os, sys
base = sys.argv[1]
for mode in ('cursor', 'default'):
  files = glob.glob(os.path.join(base, mode, 'result-*.txt'))
  total = distinct = 0
  for f in files:
    for line in open(f):
      kv = dict(p.split('=') for p in line.split() if '=' in p)
      total += int(kv.get('total', 0)); distinct += int(kv.get('distinct', 0))
  rate = total / 120.0
  print('%-8s keys=%d total_outputs=%d distinct=%d exactly_once=%s ~rate=%.0f/s'
        % (mode, len(files), total, distinct, distinct == total, rate))
PYEOF
