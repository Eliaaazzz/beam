# Submit the per-second-window UnboundedSource demo to Dataflow, then print the
# Cloud Console log URL and the cancel command. Uses dfvenv311 directly, so it
# does not matter which venv is currently active.
$ErrorActionPreference = "Continue"
$root = "C:\Users\Aufb\Desktop\beam"
$py   = "$root\dfvenv311\Scripts\python.exe"
$env:PYTHONPATH = "$root\sdks\python"
$JobName = "elia-persecond-demo-" + (Get-Date -Format "MMddHHmm")

$argList = @(
  "$root\_df_persecond_demo.py",
  "--rate=25", "--poll-interval=1", "--window-size=1",
  "--runner=DataflowRunner",
  "--project=apache-beam-testing",
  "--region=us-east1",
  "--temp_location=gs://io-performance-temp/temp",
  "--staging_location=gs://io-performance-temp/staging",
  "--sdk_location=$root\sdks\python\dist\apache_beam-2.75.0.dev0.tar.gz",
  "--sdk_container_image=apache/beam_python3.11_sdk:2.74.0",
  "--streaming", "--enable_streaming_engine",
  "--worker_machine_type=e2-standard-2", "--num_workers=1", "--max_num_workers=1",
  "--experiments=use_runner_v2", "--no_wait_until_finish",
  "--job_name=$JobName"
)

Write-Host "Submitting per-second Dataflow job (staging takes ~2 minutes)..." -ForegroundColor Cyan
$out = & $py @argList 2>&1
$text = ($out | Out-String)
Write-Host $text

$m = [regex]::Match($text, 'Created job with id: \[(.+?)\]')
if ($m.Success) {
  $jid = $m.Groups[1].Value
  Write-Host "==================== SUBMITTED ====================" -ForegroundColor Green
  Write-Host "JOB ID : $jid"
  Write-Host ""
  Write-Host "1) Wait ~3-4 min for workers to start." -ForegroundColor Cyan
  Write-Host "2) Open this log page (sign in with the Google account your gcloud uses):" -ForegroundColor Cyan
  Write-Host ("   https://console.cloud.google.com/dataflow/jobs/us-east1/{0}?project=apache-beam-testing" -f $jid) -ForegroundColor Yellow
  Write-Host "   In LOGS, filter for: Window     (a new line appears every second, Max climbing)" -ForegroundColor Cyan
  Write-Host "3) Screen-record that browser ~60s." -ForegroundColor Cyan
  Write-Host ""
  Write-Host "When done, CANCEL the job (saves money):" -ForegroundColor Cyan
  Write-Host ("   gcloud dataflow jobs cancel {0} --region=us-east1 --project=apache-beam-testing" -f $jid) -ForegroundColor Yellow
} else {
  Write-Host "Could not parse a job id. Paste the output above to inspect it." -ForegroundColor Red
}
