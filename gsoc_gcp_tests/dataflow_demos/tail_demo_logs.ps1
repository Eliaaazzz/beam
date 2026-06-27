# Live-tail the per-second "Window" worker logs of a Dataflow job into this
# terminal, so you can screen-record a clean, continuously-refreshing stream.
# Usage:  C:\Users\Aufb\Desktop\beam\tail_demo_logs.ps1 -JobId <job-id>
param([string]$JobId = "2026-06-16_01_39_49-14429597668871776044")

[System.Threading.Thread]::CurrentThread.CurrentUICulture = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
$ErrorActionPreference = "SilentlyContinue"
$seen = @{}
$filter = "resource.type=""dataflow_step"" AND resource.labels.job_id=""$JobId"" AND jsonPayload.message:""Window"""

Write-Host "Live per-second UnboundedSource windows  -  Dataflow job $JobId" -ForegroundColor Cyan
Write-Host "(event time = wall clock, Max climbs forever, Ctrl+C to stop)" -ForegroundColor DarkGray
Write-Host ("-" * 96)

while ($true) {
  $lines = @(& gcloud logging read $filter --project=apache-beam-testing --freshness=1m --limit=15 --order=desc --format="value(jsonPayload.message)" 2>$null)
  if ($lines.Count -gt 1) { [array]::Reverse($lines) }
  foreach ($l in $lines) {
    if ($l -and -not $seen.ContainsKey($l)) { $seen[$l] = 1; Write-Host $l }
  }
  Start-Sleep -Seconds 2
}
