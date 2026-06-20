param(
  [string]$JobId = "2026-06-12_17_32_13-4425011018216645165",
  [string]$Region = "us-east1",
  [string]$Project = "apache-beam-testing",
  [string]$LogFile = "C:\Users\Aufb\Desktop\beam\_df_count_monitor.log",
  [int]$MaxMinutes = 18
)
$ErrorActionPreference = "Continue"
"MONITOR START $(Get-Date -Format o) job=$JobId region=$Region" | Out-File $LogFile -Encoding utf8

for ($i = 1; $i -le $MaxMinutes; $i++) {
  Start-Sleep -Seconds 60
  $state = & gcloud dataflow jobs describe $JobId --region=$Region --project=$Project --format="value(currentState)" 2>$null
  "$(Get-Date -Format o) minute=$i state=$state" | Out-File $LogFile -Append -Encoding utf8

  # Single-token filter: PS 5.1 splits embedded quoted phrases with spaces.
  $pf = 'resource.type="dataflow_step" AND resource.labels.job_id="' + $JobId + '" AND logName:"worker" AND jsonPayload.message:element'
  $progress = & gcloud logging read $pf --project=$Project --limit=5 --format="value(timestamp,jsonPayload.message)" 2>$null
  if ($progress) {
    "minute=$i PROGRESS:" | Out-File $LogFile -Append -Encoding utf8
    $progress | Out-File $LogFile -Append -Encoding utf8
  } else {
    "minute=$i no_progress_yet" | Out-File $LogFile -Append -Encoding utf8
  }

  $errors = & gcloud logging read "resource.type=`"dataflow_step`" AND resource.labels.job_id=`"$JobId`" AND severity>=ERROR" --project=$Project --limit=3 --format="value(timestamp,textPayload)" 2>$null
  if ($errors) {
    "minute=$i ERRORS:" | Out-File $LogFile -Append -Encoding utf8
    $errors | Out-File $LogFile -Append -Encoding utf8
  }

  # Stop once two polls saw progress: liveness is established.
  if ($progress) {
    if ($script:sawProgress) {
      "MONITOR DONE (progress confirmed twice) $(Get-Date -Format o)" | Out-File $LogFile -Append -Encoding utf8
      exit 0
    }
    $script:sawProgress = $true
  }
  if ($state -notmatch "RUNNING|PENDING|QUEUED" -and $state) {
    "MONITOR DONE (terminal state $state) $(Get-Date -Format o)" | Out-File $LogFile -Append -Encoding utf8
    exit 0
  }
}
"MONITOR END (timeout) $(Get-Date -Format o)" | Out-File $LogFile -Append -Encoding utf8
