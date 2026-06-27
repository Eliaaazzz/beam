param(
  [Parameter(Mandatory=$true)][string]$LogFile,
  [string]$JobName = "elia-unboundedsource-soak-0611-r2",
  [string]$Rate = "25",
  [string]$PollInterval = "1.0",
  [string]$Region = "us-central1"
)
# Submit the indefinite UnboundedSource soak job to Dataflow (apache-beam-testing).
# Async submit only: the job keeps running until cancelled.
$ErrorActionPreference = "Continue"
$root = "C:\Users\Aufb\Desktop\beam"
$py = "$root\dfvenv311\Scripts\python.exe"
$env:PATH = "$root\dfvenv311\Scripts;$env:PATH"
$env:PYTHONPATH = "$root\sdks\python"
Set-Location "$root\sdks\python"

$argsList = @(
  "$root\dataflow_unbounded_soak.py",
  "--rate=$Rate",
  "--poll_interval=$PollInterval",
  "--runner=DataflowRunner",
  "--project=apache-beam-testing",
  "--region=$Region",
  "--temp_location=gs://io-performance-temp/temp",
  "--staging_location=gs://io-performance-temp/staging",
  "--sdk_location=$root\sdks\python\dist\apache_beam-2.75.0.dev0.tar.gz",
  "--extra_package=$root\soak_pkg\dist\beam_soak_ticks-0.0.1.tar.gz",
  "--sdk_container_image=apache/beam_python3.11_sdk:2.74.0",
  "--streaming",
  "--enable_streaming_engine",
  "--worker_machine_type=e2-standard-2",
  "--num_workers=1",
  "--max_num_workers=1",
  "--experiments=use_runner_v2",
  "--job_name=$JobName"
)

"START $(Get-Date -Format o)  job_name=$JobName rate=$Rate poll=$PollInterval region=$Region" | Out-File -FilePath $LogFile -Encoding utf8
& $py @argsList 2>&1 | Out-File -FilePath $LogFile -Append -Encoding utf8
"EXITCODE=$LASTEXITCODE END $(Get-Date -Format o)" | Out-File -FilePath $LogFile -Append -Encoding utf8
