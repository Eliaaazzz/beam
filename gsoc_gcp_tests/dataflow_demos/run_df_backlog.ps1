param(
  [Parameter(Mandatory=$true)][string]$LogFile,
  [string]$JobName = "elia-unboundedsource-backlog-0614-east1",
  [string]$Mode = "backlog",
  [string]$Count = "100000",
  [string]$Rate = "1000000",
  [string]$MaxRecords = "10000",
  [string]$Region = "us-east1"
)
# Submit the instrumented backlog/no-backlog demo to Dataflow (apache-beam-testing).
# Finite source: reads Count records (logging CHECKPOINT/FINALIZE/THROUGHPUT) then
# drains; cancel after the evidence appears.
$ErrorActionPreference = "Continue"
$root = "C:\Users\Aufb\Desktop\beam"
$py = "$root\dfvenv311\Scripts\python.exe"
$env:PATH = "$root\dfvenv311\Scripts;$env:PATH"
$env:PYTHONPATH = "$root\sdks\python"
Set-Location "$root\sdks\python"

$argsList = @(
  "-m", "apache_beam.examples.unbounded_source_backlog_demo",
  "--mode=$Mode",
  "--count=$Count",
  "--rate=$Rate",
  "--max-records-per-bundle=$MaxRecords",
  "--runner=DataflowRunner",
  "--project=apache-beam-testing",
  "--region=$Region",
  "--temp_location=gs://io-performance-temp/temp",
  "--staging_location=gs://io-performance-temp/staging",
  "--sdk_location=$root\sdks\python\dist\apache_beam-2.75.0.dev0.tar.gz",
  "--sdk_container_image=apache/beam_python3.11_sdk:2.74.0",
  "--streaming",
  "--enable_streaming_engine",
  "--worker_machine_type=e2-standard-2",
  "--num_workers=1",
  "--max_num_workers=1",
  "--experiments=use_runner_v2",
  "--job_name=$JobName"
)

"START $(Get-Date -Format o)  job_name=$JobName mode=$Mode count=$Count rate=$Rate" | Out-File -FilePath $LogFile -Encoding utf8
& $py @argsList 2>&1 | Out-File -FilePath $LogFile -Append -Encoding utf8
"EXITCODE=$LASTEXITCODE END $(Get-Date -Format o)" | Out-File -FilePath $LogFile -Append -Encoding utf8
