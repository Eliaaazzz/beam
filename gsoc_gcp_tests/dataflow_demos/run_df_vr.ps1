param(
  [Parameter(Mandatory=$true)][string]$Tests,
  [string]$Region = "us-central1",
  [Parameter(Mandatory=$true)][string]$LogFile,
  [string]$Numprocesses = "1",
  [string]$KExpr = "",
  [string]$Timeout = ""
)
# Drive Beam's own EndToEndTest against Dataflow (TestDataflowRunner) on
# apache-beam-testing, staging to the shared io-performance-temp bucket.
$ErrorActionPreference = "Continue"
$root = "C:\Users\Aufb\Desktop\beam"
$py = "$root\dfvenv311\Scripts\python.exe"
$env:PATH = "$root\dfvenv311\Scripts;$env:PATH"
Set-Location "$root\sdks\python"

$opts = @(
  "--runner=TestDataflowRunner",
  "--project=apache-beam-testing",
  "--region=$Region",
  "--temp_location=gs://io-performance-temp/temp",
  "--staging_location=gs://io-performance-temp/staging",
  "--sdk_location=dist/apache_beam-2.75.0.dev0.tar.gz",
  "--sdk_container_image=apache/beam_python3.11_sdk:2.74.0",
  "--streaming",
  "--enable_streaming_engine",
  "--worker_machine_type=e2-standard-2",
  "--num_workers=1",
  "--max_num_workers=1",
  "--experiments=use_runner_v2"
) -join " "

$pytestArgs = @(
  "-m", "pytest", $Tests,
  "--test-pipeline-options=$opts",
  "-v", "-s", "--log-cli-level=INFO",
  "-p", "no:cacheprovider"
)
if ($Numprocesses -ne "1") { $pytestArgs += "--numprocesses=$Numprocesses" }
if ($KExpr -ne "") { $pytestArgs += @("-k", $KExpr) }
if ($Timeout -ne "") { $pytestArgs += "--timeout=$Timeout" }

"START $(Get-Date -Format o)  region=$Region  tests=$Tests" | Out-File -FilePath $LogFile -Encoding utf8
& $py @pytestArgs 2>&1 | Out-File -FilePath $LogFile -Append -Encoding utf8
"EXITCODE=$LASTEXITCODE END $(Get-Date -Format o)" | Out-File -FilePath $LogFile -Append -Encoding utf8
