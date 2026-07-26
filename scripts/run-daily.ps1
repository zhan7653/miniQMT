# Runs one fundlab daily cycle and appends the outcome to logs/daily/.
# Safe to re-run: the pipeline is idempotent and resumes from the observation
# warehouse. Exit code 0 = ok/up-to-date, 2 = a fail-closed gate blocked the run.
#
# Operational requirement: the local MiniQMT client must be running for the
# xtquant provider; if it is offline the data stage blocks and the next run
# resumes where it stopped.

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

$logDir = Join-Path $repo "logs\daily"
New-Item -ItemType Directory -Force $logDir | Out-Null
$stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$logFile = Join-Path $logDir "run-$stamp.log"

"[$stamp] fundlab daily run starting" | Tee-Object -FilePath $logFile -Append
& uv run fundlab daily run *>> $logFile
$code = $LASTEXITCODE

if ($code -eq 0) {
    "[$stamp] daily run ok" | Tee-Object -FilePath $logFile -Append
    Remove-Item -Force (Join-Path $logDir "LAST-RUN-BLOCKED") -ErrorAction SilentlyContinue
} else {
    "[$stamp] daily run blocked or failed (exit $code); see data/reports/daily/" |
        Tee-Object -FilePath $logFile -Append
    Set-Content -Path (Join-Path $logDir "LAST-RUN-BLOCKED") -Value $stamp
}
exit $code
