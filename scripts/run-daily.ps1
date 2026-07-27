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

# Pre-run attempt: consume the previous snapshot only, so a catch-up run can
# still receive the decision that was knowable before its first session.
"[$stamp] pre-daily fundlab agent decide starting" |
    Tee-Object -FilePath $logFile -Append
& uv run fundlab agent decide --all *>> $logFile
$agentCode = $LASTEXITCODE
if ($agentCode -ne 0) {
    # A failed decision writes no file, which the contract treats as a hold —
    # a first-class outcome, so log a marker but do not raise the loud alarm.
    "[$stamp] agent decide failed (exit $agentCode); agent accounts hold today" |
        Tee-Object -FilePath $logFile -Append
    Set-Content -Path (Join-Path $logDir "LAST-AGENT-HOLD") -Value $stamp
} else {
    Remove-Item -Force (Join-Path $logDir "LAST-AGENT-HOLD") -ErrorAction SilentlyContinue
}

"[$stamp] fundlab daily run starting" | Tee-Object -FilePath $logFile -Append
& uv run fundlab daily run *>> $logFile
$code = $LASTEXITCODE

if ($code -eq 0) {
    "[$stamp] daily run ok" | Tee-Object -FilePath $logFile -Append
    Remove-Item -Force (Join-Path $logDir "LAST-RUN-BLOCKED") -ErrorAction SilentlyContinue

    # The successful daily run may have advanced both the account and the
    # published calendar. Prepare the following session now; idempotence makes
    # this a cheap no-op when the pre-run attempt already filled that slot.
    "[$stamp] post-daily fundlab agent decide starting" |
        Tee-Object -FilePath $logFile -Append
    & uv run fundlab agent decide --all *>> $logFile
    $postAgentCode = $LASTEXITCODE
    if ($postAgentCode -ne 0) {
        "[$stamp] post-daily agent decide failed (exit $postAgentCode); retry next run" |
            Tee-Object -FilePath $logFile -Append
        Set-Content -Path (Join-Path $logDir "LAST-AGENT-HOLD") -Value $stamp
    } else {
        Remove-Item -Force (Join-Path $logDir "LAST-AGENT-HOLD") -ErrorAction SilentlyContinue
    }
} else {
    "[$stamp] daily run blocked or failed (exit $code); see data/reports/daily/" |
        Tee-Object -FilePath $logFile -Append
    Set-Content -Path (Join-Path $logDir "LAST-RUN-BLOCKED") -Value $stamp
}
exit $code
