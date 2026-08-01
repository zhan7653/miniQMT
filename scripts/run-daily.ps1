# Runs one fundlab daily cycle and appends the outcome to logs/daily/.
# Safe to re-run: the pipeline is idempotent and resumes from the observation
# warehouse. Exit code 0 = ok/up-to-date/degraded, 2 = a fail-closed gate blocked the run.
#
# Operational requirement: the local MiniQMT client must be running for the
# xtquant provider; if it is offline the data stage blocks and the next run
# resumes where it stopped.

param([switch]$FunctionsOnly)

function Get-DailyAttemptReport {
    param(
        [Parameter(Mandatory = $true)][string]$DailyReportDir,
        [Parameter(Mandatory = $true)][datetime]$AttemptStarted
    )

    $latestReport = Get-ChildItem -Path $DailyReportDir -Filter "daily-*.json" `
            -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -ge $AttemptStarted.AddSeconds(-2) } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $latestReport) {
        return $null
    }
    try {
        $report = Get-Content -Raw -LiteralPath $latestReport.FullName |
            ConvertFrom-Json
    } catch {
        return $null
    }
    return [pscustomobject]@{
        Path = $latestReport.FullName
        Report = $report
    }
}

function Test-DailyFailureRetryable {
    param(
        [Parameter(Mandatory = $true)][string]$DailyReportDir,
        [Parameter(Mandatory = $true)][datetime]$AttemptStarted
    )

    $attemptReport = Get-DailyAttemptReport `
        -DailyReportDir $DailyReportDir `
        -AttemptStarted $AttemptStarted
    if ($null -eq $attemptReport) {
        return $false
    }
    return @(
        $attemptReport.Report.stages |
            Where-Object {
                $_.status -eq "blocked" -and $_.detail.retryable -eq $true
            }
    ).Count -gt 0
}

if ($FunctionsOnly) {
    return
}

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

# Task Scheduler can start with an environment block captured before a user
# variable was created. Refresh the LLM credential from the current user's
# environment store without ever persisting it in the repository or log.
if ([string]::IsNullOrWhiteSpace($env:FUNDLAB_LLM_API_KEY)) {
    $userLlmApiKey = [Environment]::GetEnvironmentVariable(
        "FUNDLAB_LLM_API_KEY", "User"
    )
    if (-not [string]::IsNullOrWhiteSpace($userLlmApiKey)) {
        $env:FUNDLAB_LLM_API_KEY = $userLlmApiKey
    }
}

$logDir = Join-Path $repo "logs\daily"
$dailyReportDir = Join-Path $repo "data\reports\daily"
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

$maxDailyAttempts = 3
$code = 1
for ($attempt = 1; $attempt -le $maxDailyAttempts; $attempt++) {
    "[$stamp] fundlab daily run starting (attempt $attempt/$maxDailyAttempts)" |
        Tee-Object -FilePath $logFile -Append
    $attemptStarted = Get-Date
    & uv run fundlab daily run *>> $logFile
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        break
    }
    $retryable = $code -eq 2 -and (
        Test-DailyFailureRetryable `
            -DailyReportDir $dailyReportDir `
            -AttemptStarted $attemptStarted
    )
    if (-not $retryable -or $attempt -eq $maxDailyAttempts) {
        break
    }
    $delaySeconds = 30 * [math]::Pow(2, $attempt - 1)
    "[$stamp] transient provider failure; retrying in $delaySeconds seconds" |
        Tee-Object -FilePath $logFile -Append
    Start-Sleep -Seconds $delaySeconds
}

if ($code -eq 0) {
    Remove-Item -Force (Join-Path $logDir "LAST-RUN-BLOCKED") -ErrorAction SilentlyContinue
    $attemptReport = Get-DailyAttemptReport `
        -DailyReportDir $dailyReportDir `
        -AttemptStarted $attemptStarted
    if ($null -ne $attemptReport -and $attemptReport.Report.status -eq "degraded") {
        $quarantine = $attemptReport.Report.stages |
            Where-Object { $_.name -eq "quarantine" } |
            Select-Object -Last 1
        $marker = [ordered]@{
            status = "degraded"
            detected_at = (Get-Date).ToString("o")
            report = $attemptReport.Path
            target_date = $attemptReport.Report.target_date
            snapshot_id = $attemptReport.Report.snapshot_id
            quarantine = $quarantine.detail
        } | ConvertTo-Json -Depth 12
        Set-Content -LiteralPath (Join-Path $logDir "LAST-RUN-DEGRADED") `
            -Value $marker
        $count = $quarantine.detail.instrument_count
        "[$stamp] [DEGRADED] daily run completed with $count quarantined instruments; see $($attemptReport.Path)" |
            Tee-Object -FilePath $logFile -Append
    } else {
        Remove-Item -Force (Join-Path $logDir "LAST-RUN-DEGRADED") -ErrorAction SilentlyContinue
        "[$stamp] daily run ok" | Tee-Object -FilePath $logFile -Append
    }

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
    Remove-Item -Force (Join-Path $logDir "LAST-RUN-DEGRADED") -ErrorAction SilentlyContinue
}
exit $code
