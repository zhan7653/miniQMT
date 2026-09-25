# Runs one fundlab daily cycle and appends the outcome to logs/daily/.
# Safe to re-run: the pipeline is idempotent and resumes from the observation
# warehouse. Exit code 0 = ok/up-to-date/degraded, 2 = a fail-closed gate blocked the run.
#
# The configured research-price path uses TickFlow plus BaoStock.  If direct
# execution evidence is incomplete, the affected scope is marked no-execution
# and the next run resumes where it stopped.

param([switch]$FunctionsOnly)

function Get-DailyReportSnapshot {
    param([Parameter(Mandatory = $true)][string]$DailyReportDir)

    return @(
        Get-ChildItem -Path $DailyReportDir -Filter "daily-*.json" `
            -File -ErrorAction SilentlyContinue |
            ForEach-Object { $_.FullName }
    )
}

function Get-DailyPendingReportNameSnapshot {
    param([Parameter(Mandatory = $true)][string]$DailyReportDir)

    $pendingRoot = Join-Path $DailyReportDir ".pending"
    return @(
        Get-ChildItem -Path $pendingRoot -Filter "daily-*.json" `
            -File -ErrorAction SilentlyContinue |
            ForEach-Object { $_.Name }
    )
}

function Get-DailyAttemptReport {
    param(
        [Parameter(Mandatory = $true)][string]$DailyReportDir,
        [AllowEmptyCollection()][string[]]$ExistingReportPaths = @(),
        [AllowEmptyCollection()][string[]]$PendingReportNames = @()
    )

    # A formal report is the durable commit marker for this invocation.  Do
    # not infer ownership from timestamps: an old report can easily share the
    # scheduler's clock window.  Only a file that appeared after our snapshot
    # may be used for retries or a successful outcome.
    $knownPaths = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($path in $ExistingReportPaths) {
        [void]$knownPaths.Add($path)
    }
    $pendingNames = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($name in $PendingReportNames) {
        [void]$pendingNames.Add($name)
    }
    $newReports = Get-ChildItem -Path $DailyReportDir -Filter "daily-*.json" `
            -File -ErrorAction SilentlyContinue |
        Where-Object {
            -not $knownPaths.Contains($_.FullName) -and
            -not $pendingNames.Contains($_.Name)
        } |
        Sort-Object LastWriteTime -Descending
    foreach ($newReport in $newReports) {
        try {
            $report = Get-Content -Raw -LiteralPath $newReport.FullName |
                ConvertFrom-Json
        } catch {
            continue
        }
        if ($report -isnot [pscustomobject]) {
            continue
        }
        return [pscustomobject]@{
            Path = $newReport.FullName
            Report = $report
        }
    }
    return $null
}

function Test-DailyFailureRetryable {
    param(
        [Parameter(Mandatory = $true)][string]$DailyReportDir,
        [AllowEmptyCollection()][string[]]$ExistingReportPaths = @(),
        [AllowEmptyCollection()][string[]]$PendingReportNames = @(),
        # Kept only for callers of the exported FunctionsOnly helper.  The
        # executable wrapper always supplies a pre-invocation file snapshot.
        [datetime]$AttemptStarted
    )

    $attemptReport = Get-DailyAttemptReport `
        -DailyReportDir $DailyReportDir `
        -ExistingReportPaths $ExistingReportPaths `
        -PendingReportNames $PendingReportNames
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
    $reportSnapshot = Get-DailyReportSnapshot -DailyReportDir $dailyReportDir
    $pendingReportNames = Get-DailyPendingReportNameSnapshot `
        -DailyReportDir $dailyReportDir
    & uv run fundlab daily run *>> $logFile
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        $attemptReport = Get-DailyAttemptReport `
            -DailyReportDir $dailyReportDir `
            -ExistingReportPaths $reportSnapshot `
            -PendingReportNames $pendingReportNames
        if ($null -eq $attemptReport) {
            # Exit 0 without a new formal report is not an auditable daily
            # outcome.  Treat it as a failed run rather than publishing a
            # misleading success based on an older report.
            $code = 2
            "[$stamp] daily run audit failed: no new formal report" |
                Tee-Object -FilePath $logFile -Append
        }
        break
    }
    $retryable = $code -eq 2 -and (
        Test-DailyFailureRetryable `
            -DailyReportDir $dailyReportDir `
            -ExistingReportPaths $reportSnapshot `
            -PendingReportNames $pendingReportNames
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
    if ($null -ne $attemptReport -and $attemptReport.Report.status -eq "degraded") {
        $degradedStages = @(
            $attemptReport.Report.stages |
                Where-Object { $_.status -eq "degraded" }
        )
        $affectedAccounts = @(
            $attemptReport.Report.accounts |
                Where-Object { $_.status -in @("degraded", "blocked") }
        )
        $quarantine = $attemptReport.Report.stages |
            Where-Object { $_.name -eq "quarantine" } |
            Select-Object -Last 1

        $summaryParts = @()
        if ($null -ne $quarantine) {
            $count = $quarantine.detail.instrument_count
            if (
                $null -ne $count -and
                -not [string]::IsNullOrWhiteSpace([string]$count)
            ) {
                $summaryParts += "$count quarantined instruments"
            } else {
                $summaryParts += "quarantine reported"
            }
        }
        if ($degradedStages.Count -gt 0) {
            $stageNames = @(
                $degradedStages | ForEach-Object {
                    if ([string]::IsNullOrWhiteSpace([string]$_.name)) {
                        "<unnamed>"
                    } else {
                        [string]$_.name
                    }
                }
            ) -join ", "
            $summaryParts += "degraded stages: $stageNames"
        }
        if ($affectedAccounts.Count -gt 0) {
            $accountSummaries = @(
                $affectedAccounts | ForEach-Object {
                    $accountId = [string]$_.account_id
                    if ([string]::IsNullOrWhiteSpace($accountId)) {
                        $accountId = "<unknown>"
                    }
                    "$accountId ($($_.status))"
                }
            ) -join ", "
            $summaryParts += "affected accounts: $accountSummaries"
        }
        if ($summaryParts.Count -eq 0) {
            $summaryParts += "degraded status reported without stage or account detail"
        }
        $summary = $summaryParts -join "; "

        $markerData = [ordered]@{
            status = "degraded"
            detected_at = (Get-Date).ToString("o")
            report = $attemptReport.Path
            target_date = $attemptReport.Report.target_date
            snapshot_id = $attemptReport.Report.snapshot_id
            summary = $summary
            degraded_stages = $degradedStages
            affected_accounts = $affectedAccounts
        }
        if ($null -ne $quarantine) {
            $markerData.quarantine = $quarantine.detail
        }
        $marker = $markerData | ConvertTo-Json -Depth 12
        Set-Content -LiteralPath (Join-Path $logDir "LAST-RUN-DEGRADED") `
            -Value $marker
        "[$stamp] [DEGRADED] daily run completed with $summary; see $($attemptReport.Path)" |
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
