# Registers the "FundLab Daily" Windows scheduled task: Tuesday-Saturday at
# 06:00, processing the previous completed trading session, with catch-up if
# the machine was off. The wrapper owns short, classified retries; Task
# Scheduler's retries are a longer-interval fallback for process-level failure.
#
# Compatible with Windows PowerShell 5.1 and pwsh 7+.
# Run from an elevated PowerShell if task registration is denied:
#   pwsh -File scripts\register-daily-task.ps1
# Remove with: Unregister-ScheduledTask -TaskName "FundLab Daily" -Confirm:$false

param(
    [string]$Time = "06:00"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$pwshCommand = Get-Command pwsh -ErrorAction SilentlyContinue
if ($pwshCommand) {
    $shellExe = $pwshCommand.Source
} else {
    $shellExe = (Get-Command powershell).Source
}

$action = New-ScheduledTaskAction -Execute $shellExe `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$repo\scripts\run-daily.ps1`"" `
    -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Weekly `
    -DaysOfWeek Tuesday, Wednesday, Thursday, Friday, Saturday -At $Time
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 30) `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName "FundLab Daily" `
    -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Registered 'FundLab Daily': Tuesday-Saturday at $Time, catch-up + scheduler fallback retries."
Write-Host "Research prices, state, and factors use TickFlow/BaoStock; execution remains guarded until two direct-limit sources are available."
