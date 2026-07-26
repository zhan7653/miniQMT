# Registers the "FundLab Daily" Windows scheduled task: weekdays at 20:00,
# catch-up if the machine was off, and up to 3 retries 30 minutes apart
# (the pipeline is idempotent, so retries are safe).
#
# Run from an elevated PowerShell if task registration is denied:
#   pwsh -File scripts\register-daily-task.ps1
# Remove with: Unregister-ScheduledTask -TaskName "FundLab Daily" -Confirm:$false

param(
    [string]$Time = "20:00",
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$pwshExe = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source
if (-not $pwshExe) { $pwshExe = (Get-Command powershell).Source }

$action = New-ScheduledTaskAction -Execute $pwshExe `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$repo\scripts\run-daily.ps1`"" `
    -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Weekly `
    -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At $Time
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 30) `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName "FundLab Daily" `
    -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Registered 'FundLab Daily': weekdays at $Time, catch-up + 3 retries."
Write-Host "Requirement: keep the MiniQMT client running so xtquant can serve data."
