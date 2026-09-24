<#
Registers ONE Windows scheduled task that keeps collector.py running.

collector.py loops forever on its own (one poll per minute). The task fires at logon and
every 15 minutes; MultipleInstances=IgnoreNew means a new instance only starts if the
previous one has exited or crashed, so this doubles as a watchdog. collector.py also has
its own heartbeat guard against duplicate instances, and restores its state from the
database on start, so a restart loses at most the poll in flight.

Launches pythonw.exe DIRECTLY (no PowerShell wrapper, no console window). An earlier version
wrapped python in `powershell -WindowStyle Hidden ... 2>&1 | Out-File` and the process was
killed with exit code 0xC000013A (Ctrl+C / console close) about 5 minutes in, so the wrapper
was removed. With no console, all logging goes to data\collector.log, including any crash.

ExecutionTimeLimit is an explicit 7 days rather than "unlimited" (PT0S); the watchdog trigger
restarts it after that. Runs as the current user while logged on (no stored password, no
elevation). Read-only: public Kalshi endpoints only. While the machine sleeps nothing is
collected; missed periods are gaps in the data.

Safe to re-run (replaces the task). Remove with:
  Unregister-ScheduledTask -TaskName KalshiLIP-Collector -Confirm:$false
#>

$ErrorActionPreference = "Stop"
$project = $PSScriptRoot
$pythonw = Join-Path $project ".venv\Scripts\pythonw.exe"

if (-not (Test-Path $pythonw)) { throw "venv pythonw not found at $pythonw" }
New-Item -ItemType Directory -Force -Path (Join-Path $project "data") | Out-Null

$action = New-ScheduledTaskAction -Execute $pythonw -Argument "collector.py" -WorkingDirectory $project

$repeat  = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) -RepetitionDuration (New-TimeSpan -Days 3650)
$atLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Days 7)

Register-ScheduledTask -TaskName "KalshiLIP-Collector" -Force -Action $action `
    -Trigger @($repeat, $atLogon) -Settings $settings `
    -Description "Kalshi LIP Phase 1 collector (read-only public data)." | Out-Null

Get-ScheduledTask -TaskName "KalshiLIP-Collector" | ForEach-Object {
    $i = $_ | Get-ScheduledTaskInfo
    "{0}  state={1}  next run={2}" -f $_.TaskName, $_.State, $i.NextRunTime }
