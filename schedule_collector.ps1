<#
Registers ONE Windows scheduled task that keeps collector.py running.

collector.py loops forever on its own (one poll per minute). The task fires at logon and
every 15 minutes; MultipleInstances=IgnoreNew means a new instance only starts if the
previous one has exited or crashed, so this doubles as a watchdog. collector.py also has
its own heartbeat guard against duplicate instances.

Runs as the current user while logged on (no stored password, no elevation). Read-only:
public Kalshi endpoints only. Data lands in data\lip.db; the collector's own log is
data\collector.log; anything printed before logging starts goes to data\collector_stderr.log.
While the machine sleeps nothing is collected; missed periods are simply gaps in the data.

Safe to re-run (replaces the task). Remove with:
  Unregister-ScheduledTask -TaskName KalshiLIP-Collector -Confirm:$false
#>

$ErrorActionPreference = "Stop"
$project = $PSScriptRoot
$python  = Join-Path $project ".venv\Scripts\python.exe"
$errlog  = Join-Path $project "data\collector_stderr.log"

if (-not (Test-Path $python)) { throw "venv python not found at $python" }
New-Item -ItemType Directory -Force -Path (Join-Path $project "data") | Out-Null

# Out-File -Encoding utf8: `*>>` in Windows PowerShell 5.1 writes UTF-16.
$inner  = "& '$python' collector.py 2>&1 | Out-File -FilePath '$errlog' -Append -Encoding utf8"
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -WindowStyle Hidden -Command `"$inner`"" -WorkingDirectory $project

$repeat  = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) -RepetitionDuration (New-TimeSpan -Days 3650)
$atLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

Register-ScheduledTask -TaskName "KalshiLIP-Collector" -Force -Action $action `
    -Trigger @($repeat, $atLogon) -Settings $settings `
    -Description "Kalshi LIP Phase 1 collector (read-only public data)." | Out-Null

Get-ScheduledTask -TaskName "KalshiLIP-Collector" | ForEach-Object {
    $i = $_ | Get-ScheduledTaskInfo
    "{0}  state={1}  next run={2}" -f $_.TaskName, $_.State, $i.NextRunTime }
