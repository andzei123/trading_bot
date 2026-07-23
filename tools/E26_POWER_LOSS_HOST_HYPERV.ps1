[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$VMName,
  [Parameter(Mandatory=$true)][System.Management.Automation.PSCredential]$Credential,
  [Parameter(Mandatory=$true)][switch]$ConfirmHardPowerOff,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$GuestPython,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$RepositoryPath,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$LedgerPath,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$PhaseFile,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$StateFile,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$GuestWorkDirectory,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$BaselineCommit,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$PatchPath,
  [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-fA-F]{64}$')][string]$PatchSHA256,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$SnapshotName,
  [Parameter(Mandatory=$true)][ValidateRange(1,1000)][int]$Iterations,
  [Parameter(Mandatory=$true)][ValidateRange(5,3600)][int]$PhaseTimeoutSeconds,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$HostEvidenceDirectory,
  [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$WriteCacheDetailsJson
)

$ErrorActionPreference='Stop'
$ExpectedBaseline='a9132f7db60e7932e1d922ae4a66f673d22f2646'
if(-not $ConfirmHardPowerOff){ throw 'Explicit -ConfirmHardPowerOff is required.' }
if($BaselineCommit.ToLowerInvariant() -ne $ExpectedBaseline){ throw "BaselineCommit must equal $ExpectedBaseline" }
if([string]::IsNullOrWhiteSpace($VMName) -or $VMName -match '[\*\?\[]'){ throw 'VMName must be one exact non-wildcard VM name.' }
if(-not (Test-Path -LiteralPath $PatchPath -PathType Leaf)){ throw 'PatchPath must identify one existing regular file.' }
$resolvedPatch=(Resolve-Path -LiteralPath $PatchPath).Path
$calculatedPatchHash=(Get-FileHash -LiteralPath $resolvedPatch -Algorithm SHA256).Hash.ToLowerInvariant()
if($calculatedPatchHash -ne $PatchSHA256.ToLowerInvariant()){ throw 'Patch SHA256 mismatch.' }
try { $writeCacheDetails=$WriteCacheDetailsJson | ConvertFrom-Json } catch { throw 'WriteCacheDetailsJson must be valid JSON.' }
if($null -eq $writeCacheDetails){ throw 'WriteCacheDetailsJson must contain factual evidence.' }

$vms=@(Get-VM -Name $VMName -ErrorAction Stop)
if($vms.Count -ne 1 -or $vms[0].Name -cne $VMName){ throw 'Exact VM identity validation failed.' }
$snapshots=@(Get-VMSnapshot -VMName $VMName -Name $SnapshotName -ErrorAction SilentlyContinue)
if($snapshots.Count -ne 1){ throw 'Exactly one required snapshot must exist.' }
$vm=$vms[0]
$snapshot=$snapshots[0]
$null=New-Item -ItemType Directory -Force -Path $HostEvidenceDirectory
$aggregatePath=Join-Path $HostEvidenceDirectory 'E26_4_POWER_LOSS_RESULTS_HOST.jsonl'
if(Test-Path -LiteralPath $aggregatePath){ throw 'Host aggregate already exists; use a new evidence directory.' }

$patchInfo=Get-Item -LiteralPath $resolvedPatch
$patchEvidence=[ordered]@{
  resolved_patch_path=$resolvedPatch
  declared_sha256=$PatchSHA256.ToLowerInvariant()
  calculated_sha256=$calculatedPatchHash
  size_bytes=[int64]$patchInfo.Length
  last_write_time_utc=$patchInfo.LastWriteTimeUtc.ToString('o')
  baseline_commit=$BaselineCommit
}
$patchEvidence | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 (Join-Path $HostEvidenceDirectory 'patch_identity.json')
Copy-Item -LiteralPath $resolvedPatch -Destination (Join-Path $HostEvidenceDirectory 'validated_E26_4.patch')

$hostWindows=Get-CimInstance Win32_OperatingSystem | Select-Object Caption,Version,BuildNumber
$controllers=@(Get-VMHardDiskDrive -VMName $VMName | Select-Object ControllerType,ControllerNumber,ControllerLocation,Path)
$diskDetails=@()
foreach($disk in Get-VMHardDiskDrive -VMName $VMName){
  if($disk.Path){
    $vhd=Get-VHD -Path $disk.Path
    $diskDetails += [pscustomobject]@{Path=$disk.Path;VhdType=[string]$vhd.VhdType;VhdFormat=[string]$vhd.VhdFormat;Size=$vhd.Size;FileSize=$vhd.FileSize}
  }
}
if($diskDetails.Count -eq 0 -or $controllers.Count -eq 0){ throw 'VM storage evidence is incomplete.' }

function Wait-GuestReady {
  param([datetime]$Deadline)
  while((Get-Date) -lt $Deadline){
    try {
      $ready=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock { 'READY' } -ErrorAction Stop
      if($ready -eq 'READY'){ return }
    } catch {}
    Start-Sleep -Milliseconds 500
  }
  throw 'Timed out waiting for PowerShell Direct guest readiness.'
}

function Write-GuestJson {
  param([object]$Payload,[string]$Path)
  Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
    param($Value,$Destination)
    $parent=[System.IO.Path]::GetDirectoryName($Destination)
    if($parent){ [System.IO.Directory]::CreateDirectory($parent) | Out-Null }
    $json=$Value | ConvertTo-Json -Depth 16 -Compress
    [System.IO.File]::WriteAllText($Destination,$json,[System.Text.UTF8Encoding]::new($false))
  } -ArgumentList $Payload,$Path
}

function Invoke-GuestVerifierStructured {
  param([string]$InputPath,[string]$StdoutPath,[string]$StderrPath)
  $result=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
    param($Python,$Repo,$InputFile,$OutPath,$ErrPath)
    Set-Location -LiteralPath $Repo
    & $Python '-m' 'tools.e26_power_loss_verify' '--input' $InputFile 1> $OutPath 2> $ErrPath
    [pscustomobject]@{ExitCode=[int]$LASTEXITCODE;StdoutPath=$OutPath;StderrPath=$ErrPath}
  } -ArgumentList $GuestPython,$RepositoryPath,$InputPath,$StdoutPath,$StderrPath
  if($null -eq $result -or $result.ExitCode -isnot [int]){ throw 'Structured verifier exit-code capture failed.' }
  return $result
}

function Add-HostAggregateRecord {
  param([object]$Record)
  $line=$Record | ConvertTo-Json -Depth 20 -Compress
  $stream=[System.IO.File]::Open($aggregatePath,[System.IO.FileMode]::Append,[System.IO.FileAccess]::Write,[System.IO.FileShare]::Read)
  try {
    $writer=[System.IO.StreamWriter]::new($stream,[System.Text.UTF8Encoding]::new($false))
    try { $writer.WriteLine($line); $writer.Flush(); $stream.Flush($true) } finally { $writer.Dispose() }
  } finally { $stream.Dispose() }
}

$phases=@('BEFORE_TEMP_CREATE','TEMP_WRITING','TEMP_FLUSHED','BEFORE_REPLACE','REPLACE_RETURNED','COMMITTED_FILE_FLUSHED','COMMIT_ACKNOWLEDGED')
foreach($phase in $phases){
  for($i=1;$i -le $Iterations;$i++){
    $runId=[guid]::NewGuid().ToString()
    $runLabel="${phase}_$i`_$runId"
    $runDir=Join-Path $HostEvidenceDirectory $runLabel
    $null=New-Item -ItemType Directory -Force -Path $runDir
    try {
      Restore-VMSnapshot -VMName $VMName -Name $SnapshotName -Confirm:$false
      Start-VM -Name $VMName | Out-Null
      Wait-GuestReady -Deadline (Get-Date).AddSeconds($PhaseTimeoutSeconds)

      $prep=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
        param($PhasePath,$StatePath,$Repo)
        Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*e26_power_loss_writer.py*' -or $_.CommandLine -like '*tools.e26_power_loss_writer*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
        Remove-Item -LiteralPath $PhasePath,$StatePath -Force -ErrorAction SilentlyContinue
        Set-Location -LiteralPath $Repo
        [pscustomobject]@{Head=(git rev-parse HEAD).Trim();StaleWriters=@(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*tools.e26_power_loss_writer*' }).Count}
      } -ArgumentList $PhaseFile,$StateFile,$RepositoryPath
      if($prep.StaleWriters -ne 0){ throw 'Stale writer process remains.' }
      if($prep.Head -ne $BaselineCommit){ throw "Guest repository HEAD mismatch: $($prep.Head)" }

      $writerInputPath=Join-Path $GuestWorkDirectory "$runLabel.writer_input.json"
      $writerInput=[ordered]@{schema_version=1;ledger_path=$LedgerPath;phase_file=$PhaseFile;state_file=$StateFile;run_id=$runId;delay=0.25}
      Write-GuestJson -Payload $writerInput -Path $writerInputPath
      $writer=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
        param($Python,$Repo,$InputPath)
        Set-Location -LiteralPath $Repo
        $quotedInput='"'+$InputPath.Replace('"','\"')+'"'
        $p=Start-Process -FilePath $Python -ArgumentList "-m tools.e26_power_loss_writer --input $quotedInput" -PassThru -NoNewWindow
        [pscustomobject]@{Pid=[int]$p.Id}
      } -ArgumentList $GuestPython,$RepositoryPath,$writerInputPath
      $writerPid=[int]$writer.Pid

      $deadline=(Get-Date).AddSeconds($PhaseTimeoutSeconds)
      $observation=$null
      while((Get-Date) -lt $deadline){
        Start-Sleep -Milliseconds 200
        $health=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
          param($PhasePath,$Pid)
          $proc=Get-Process -Id $Pid -ErrorAction SilentlyContinue
          $phaseData=$null
          if(Test-Path -LiteralPath $PhasePath){ try{$phaseData=Get-Content -LiteralPath $PhasePath -Raw | ConvertFrom-Json}catch{} }
          [pscustomobject]@{Alive=($null -ne $proc);Phase=$phaseData}
        } -ArgumentList $PhaseFile,$writerPid
        if(-not $health.Alive){ throw "Writer PID $writerPid exited before target phase $phase" }
        if($health.Phase -and $health.Phase.phase -eq $phase -and $health.Phase.run_id -eq $runId -and [int]$health.Phase.writer_pid -eq $writerPid){ $observation=$health.Phase; break }
      }
      if(-not $observation){ throw "Timed out waiting for current iteration phase $phase" }

      $hostObservation=[ordered]@{run_id=$runId;writer_pid=$writerPid;target_phase=$phase;iteration=$i;previous_acknowledged_commit_id=[int]$observation.previous_acknowledged_commit_id;in_flight_commit_id=[int]$observation.in_flight_commit_id;vm_identity=$VMName;snapshot_identity=$SnapshotName;baseline_commit=$BaselineCommit;patch_sha256=$calculatedPatchHash;host_capture_timestamp_utc=[DateTime]::UtcNow.ToString('o')}
      $hostObsPath=Join-Path $runDir 'host_observation.json'
      $hostObservation | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $hostObsPath

      Stop-VM -Name $VMName -TurnOff -Confirm:$false
      $physicalPowerOffExecuted=$true
      Start-VM -Name $VMName | Out-Null
      Wait-GuestReady -Deadline (Get-Date).AddSeconds($PhaseTimeoutSeconds)

      $guestInputPath=Join-Path $GuestWorkDirectory "$runLabel.verifier_input.json"
      $guestResultPath=Join-Path $GuestWorkDirectory "$runLabel.verifier_result.json"
      $guestOut=Join-Path $GuestWorkDirectory "$runLabel.verifier_stdout.txt"
      $guestErr=Join-Path $GuestWorkDirectory "$runLabel.verifier_stderr.txt"
      $verifierInput=[ordered]@{
        schema_version=1;run_id=$runId;writer_pid=$writerPid;target_phase=$phase;iteration=$i
        ledger_path=$LedgerPath;result_path=$guestResultPath;baseline_commit=$BaselineCommit;patch_sha256=$calculatedPatchHash
        previous_acknowledged_commit_id=[int]$observation.previous_acknowledged_commit_id;in_flight_commit_id=[int]$observation.in_flight_commit_id
        vm_identity=$VMName;snapshot_identity=$SnapshotName;host_environment=$hostWindows;virtual_disk=$diskDetails
        storage_controller=$controllers;write_cache_details=$writeCacheDetails;physical_hard_power_off_executed=$physicalPowerOffExecuted
      }
      Write-GuestJson -Payload $verifierInput -Path $guestInputPath
      $verifyResult=Invoke-GuestVerifierStructured -InputPath $guestInputPath -StdoutPath $guestOut -StderrPath $guestErr

      $guestEvidence=Invoke-Command -VMName $VMName -Credential $Credential -ScriptBlock {
        param($Result,$Out,$Err)
        if(-not (Test-Path -LiteralPath $Result -PathType Leaf)){ throw 'Verifier result is missing.' }
        [pscustomobject]@{Result=(Get-Content -LiteralPath $Result -Raw);Stdout=(Get-Content -LiteralPath $Out -Raw -ErrorAction SilentlyContinue);Stderr=(Get-Content -LiteralPath $Err -Raw -ErrorAction SilentlyContinue)}
      } -ArgumentList $guestResultPath,$guestOut,$guestErr
      $guestRecord=$guestEvidence.Result | ConvertFrom-Json

      $joinChecks=[ordered]@{
        run_id=($guestRecord.run_id -eq $runId)
        writer_pid=([int]$guestRecord.writer_pid -eq $writerPid)
        target_phase=($guestRecord.target_phase -eq $phase)
        iteration=([int]$guestRecord.iteration -eq $i)
        baseline_commit=($guestRecord.baseline_commit -eq $BaselineCommit)
        patch_sha256=($guestRecord.patch_sha256 -eq $calculatedPatchHash)
        vm_identity=($guestRecord.vm_identity -eq $VMName)
        snapshot_identity=($guestRecord.snapshot_identity -eq $SnapshotName)
      }
      $joinPassed=-not ($joinChecks.Values -contains $false)
      if(-not $joinPassed){ throw 'Host/guest evidence join failed.' }

      $guestEvidence.Result | Out-File -Encoding utf8 (Join-Path $runDir 'verifier_result.json')
      $guestEvidence.Stdout | Out-File -Encoding utf8 (Join-Path $runDir 'verifier_stdout.txt')
      $guestEvidence.Stderr | Out-File -Encoding utf8 (Join-Path $runDir 'verifier_stderr.txt')
      $processEvidence=[ordered]@{exit_code=[int]$verifyResult.ExitCode;stdout_guest_path=$guestOut;stderr_guest_path=$guestErr}
      $processEvidence | ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $runDir 'verifier_process.json')
      $environment=[ordered]@{host_windows=$hostWindows;guest_environment=$guestRecord.guest_environment;vm_generation=[string]$vm.Generation;virtual_disk=$diskDetails;storage_controller=$controllers;write_cache_details=$writeCacheDetails}
      $environment | ConvertTo-Json -Depth 12 | Out-File -Encoding utf8 (Join-Path $runDir 'environment.json')
      $artifactHashes=[ordered]@{patch_sha256=$calculatedPatchHash;ledger_sha256=$guestRecord.main_sha256;temp_sha256=$guestRecord.temp_sha256}
      $artifactHashes | ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $runDir 'artifact_hashes.json')

      $artifacts=[ordered]@{}
      foreach($name in @('host_observation.json','verifier_result.json','verifier_stdout.txt','verifier_stderr.txt','verifier_process.json','environment.json','artifact_hashes.json')){ $artifacts[$name]=Test-Path -LiteralPath (Join-Path $runDir $name) -PathType Leaf }
      if($artifacts.Values -contains $false){ throw 'Mandatory host artifact is missing.' }
      $joined=[ordered]@{
        schema_version=1;run_id=$runId;writer_pid=$writerPid;target_phase=$phase;iteration=$i;vm_identity=$VMName;snapshot_identity=$SnapshotName
        baseline_commit=$BaselineCommit;patch_sha256=$calculatedPatchHash;post_boot_classification=$guestRecord.post_boot_classification
        forbidden_outcomes=@($guestRecord.forbidden_outcomes);verification_errors=@($guestRecord.verification_errors);verifier_exit_code=[int]$verifyResult.ExitCode
        physical_hard_power_off_executed=$physicalPowerOffExecuted;environment_complete=($null -ne $guestRecord.guest_environment.filesystem -and $null -ne $guestRecord.guest_environment.volume_identity)
        join_validation=[ordered]@{passed=$joinPassed;checks=$joinChecks};host_artifacts=$artifacts
      }
      Add-HostAggregateRecord -Record $joined
      $lastLine=Get-Content -LiteralPath $aggregatePath -Tail 1 | ConvertFrom-Json
      if($lastLine.run_id -ne $runId){ throw 'Host aggregate append verification failed.' }
      if([int]$verifyResult.ExitCode -ne 0){ throw "Verifier failed for $runLabel with exit code $($verifyResult.ExitCode)" }
    } catch {
      $_ | Out-String | Out-File -Encoding utf8 (Join-Path $runDir 'iteration_error.txt')
      throw
    }
  }
}
