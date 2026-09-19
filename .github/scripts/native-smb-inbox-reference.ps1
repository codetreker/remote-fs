[CmdletBinding()]
param([Parameter(Mandatory)][ValidateSet('Prepare','Run','Verify')][string]$Phase)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:Workspace = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$script:Probe = Join-Path $script:Workspace '.tmp/native-inbox-reference'
$script:Results = Join-Path $script:Probe 'results'
$script:LedgerPath = Join-Path $script:Results 'ownership.json'
$script:Ledger = $null
$script:ProtocolPrefix = 'RFS_INBOX:'

function Write-InboxJSON([string]$Path, $Value) {
    $text = ConvertTo-Json -InputObject $Value -Depth 32 -Compress
    if ([Text.Encoding]::UTF8.GetByteCount($text) -gt 1048576) { throw 'Controller receipt exceeds its bound.' }
    $temporary = "$Path.new"
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($text + "`n")
    $stream = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try { $stream.Write($bytes); $stream.Flush($true) } finally { $stream.Dispose() }
    [IO.File]::Move($temporary, $Path, $true)
}
function Save-InboxLedger { Write-InboxJSON $script:LedgerPath $script:Ledger }
function Get-InboxSHA256([string]$Path) { return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Get-InboxCanonicalHash([string]$Path) {
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes([IO.File]::ReadAllText($Path).Replace("`r`n","`n")))).ToLowerInvariant()
}
function Get-InboxRequired($Value, [string]$Name) {
    if ($Value -is [Collections.IDictionary]) {
        if (-not $Value.Contains($Name) -or $null -eq $Value[$Name]) { throw "Missing required field: $Name" }
        return $Value[$Name]
    }
    $property = $Value.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) { throw "Missing required provider field: $Name" }
    return $property.Value
}
function Convert-InboxID($Value) {
    if($Value -is [double] -or $Value -is [single] -or $Value -is [decimal]){throw 'Administrative ID lost its integer representation.'}
    $text = [string]$Value
    if ($text -cnotmatch '^[0-9]{1,20}$') { throw 'Administrative ID is not an exact unsigned decimal value.' }
    $id = [UInt64]::Parse($text, [Globalization.CultureInfo]::InvariantCulture)
    return $id.ToString([Globalization.CultureInfo]::InvariantCulture)
}
function Assert-InboxToken($Expected, $Actual) {
    foreach ($key in @('sid','authentication_id','session_id')) {
        if ([string](Get-InboxRequired $Expected $key) -cne [string](Get-InboxRequired $Actual $key)) { throw "Current logon changed: $key" }
    }
}
function Assert-InboxNoReparse([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    while ($null -ne $item) {
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Owned path contains a reparse component.' }
        $item = if ($item -is [IO.DirectoryInfo]) { $item.Parent } elseif ($item -is [IO.FileInfo]) { $item.Directory } else { throw 'Owned path has an unsupported filesystem type.' }
    }
}
function Initialize-InboxNative {
    if ('InboxReference.Native' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
namespace InboxReference {
 public sealed class Token { public string sid; public ulong authentication_id; public uint session_id; public bool elevated; }
 public sealed class DirectoryIdentity { public string path,volume_serial_hex,file_index_hex; public uint attributes; }
 public static class Native {
  [StructLayout(LayoutKind.Sequential)] struct Luid { public uint Low; public int High; }
  [StructLayout(LayoutKind.Sequential)] struct Statistics { public Luid TokenID,AuthenticationID; public long Expiration; public uint TokenType,ImpersonationLevel,DynamicCharged,DynamicAvailable,GroupCount,PrivilegeCount; public Luid ModifiedID; }
  [StructLayout(LayoutKind.Sequential)] struct FileInformation { public uint Attributes; public System.Runtime.InteropServices.ComTypes.FILETIME Creation,Access,Write; public uint Volume,SizeHigh,SizeLow,Links,IndexHigh,IndexLow; }
  [StructLayout(LayoutKind.Sequential)] struct SecurityAttributes { public int Length; public IntPtr Descriptor; [MarshalAs(UnmanagedType.Bool)] public bool Inherit; }
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentThread();
  [DllImport("advapi32.dll",SetLastError=true)] static extern bool OpenProcessToken(IntPtr process,uint access,out IntPtr token);
  [DllImport("advapi32.dll",SetLastError=true)] static extern bool OpenThreadToken(IntPtr thread,uint access,bool self,out IntPtr token);
  [DllImport("advapi32.dll",SetLastError=true)] static extern bool GetTokenInformation(IntPtr token,int kind,IntPtr buffer,int length,out int actual);
  [DllImport("kernel32.dll",SetLastError=true)] static extern bool CloseHandle(IntPtr handle);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern IntPtr CreateFileW(string path,uint access,uint share,IntPtr security,uint creation,uint flags,IntPtr template);
  [DllImport("kernel32.dll",SetLastError=true)] static extern bool GetFileInformationByHandle(IntPtr file,out FileInformation information);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool GetVolumePathNameW(string path,StringBuilder volume,uint length);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool GetVolumeInformationW(string root,StringBuilder label,uint labelLength,out uint serial,out uint component,out uint flags,StringBuilder filesystem,uint filesystemLength);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool CreateDirectoryW(string path,ref SecurityAttributes attributes);
  [DllImport("advapi32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool ConvertStringSecurityDescriptorToSecurityDescriptorW(string text,uint revision,out IntPtr descriptor,out uint size);
  [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr memory);
  [DllImport("kernel32.dll",SetLastError=true)] public static extern uint GetLogicalDrives();
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern uint QueryDosDeviceW(string name,char[] buffer,uint length);
  [DllImport("mpr.dll",CharSet=CharSet.Unicode)] static extern uint WNetGetConnectionW(string local,StringBuilder remote,ref uint length);
  [DllImport("mpr.dll",CharSet=CharSet.Unicode)] static extern uint WNetCancelConnection2W(string name,uint flags,bool force);
  static Exception Error(string operation) { return new Win32Exception(Marshal.GetLastWin32Error(),operation); }
  static Token ReadToken(IntPtr handle) {
   var answer=new Token(); using(var identity=new WindowsIdentity(handle)) { answer.sid=identity.User.Value; }
   if(Marshal.SizeOf<Statistics>()!=56 || Marshal.OffsetOf<Statistics>("AuthenticationID").ToInt32()!=8) throw new InvalidOperationException("TOKEN_STATISTICS ABI differs.");
   IntPtr memory=Marshal.AllocHGlobal(56);
   try {
    int actual;
    if(!GetTokenInformation(handle,10,memory,56,out actual)||actual!=56) throw Error("Read token statistics");
    var stats=Marshal.PtrToStructure<Statistics>(memory); answer.authentication_id=((ulong)(uint)stats.AuthenticationID.High<<32)|stats.AuthenticationID.Low;
    if(!GetTokenInformation(handle,12,memory,4,out actual)||actual!=4) throw Error("Read token session"); answer.session_id=(uint)Marshal.ReadInt32(memory);
    if(!GetTokenInformation(handle,20,memory,4,out actual)||actual!=4) throw Error("Read token elevation"); answer.elevated=Marshal.ReadInt32(memory)==1;
    return answer;
   } finally { Marshal.FreeHGlobal(memory); }
  }
  public static Token CurrentToken() {
   IntPtr process,thread=IntPtr.Zero;
   if(!OpenProcessToken(GetCurrentProcess(),8,out process)) throw Error("Open process token");
   Token result=null;Exception failure=null;
   try {
    Token p=ReadToken(process),effective=p;
    if(OpenThreadToken(GetCurrentThread(),8,true,out thread)) effective=ReadToken(thread); else if(Marshal.GetLastWin32Error()!=1008) throw Error("Open effective thread token");
    if(p.sid!=effective.sid||p.authentication_id!=effective.authentication_id||p.session_id!=effective.session_id) throw new InvalidOperationException("Process and effective thread logon differ.");
    result=p;
   } catch(Exception e){failure=e;}
   if(thread!=IntPtr.Zero&&!CloseHandle(thread))failure=Join(failure,Error("Close thread token"));
   if(!CloseHandle(process))failure=Join(failure,Error("Close process token"));
   if(failure!=null)throw failure;return result;
  }
  static Exception Join(Exception first,Exception second){return first==null?second:new AggregateException(first,second);}
  public static DirectoryIdentity Directory(string path) {
   IntPtr handle=CreateFileW(path,0x80,7,IntPtr.Zero,3,0x02200000,IntPtr.Zero);
   if(handle==new IntPtr(-1)) throw Error("Open owned directory metadata");
   DirectoryIdentity result=null;Exception failure=null;
   try { FileInformation i; if(!GetFileInformationByHandle(handle,out i)) throw Error("Read owned directory identity");
    if((i.Attributes&0x10)==0||(i.Attributes&0x400)!=0) throw new InvalidOperationException("Owned directory is not a non-reparse directory.");
    result=new DirectoryIdentity{path=Path.GetFullPath(path),volume_serial_hex=i.Volume.ToString("x8"),file_index_hex=(((ulong)i.IndexHigh<<32)|i.IndexLow).ToString("x16"),attributes=i.Attributes};
   } catch(Exception e){failure=e;}
   if(!CloseHandle(handle))failure=Join(failure,Error("Close directory metadata handle"));
   if(failure!=null)throw failure;return result;
  }
  public static string Filesystem(string path) {
   var volume=new StringBuilder(32768); if(!GetVolumePathNameW(path,volume,(uint)volume.Capacity)) throw Error("Find checkout volume");
   var label=new StringBuilder(261); var filesystem=new StringBuilder(261); uint serial,component,flags;
   if(!GetVolumeInformationW(volume.ToString(),label,(uint)label.Capacity,out serial,out component,out flags,filesystem,(uint)filesystem.Capacity)) throw Error("Read checkout filesystem");
   return filesystem.ToString();
  }
  public static void CreatePrivateDirectory(string path,string sid) {
   IntPtr descriptor; uint size; if(!ConvertStringSecurityDescriptorToSecurityDescriptorW("O:"+sid+"D:P(A;OICI;FA;;;"+sid+")(A;OICI;FA;;;SY)",1,out descriptor,out size)) throw Error("Build private directory security");
   try { var a=new SecurityAttributes{Length=Marshal.SizeOf<SecurityAttributes>(),Descriptor=descriptor,Inherit=false}; if(!CreateDirectoryW(path,ref a)) throw Error("Create new private directory"); }
   finally { LocalFree(descriptor); }
  }
  public static string Device(string drive) {
   var bytes=new char[32768]; uint n=QueryDosDeviceW(drive,bytes,(uint)bytes.Length);
   if(n==0) { if(Marshal.GetLastWin32Error()==2)return null; throw Error("Query selected drive device"); }
   var values=new string(bytes,0,(int)n).Split(new[]{'\0'},StringSplitOptions.RemoveEmptyEntries);
   if(values.Length!=1)throw new InvalidOperationException("Selected drive has ambiguous DOS targets.");return values[0];
  }
  public static string Remote(string drive) {
   uint length=32768;var value=new StringBuilder((int)length);uint code=WNetGetConnectionW(drive,value,ref length);
   if(code==2250)return null;if(code!=0)throw new Win32Exception((int)code,"Read selected WNet mapping");return value.ToString();
  }
  public static void RemoveMapping(string drive) { uint code=WNetCancelConnection2W(drive,0,false);if(code!=0)throw new Win32Exception((int)code,"Remove exact owned WNet mapping"); }
  public static void ValidateJSON(string text) { using(var doc=JsonDocument.Parse(text,new JsonDocumentOptions{MaxDepth=32})) { Unique(doc.RootElement); } }
  static void Unique(JsonElement value) { if(value.ValueKind==JsonValueKind.Object){var keys=new HashSet<string>(StringComparer.Ordinal);foreach(var p in value.EnumerateObject()){if(!keys.Add(p.Name))throw new InvalidOperationException("Duplicate JSON field.");Unique(p.Value);}}else if(value.ValueKind==JsonValueKind.Array){foreach(var e in value.EnumerateArray())Unique(e);} }
 }
}
'@
}

function Initialize-InboxChild {
    if ('InboxReference.Child' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace InboxReference {
 public sealed class Child {
  static readonly ConcurrentDictionary<Child,byte> retained=new ConcurrentDictionary<Child,byte>();
  readonly Process process; readonly Stopwatch elapsed=Stopwatch.StartNew(); readonly int budget,drain;
  readonly TaskCompletionSource<bool> ready=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
  readonly ConcurrentQueue<string> lines=new ConcurrentQueue<string>(); readonly AutoResetEvent changed=new AutoResetEvent(false);
  readonly object outputLock=new object(),ownerLock=new object(); bool settledOwner,killInitiated; readonly TaskCompletionSource<bool> killFinished=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously); readonly MemoryStream stdout=new MemoryStream(),stderr=new MemoryStream();
  readonly CancellationTokenSource lifetime=new CancellationTokenSource();
  Task worker,outputTask,errorTask,inputTask; int stop; long firstStop=-1;
  public volatile bool Started,ExitConfirmed,Forced,Unsettled; public int PID,ExitCode=-1; public long ExitElapsedTicks,InputElapsedTicks,OutputElapsedTicks,ErrorElapsedTicks; public string StartUTC,Error,KillError,WaitError;
  public Child(string executable,string directory,int budgetMilliseconds,int drainMilliseconds) {
   budget=budgetMilliseconds;drain=drainMilliseconds;
   process=new Process{StartInfo=new ProcessStartInfo(executable){WorkingDirectory=directory,UseShellExecute=false,RedirectStandardInput=true,RedirectStandardOutput=true,RedirectStandardError=true,CreateNoWindow=true,StandardInputEncoding=new UTF8Encoding(false,true)}};
   process.StartInfo.ArgumentList.Add("-test.run=^TestNativeInboxReference$");process.StartInfo.ArgumentList.Add("-test.count=1");process.StartInfo.ArgumentList.Add("-test.timeout=60s");process.StartInfo.Environment["RFS_INBOX_REFERENCE"]="1";
   retained[this]=0;
   worker=Task.Run(()=>{
    try {
     if(!process.Start())throw new InvalidOperationException("Native reference process did not start.");
     Started=true;PID=process.Id;StartUTC=process.StartTime.ToUniversalTime().ToString("O");
     outputTask=Task.Run(()=>ReadOutput());errorTask=Task.Run(()=>ReadError());ready.TrySetResult(true);
    } catch(Exception e){Fail("Native reference launch failed: "+e.Message);ready.TrySetException(e);if(!Started){changed.Set();return;}Stop();}
    if(Volatile.Read(ref stop)!=0)KillOwned();
    try {process.WaitForExit();ExitElapsedTicks=elapsed.ElapsedTicks;ExitCode=process.ExitCode;ExitConfirmed=true;if(elapsed.Elapsed>=TimeSpan.FromMilliseconds(budget))Fail("Native reference exit confirmation exceeded its original lifetime.");} catch(Exception e){WaitError=e.Message;Fail("Process exit is unconfirmed: "+e.Message);}
    finally {changed.Set();}
   });
   _=Task.Run(async()=>{
    try {
     var first=await Task.WhenAny(worker,Task.Delay(Math.Max(0,budget-(int)elapsed.ElapsedMilliseconds),lifetime.Token));
     if(first!=worker || Started&&!ExitConfirmed){Fail("Native reference exceeded its owned lifetime.");Stop();await Task.WhenAny(worker,Task.Delay(RemainingDrain(),lifetime.Token));if(!ExitConfirmed)Unsettled=true;}
    } catch(OperationCanceledException){}
   });
  }
  void Fail(string value){lock(outputLock){Error=Error==null?value:Error+"; "+value;}changed.Set();}
  void Capture(MemoryStream target,byte[] bytes,int count,int maximum){lock(outputLock){int available=maximum-(int)target.Length;target.Write(bytes,0,Math.Min(available,count));if(count>available)throw new InvalidOperationException("Native reference output exceeded its bound.");}}
  void ReadOutput(){
   try {var buffer=new byte[1024];using(var line=new MemoryStream()){for(;;){int n=process.StandardOutput.BaseStream.Read(buffer,0,buffer.Length);if(n==0)break;Capture(stdout,buffer,n,131072);for(int i=0;i<n;i++){line.WriteByte(buffer[i]);if(line.Length>16384)throw new InvalidOperationException("Native reference line exceeded its bound.");if(buffer[i]==10){string text=new UTF8Encoding(false,true).GetString(line.ToArray()).TrimEnd('\r','\n');lines.Enqueue(text);line.SetLength(0);changed.Set();}}}if(line.Length!=0)throw new InvalidOperationException("Native reference ended with an incomplete line.");}}
   catch(Exception e){Fail(e.Message);Stop();}finally{OutputElapsedTicks=elapsed.ElapsedTicks;if(elapsed.Elapsed>=TimeSpan.FromMilliseconds(budget))Fail("Native stdout completion exceeded its original lifetime.");changed.Set();}
  }
  void ReadError(){try{var b=new byte[1024];for(;;){int n=process.StandardError.BaseStream.Read(b,0,b.Length);if(n==0)break;Capture(stderr,b,n,65536);}}catch(Exception e){Fail(e.Message);Stop();}finally{ErrorElapsedTicks=elapsed.ElapsedTicks;if(elapsed.Elapsed>=TimeSpan.FromMilliseconds(budget))Fail("Native stderr completion exceeded its original lifetime.");changed.Set();}}
  void KillOwned(){
   lock(ownerLock){if(settledOwner||killInitiated||!Started||ExitConfirmed)return;killInitiated=true;Forced=true;}
   try {_=Task.Run(()=>{try{if(!ExitConfirmed)process.Kill();}catch(Exception e){KillError=e.Message;}finally{killFinished.TrySetResult(true);changed.Set();}});}catch(Exception e){KillError=e.Message;killFinished.TrySetResult(true);}
  }
  public void Stop(){Interlocked.CompareExchange(ref firstStop,Math.Min(elapsed.ElapsedMilliseconds,budget),-1);Interlocked.Exchange(ref stop,1);KillOwned();}
  int RemainingDrain(){long at=Interlocked.Read(ref firstStop);return at<0?0:(int)Math.Max(0,at+drain-elapsed.ElapsedMilliseconds);}
  bool Await(Task task,int timeout){try{return task.Wait(timeout);}catch(AggregateException e){Fail(e.Flatten().Message);return task.IsCompleted;}}
  public bool WaitStarted(){int remaining=Math.Max(0,budget-(int)elapsed.ElapsedMilliseconds);try{return ready.Task.Wait(remaining)&&Started;}catch(AggregateException){return false;}}
  public string NextLine(){for(;;){string text;if(lines.TryDequeue(out text))return text;if(Error!=null)throw new InvalidOperationException(Error);if(worker.IsCompleted&&outputTask!=null&&outputTask.IsCompleted)return null;int remaining=budget-(int)elapsed.ElapsedMilliseconds;if(remaining<=0){Stop();throw new TimeoutException("Native reference stage exceeded its owned lifetime.");}changed.WaitOne(Math.Min(remaining,100));}}
  public void SendLine(string line){if(Encoding.UTF8.GetByteCount(line)+1>16384)throw new InvalidOperationException("Controller envelope exceeded its bound.");lock(ownerLock){if(settledOwner||Volatile.Read(ref stop)!=0||elapsed.ElapsedMilliseconds>=budget||!Started||ExitConfirmed||inputTask!=null&&!inputTask.IsCompleted)throw new InvalidOperationException("Native reference is not available for admission.");inputTask=Task.Run(()=>{try{process.StandardInput.WriteLine(line);process.StandardInput.Flush();}finally{InputElapsedTicks=elapsed.ElapsedTicks;if(elapsed.Elapsed>=TimeSpan.FromMilliseconds(budget))Fail("Native input completion exceeded its original lifetime.");}});}int remaining=Math.Max(0,budget-(int)elapsed.ElapsedMilliseconds);if(!Await(inputTask,remaining)){Stop();throw new TimeoutException("Controller envelope did not drain.");}if(inputTask.IsFaulted)throw inputTask.Exception;}
  public bool Finish(bool stopChild){
   if(stopChild)Stop();
   if(Volatile.Read(ref stop)==0&&!Await(worker,Math.Max(0,budget-(int)elapsed.ElapsedMilliseconds)))Stop();
   if(Volatile.Read(ref stop)!=0&&!Await(worker,RemainingDrain())){Unsettled=true;return false;}
   if(!Started){lifetime.Cancel();retained.TryRemove(this,out _);return false;}
   if(!ExitConfirmed){Unsettled=true;return false;}
   Task[] terminal;
   lock(ownerLock){settledOwner=true;if(!killInitiated)killFinished.TrySetResult(true);terminal=new[]{inputTask,outputTask,errorTask,killFinished.Task};}
   foreach(Task task in terminal){
    if(task==null)continue;
    int remaining=Volatile.Read(ref stop)==0?Math.Max(0,budget-(int)elapsed.ElapsedMilliseconds):RemainingDrain();
    if(!Await(task,remaining)){Stop();if(!Await(task,RemainingDrain())){Unsettled=true;return false;}}
   }
   lifetime.Cancel();retained.TryRemove(this,out _);process.Dispose();return true;
  }
  public byte[] Stdout(){lock(outputLock){return stdout.ToArray();}}
  public byte[] Stderr(){lock(outputLock){return stderr.ToArray();}}
  public bool OwnerRetained {get{return retained.ContainsKey(this);}}
 }
}
'@
}

function Get-InboxToken {
    $token = [InboxReference.Native]::CurrentToken()
    if (-not $token.elevated) { throw 'The inbox reference requires an already elevated current token.' }
    return [ordered]@{ sid=$token.sid; authentication_id=$token.authentication_id; session_id=$token.session_id }
}
function Get-InboxDirectory([string]$Path) {
    Assert-InboxNoReparse $Path
    $identity = [InboxReference.Native]::Directory($Path)
    return [ordered]@{path=$identity.path;volume_serial_hex=$identity.volume_serial_hex;file_index_hex=$identity.file_index_hex;attributes=$identity.attributes}
}
function Assert-InboxDirectory($Expected) {
    $actual = Get-InboxDirectory ([string]$Expected.path)
    foreach ($key in @('path','volume_serial_hex','file_index_hex','attributes')) {
        if ([string]$actual[$key] -cne [string]$Expected[$key]) { throw "Owned directory fingerprint changed: $key" }
    }
}
function Assert-InboxDACL([string]$Path,[string]$SID) {
    $acl = Get-Acl -LiteralPath $Path
    if (-not $acl.AreAccessRulesProtected -or $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -cne $SID) { throw 'Owned directory owner or protected DACL changed.' }
    $rules = @($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
    if ($rules.Count -ne 2) { throw 'Owned directory has unexpected access rules.' }
    $seen = @{}
    foreach ($rule in $rules) {
        $identity=$rule.IdentityReference.Value
        if ($identity -cnotin @($SID,'S-1-5-18') -or $seen.ContainsKey($identity) -or $rule.IsInherited -or $rule.PropagationFlags -ne [Security.AccessControl.PropagationFlags]::None -or $rule.AccessControlType -ne 'Allow' -or $rule.FileSystemRights -ne [Security.AccessControl.FileSystemRights]::FullControl -or $rule.InheritanceFlags -ne ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit)) { throw 'Owned directory DACL is not the exact current-SID/SYSTEM policy.' }
        $seen[$identity]=$true
    }
}
function Resolve-InboxSID([string]$Account) {
    if ([string]::IsNullOrWhiteSpace($Account)) { throw 'Administrative account identity is missing.' }
    return ([Security.Principal.NTAccount]::new($Account)).Translate([Security.Principal.SecurityIdentifier]).Value
}
function Get-InboxDrive([string]$Drive) {
    $logical=[InboxReference.Native]::GetLogicalDrives()
    if ($logical -eq 0) { throw 'Logical drive inventory failed.' }
    return [ordered]@{drive=$Drive;logical=0 -ne ($logical -band (1 -shl ([int]$Drive[0]-[int][char]'A')));device=[InboxReference.Native]::Device($Drive);unc=[InboxReference.Native]::Remote($Drive)}
}
function Select-InboxDrive {
    foreach ($letter in [char[]]'ZYXWVUTSRQPONMLKJIHGFED') {
        $candidate = Get-InboxDrive "$letter`:"
        if (-not $candidate.logical -and $null -eq $candidate.device -and $null -eq $candidate.unc) { return $candidate.drive }
    }
    throw 'No unused drive letter is available.'
}

function Get-InboxSettings {
    $client=Get-SmbClientConfiguration
    $server=Get-SmbServerConfiguration
    $value=[ordered]@{client=[ordered]@{};server=[ordered]@{}}
    foreach($name in @('FileNotFoundCacheLifetime','DirectoryCacheLifetime','FileInfoCacheLifetime','EnableMultiChannel','EnableSecuritySignature','RequireSecuritySignature','EnableInsecureGuestLogons','RequireEncryption')) {
        $property=$client.PSObject.Properties[$name]
        $value.client[$name]=[ordered]@{available=$null -ne $property;value=$(if($null -ne $property){$property.Value}else{$null})}
    }
    foreach($name in @('EnableSMB2Protocol','EnableMultiChannel','EnableSecuritySignature','RequireSecuritySignature','EncryptData','RejectUnencryptedAccess','EnableLeasing')) {
        $property=$server.PSObject.Properties[$name]
        $value.server[$name]=[ordered]@{available=$null -ne $property;value=$(if($null -ne $property){$property.Value}else{$null})}
    }
    if ((Get-InboxRequired $client 'FileNotFoundCacheLifetime') -ne 5 -or (Get-InboxRequired $client 'DirectoryCacheLifetime') -ne 10 -or (Get-InboxRequired $client 'FileInfoCacheLifetime') -ne 10) { throw 'The unchanged comparison requires cache lifetimes 5/10/10.' }
    if ((Get-InboxRequired $server 'EnableSMB2Protocol') -ne $true) { throw 'Inbox SMB2 is not enabled.' }
    return $value
}
function Get-InboxBaselineCounts {
    $counts=[ordered]@{}
    foreach($command in @('Get-SmbShare','Get-SmbConnection','Get-SmbOpenFile','Get-SmbSession')) {
        $counter=0
        & $command -ErrorAction Stop | ForEach-Object { $counter++;if($counter -gt 512){throw "Administrative baseline exceeds its bound: $command"} }
        $counts[$command]=$counter
    }
    return $counts
}
function Get-InboxPreflight {
    if(-not $IsWindows -or [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() -cne 'Arm64'){throw 'Windows ARM64 is required.'}
    $os=Get-CimInstance Win32_OperatingSystem
    if($os.ProductType -ne 1 -or [int]$os.BuildNumber -lt 26100 -or ([version]$os.Version).Major -ne 10){throw 'Windows 11 24H2 or newer is required.'}
    foreach($name in @('Get-SmbShare','New-SmbShare','Remove-SmbShare','Get-SmbShareAccess','Get-SmbConnection','Get-SmbOpenFile','Get-SmbSession','Get-SmbClientConfiguration','Get-SmbServerConfiguration')){
        if((Get-Command $name).ModuleName -cne 'SmbShare'){throw "Inbox SmbShare command is unavailable: $name"}
    }
    $token=Get-InboxToken
    foreach($service in @('LanmanServer','LanmanWorkstation')) {if((Get-Service -Name $service).Status -ne 'Running'){throw "Required existing service is not running: $service"}}
    $listeners=0;Get-NetTCPConnection -State Listen -LocalPort 445 -ErrorAction Stop | ForEach-Object{$listeners++;if($listeners -gt 16){throw 'Port 445 listener inventory exceeds its bound.'}}
    if($listeners -eq 0){throw 'There is no existing direct port 445 listener.'}
    Assert-InboxNoReparse $script:Workspace
    if([InboxReference.Native]::Filesystem($script:Workspace) -cne 'NTFS'){throw 'The actual checkout volume must be NTFS.'}
    $computer=[Environment]::MachineName
    if($computer -cnotmatch '^[A-Za-z0-9_-]{1,63}$'){throw 'The actual computer name is not an accepted direct UNC server.'}
    $addresses=[Collections.Generic.List[string]]::new()
    Get-NetIPAddress -ErrorAction Stop | ForEach-Object {if($addresses.Count -ge 128){throw 'Local address inventory exceeds its bound.'};$addresses.Add(([Net.IPAddress]::Parse($_.IPAddress)).ToString())}
    return [ordered]@{token=$token;computer=$computer;addresses=@($addresses);os_build=[string]$os.BuildNumber;architecture='Arm64';filesystem='NTFS';settings=Get-InboxSettings;baseline_counts=Get-InboxBaselineCounts}
}
function Get-InboxShare([string]$Name) {
    $found=[Collections.Generic.List[object]]::new();$scanned=0
    Get-SmbShare -ErrorAction Stop | ForEach-Object {
        $scanned++;if($scanned -gt 512){throw 'Share inventory exceeds its bound.'}
        if([string](Get-InboxRequired $_ 'Name') -ceq $Name){if($found.Count -ge 2){throw 'Owned share name is ambiguous.'};$found.Add($_)}
    }
    if($found.Count -gt 1){throw 'Owned share name occurs in multiple scopes.'}
    if($found.Count -eq 1){return $found[0]}
    return $null
}
function Get-InboxShareFingerprint($Share,[string]$SID) {
    $rules=[Collections.Generic.List[object]]::new()
    Get-SmbShareAccess -InputObject $Share -ErrorAction Stop | ForEach-Object {
        if($rules.Count -ge 8){throw 'Share access inventory exceeds its bound.'}
        $rules.Add([ordered]@{sid=Resolve-InboxSID ([string](Get-InboxRequired $_ 'AccountName'));type=[string](Get-InboxRequired $_ 'AccessControlType');right=[string](Get-InboxRequired $_ 'AccessRight')})
    }
    if($rules.Count -ne 1 -or $rules[0].sid -cne $SID -or $rules[0].type -cne 'Allow' -or $rules[0].right -cne 'Full'){throw 'Owned share access is not exactly current-SID Full.'}
    $temporary=Get-InboxRequired $Share 'Temporary'
    if($temporary -isnot [bool] -or -not $temporary -or [string](Get-InboxRequired $Share 'CachingMode') -cne 'None'){throw 'Owned share is not Temporary with offline caching disabled.'}
    $lease=$Share.PSObject.Properties['LeasingMode']
    $available=$null -ne $lease -and $null -ne $lease.Value
    $leasing=[ordered]@{available=$available;value=$(if($available){[string]$lease.Value}else{$null})}
    return [ordered]@{leasing_mode=$leasing;name=[string](Get-InboxRequired $Share 'Name');scope=[string](Get-InboxRequired $Share 'ScopeName');path=[string](Get-InboxRequired $Share 'Path');description=[string](Get-InboxRequired $Share 'Description');temporary=$temporary;caching_mode=[string](Get-InboxRequired $Share 'CachingMode');instance=[string](Get-InboxRequired $Share 'SmbInstance');encrypted=Get-InboxRequired $Share 'EncryptData';access=@($rules)}
}
function Assert-InboxShare($Ledger) {
    $share=Get-InboxShare $Ledger.share_name
    if($null -eq $share){throw 'Owned share is absent before its confirmed removal.'}
    $fingerprint=Get-InboxShareFingerprint $share $Ledger.token.sid
    if((ConvertTo-Json $fingerprint -Depth 8 -Compress) -cne (ConvertTo-Json $Ledger.share -Depth 8 -Compress)){throw 'Owned share fingerprint changed.'}
    return $share
}
function Convert-InboxLocalPath([string]$Path) {
    $value=$Path.TrimEnd('\')
    if($value -cnotmatch '^[A-Za-z]:\\' -or $value.Contains('/') -or $value.Contains([char]0)){throw 'Administrative path is not an explicit local DOS path.'}
    foreach($part in $value.Substring(3).Split('\')){if($part -in @('','.', '..') -or $part.IndexOfAny([char[]]':*?"<>|') -ge 0){throw 'Administrative path contains an ambiguous component.'}}
    return $value
}
function Test-InboxOwnedPath([string]$Path,[string]$Root) {
    $pathValue=Convert-InboxLocalPath $Path;$rootValue=Convert-InboxLocalPath $Root
    return $pathValue.Equals($rootValue,[StringComparison]::OrdinalIgnoreCase) -or $pathValue.StartsWith($rootValue+'\',[StringComparison]::OrdinalIgnoreCase)
}
function Assert-InboxLocalClient([string]$Name,$Ledger,[bool]$AllowUnzoned=$false) {
    if($Name.Equals($Ledger.computer,[StringComparison]::OrdinalIgnoreCase)){return}
    $text=$Name
    if($text.StartsWith('[') -and $text.EndsWith(']')){$text=$text.Substring(1,$text.Length-2)}
    $address=$null
    if(-not [Net.IPAddress]::TryParse($text,[ref]$address)){throw 'Administrative client is not the exact computer or a known local address.'}
    if($address.IsIPv4MappedToIPv6){$address=$address.MapToIPv4()}
    if([Net.IPAddress]::IsLoopback($address)){return}
    foreach($local in $Ledger.addresses){$candidate=[Net.IPAddress]::Parse($local);if($candidate.IsIPv4MappedToIPv6){$candidate=$candidate.MapToIPv4()};if($candidate.Equals($address)){return}}
    # This corroborates the fixture's administrative snapshot, without assigning a zone or establishing client identity.
    # Unzoned link-local addresses are not independent access-control selectors: https://www.rfc-editor.org/rfc/rfc4007.html#section-12
    if($AllowUnzoned -and $Name.Length -le 1024 -and -not $text.Contains('%') -and $text.IndexOfAny([char[]]'[]') -lt 0 -and $address.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetworkV6 -and $address.IsIPv6LinkLocal -and $address.ScopeId -eq 0 -and -not $address.IsIPv4MappedToIPv6 -and @($Ledger.addresses).Count -le 128){
        $bounded=$true
        foreach($local in $Ledger.addresses){if($local -isnot [string] -or $local.Length -gt 1024){$bounded=$false;break}}
        if($bounded){
            $bytes=[Convert]::ToHexString($address.GetAddressBytes());$matches=0;$matched=$null;$matchedInput=$null
            foreach($local in $Ledger.addresses){
                $candidate=[Net.IPAddress]::Parse($local)
                if($candidate.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetworkV6 -and $candidate.IsIPv6LinkLocal -and [Convert]::ToHexString($candidate.GetAddressBytes()) -ceq $bytes){$matches++;$matched=$candidate;$matchedInput=$local}
            }
            if($matches -eq 1 -and $matched.ScopeId -ne 0){
                return [ordered]@{mode='unique_unzoned_link_local_inventory_match';evaluated_argument=$Name;peer=Get-InboxAddressObservation $address;local_input=$matchedInput;local=Get-InboxAddressObservation $matched;exact_equals=$address.Equals($matched)}
            }
        }
    }
    throw 'Administrative client address is not local.'
}
function Get-InboxAddressObservation([Net.IPAddress]$Address) {
    $scope=$null
    if($Address.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetworkV6){$scope=$Address.ScopeId.ToString([Globalization.CultureInfo]::InvariantCulture)}
    return [ordered]@{family=$Address.AddressFamily.ToString();presentation=$Address.ToString();bytes_hex=[Convert]::ToHexString($Address.GetAddressBytes()).ToLowerInvariant();scope_id=$scope;ipv4_mapped=$Address.IsIPv4MappedToIPv6}
}
function New-InboxOriginRefusal($Ledger,[int]$Stage,[string]$Kind,[string]$OpenID,[string]$SessionID,[string]$Scope,[string]$Instance,$Value,[string]$Argument,$Original) {
    $type=if($null -eq $Value){$null}else{$Value.GetType().FullName}
    $record=[ordered]@{stage=$Stage;source_kind=$Kind;capture_state='capture_incomplete';reason=$null;peer_type=$null;peer_type_length=$(if($null -eq $type){$null}else{$type.Length});argument_length=$Argument.Length;local_count=@($Ledger.addresses).Count;mechanism_trace_complete=$false}
    $fields=[ordered]@{source_sha=@([string]$Ledger.source_sha,40);nonce=@([string]$Ledger.nonce,64);open_id=@($OpenID,20);session_id=@($SessionID,20);scope=@($Scope,256);instance=@($Instance,256);computer=@([string]$Ledger.computer,63);original_error=@([string]$Original.Exception.Message,2048)}
    foreach($key in $fields.Keys){
        if($fields[$key][0].Length -gt $fields[$key][1]){$record.reason="Context field exceeds bound: $key";return $record}
        $record[$key]=$fields[$key][0]
    }
    $record.completed_checks=if($Kind -ceq 'session'){'owned_path_connection_credential_session_join_session_sid'}else{'owned_path_connection_credential_session_join_session_sid_session_peer_open_sid'}
    if($null -eq $type -or $type.Length -gt 256){$record.reason='Provider type is unavailable or exceeds bound.';return $record}
    $record.peer_type=$type
    if($Argument.Length -gt 1024){$record.reason='Evaluated peer argument exceeds bound.';return $record}
    if($Value -isnot [string]){$record.reason='Provider peer value is not a string.';return $record}
    $record.evaluated_argument=$Argument
    $record.original_value=$Value
    if($record.local_count -gt 128){$record.reason='Local comparison inventory exceeds bound.';return $record}
    foreach($local in $Ledger.addresses){if($local -isnot [string] -or $local.Length -gt 1024){$record.reason='Local comparison input has unsupported type or length.';return $record}}
    $record.local_inputs=@($Ledger.addresses)
    $record.computer_name_equal=$Argument.Equals($Ledger.computer,[StringComparison]::OrdinalIgnoreCase)
    $text=$Argument
    if($text.StartsWith('[') -and $text.EndsWith(']')){$text=$text.Substring(1,$text.Length-2)}
    $peer=$null;$parsed=[Net.IPAddress]::TryParse($text,[ref]$peer)
    $record.peer=[ordered]@{parse_success=$parsed;before_conversion=$null;after_conversion=$null;loopback=$null}
    if($parsed){
        $record.peer.before_conversion=Get-InboxAddressObservation $peer
        if($peer.IsIPv4MappedToIPv6){$peer=$peer.MapToIPv4()}
        $record.peer.after_conversion=Get-InboxAddressObservation $peer;$record.peer.loopback=[Net.IPAddress]::IsLoopback($peer)
    }
    $locals=[Collections.Generic.List[object]]::new();$malformed=$false
    foreach($local in $Ledger.addresses){
        $address=$null;$ok=[Net.IPAddress]::TryParse($local,[ref]$address)
        $row=[ordered]@{input=$local;parse_success=$ok;before_conversion=$null;after_conversion=$null;equals_peer=$null;equality_state='unavailable'}
        if($ok){
            $row.before_conversion=Get-InboxAddressObservation $address
            if($address.IsIPv4MappedToIPv6){$address=$address.MapToIPv4()}
            $row.after_conversion=Get-InboxAddressObservation $address
            if($parsed){$row.equals_peer=$address.Equals($peer);$row.equality_state='compared'}
        }else{$malformed=$true}
        $locals.Add($row)
    }
    $record.local_comparisons=@($locals)
    if($malformed){$record.reason='Local comparison input did not parse.'}else{$record.capture_state='captured'}
    return $record
}
function Write-InboxOriginRefusal([string]$Path,$Record) {
    $bytes=[Text.UTF8Encoding]::new($false).GetBytes((ConvertTo-Json -InputObject $Record -Depth 12 -Compress)+"`n")
    if($bytes.Length -gt 65536){
        $small=[ordered]@{capture_state='capture_incomplete';reason='Encoded observation exceeds bound.';encoded_length=$bytes.Length;mechanism_trace_complete=$false}
        foreach($key in @('stage','source_kind','source_sha','nonce','open_id','session_id','scope','instance','completed_checks','original_error','peer_type','argument_length','peer_type_length','local_count')){$small[$key]=$Record[$key]}
        $bytes=[Text.UTF8Encoding]::new($false).GetBytes((ConvertTo-Json -InputObject $small -Compress)+"`n")
    }
    $stream=[IO.File]::Open($Path,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
    $failure=$null
    try{$stream.Write($bytes);$stream.Flush($true)}catch{$failure=$_}
    try{$stream.Dispose()}catch{if($null -eq $failure){$failure=$_}else{$failure.Exception.Data['origin_capture_close_error']=$_.Exception.Message}}
    if($null -ne $failure){throw $failure}
}
function Assert-InboxSelectedPeer($Ledger,[int]$Stage,[string]$Kind,[string]$OpenID,[string]$SessionID,[string]$Scope,[string]$Instance,$Value) {
    $argument=[string]$Value
    try{
        $match=Assert-InboxLocalClient $argument $Ledger ($Value -is [string])
        if($null -ne $match){return $match}
        return
    }
    catch{
        $original=$_
        try{
            $record=New-InboxOriginRefusal $Ledger $Stage $Kind $OpenID $SessionID $Scope $Instance $Value $argument $original
            Write-InboxOriginRefusal (Join-Path $script:Results "origin-refusal-$Stage.json") $record
        }catch{
            $secondary=$_
            $original.Exception.Data['origin_capture_error']=$secondary
            try{
                $Ledger.errors+=,"Origin refusal capture failed: $($secondary.Exception.Message)"
                if($secondary.Exception.Data.Contains('origin_capture_close_error')){$Ledger.errors+=,"Origin refusal capture close failed: $($secondary.Exception.Data['origin_capture_close_error'])"}
            }catch{$original.Exception.Data['origin_capture_ledger_error']=$_}
        }
        throw $original
    }
}
function Get-InboxOwnedOpens($Ledger) {
    $owned=[Collections.Generic.List[object]]::new();$scanned=0
    Get-SmbOpenFile -IncludeHidden -ErrorAction Stop | ForEach-Object {
        $scanned++;if($scanned -gt 512){throw 'Open-file inventory exceeds its scanned-row bound.'}
        $path=[string](Get-InboxRequired $_ 'Path')
        $root=[string]$Ledger.root_local
        if($path.TrimEnd('\').Equals($root,[StringComparison]::OrdinalIgnoreCase) -or $path.StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)){
            if(-not (Test-InboxOwnedPath $path $root)){throw 'Selected owned open path is invalid.'}
            if($owned.Count -ge 32){throw 'Owned open-file inventory exceeds its bound.'};$owned.Add($_)
        }
    }
    return ,$owned.ToArray()
}
function Get-InboxOrigin($Ledger,[int]$Stage) {
    Assert-InboxToken $Ledger.token (Get-InboxToken)
    $null=Assert-InboxShare $Ledger
    $connections=[Collections.Generic.List[object]]::new();$scanned=0
    Get-SmbConnection -ErrorAction Stop | ForEach-Object {
        $scanned++;if($scanned -gt 512){throw 'Connection inventory exceeds its bound.'}
        if([string](Get-InboxRequired $_ 'ShareName') -ceq $Ledger.share_name){
            if(-not ([string](Get-InboxRequired $_ 'ServerName')).Equals($Ledger.computer,[StringComparison]::OrdinalIgnoreCase)){throw 'Owned share uses a different server name.'}
            if((Resolve-InboxSID ([string](Get-InboxRequired $_ 'Credential'))) -cne $Ledger.token.sid){throw 'Actual SMB connection credential SID differs.'}
            if($connections.Count -ge 8){throw 'Owned connection inventory exceeds its bound.'}
            $signed=Get-InboxRequired $_ 'Signed';$encrypted=Get-InboxRequired $_ 'Encrypted'
            if($signed -isnot [bool] -or $encrypted -isnot [bool]){throw 'SMB security properties are not actual Boolean observations.'}
            $connections.Add([ordered]@{server=[string]$_.ServerName;share=[string]$_.ShareName;credential_sid=$Ledger.token.sid;instance=[string](Get-InboxRequired $_ 'SmbInstance');dialect=[string](Get-InboxRequired $_ 'Dialect');signed=$signed;encrypted=$encrypted;opens=Convert-InboxID (Get-InboxRequired $_ 'NumOpens')})
        }
    }
    if($connections.Count -ne 1 -or $connections[0].opens -eq '0'){throw 'Owned client connection is absent or ambiguous.'}
    $opens=Get-InboxOwnedOpens $Ledger
    $ids=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $sessions=[ordered]@{};$rows=[Collections.Generic.List[object]]::new();$roles=@{root=$false;v=$false;target=$false}
    foreach($open in $opens){
        $id=Convert-InboxID (Get-InboxRequired $open 'FileId');$session=Convert-InboxID (Get-InboxRequired $open 'SessionId')
        if(-not $ids.Add($id)){throw 'Duplicate administrative open ID.'}
        $scope=[string](Get-InboxRequired $open 'ScopeName');$instance=[string](Get-InboxRequired $open 'SmbInstance')
        if($scope -cne $Ledger.share.scope -or $instance -cne $Ledger.share.instance -or $instance -cne $connections[0].instance){throw 'Owned open provider scope/instance differs.'}
        if(-not $sessions.Contains($session)){
            $matches=[Collections.Generic.List[object]]::new();$sessionScanned=0
            Get-SmbSession -SessionId ([uint64]$session) -SmbInstance $instance -IncludeHidden -ErrorAction Stop | ForEach-Object{$sessionScanned++;if($sessionScanned -gt 8){throw 'Session lookup exceeds its bound.'};$matches.Add($_)}
            if($matches.Count -ne 1){throw 'Owned open has no unique server session.'}
            $found=$matches[0]
            if((Convert-InboxID (Get-InboxRequired $found 'SessionId')) -cne $session -or [string](Get-InboxRequired $found 'ScopeName') -cne $scope -or [string](Get-InboxRequired $found 'SmbInstance') -cne $instance){throw 'Server session join changed its exact identity.'}
            if((Resolve-InboxSID ([string](Get-InboxRequired $found 'ClientUserName'))) -cne $Ledger.token.sid){throw 'Server-authenticated client SID differs.'}
            $peer=Get-InboxRequired $found 'ClientComputerName'
            $sessionPeerMatch=Assert-InboxSelectedPeer $Ledger $Stage 'session' $id $session $scope $instance $peer
            $sessions[$session]=[ordered]@{id=$session;scope=$scope;instance=$instance;sid=$Ledger.token.sid;local_client=$true}
            if($null -ne $sessionPeerMatch){$sessions[$session].peer_match=$sessionPeerMatch}
        }
        if((Resolve-InboxSID ([string](Get-InboxRequired $open 'ClientUserName'))) -cne $Ledger.token.sid){throw 'Open-file authenticated SID differs.'}
        $peer=Get-InboxRequired $open 'ClientComputerName'
        $openPeerMatch=Assert-InboxSelectedPeer $Ledger $Stage 'open' $id $session $scope $instance $peer
        $path=Convert-InboxLocalPath ([string](Get-InboxRequired $open 'Path'))
        $relative=[string](Get-InboxRequired $open 'ShareRelativePath')
        $expected=$path.Substring($Ledger.root_local.Length).TrimStart('\')
        if(-not $relative.Trim('\').Equals($expected,[StringComparison]::OrdinalIgnoreCase)){throw 'Owned open local and share-relative paths disagree.'}
        if($path.Equals($Ledger.root_local,[StringComparison]::OrdinalIgnoreCase)){$roles.root=$true}
        if($path.Equals($Ledger.root_local+'\v',[StringComparison]::OrdinalIgnoreCase)){$roles.v=$true}
        if($path.Equals($Ledger.root_local+'\v\target',[StringComparison]::OrdinalIgnoreCase)){$roles.target=$true}
        $row=[ordered]@{id=$id;session=$session;path=$path;relative_path=$relative;scope=$scope;instance=$instance}
        if($null -ne $openPeerMatch){$row.peer_match=$openPeerMatch}
        $rows.Add($row)
    }
    if(-not $roles.root -or -not $roles.v -or -not $roles.target){throw 'Administrative snapshot lacks explicit root/v/target open roles.'}
    $raw=[ordered]@{stage=$Stage;nonce=$Ledger.nonce;connections=@($connections);sessions=@($sessions.Values);opens=@($rows);mechanism_trace_complete=$false}
    $path=Join-Path $script:Results "origin-$Stage.json";Write-InboxJSON $path $raw
    return [ordered]@{share_unc=$Ledger.share_unc;root_local=$Ledger.root_local;sid=$Ledger.token.sid;server=$Ledger.computer;session_ids=@($sessions.Keys);open_ids=@($rows | ForEach-Object{$_.id});open_paths=@($rows | ForEach-Object{$_.path});sha256=Get-InboxSHA256 $path}
}

function Assert-InboxEnvelope($Message,$Ledger,[int]$PIDExpected) {
    $allowed=@('kind','stage','nonce','pid','executable_sha256','token','mapping','receipt_file','receipt_sha256','outcome','cleanup_complete','mechanism_trace_complete','error')
    foreach($key in $Message.Keys){if($key -cnotin $allowed){throw 'Native envelope contains an unknown field.'}}
    if([string](Get-InboxRequired $Message 'nonce') -cne $Ledger.nonce -or [string](Get-InboxRequired $Message 'executable_sha256') -cne $Ledger.executable_sha256 -or (Convert-InboxID (Get-InboxRequired $Message 'pid')) -cne ([string]$PIDExpected)){throw 'Native envelope owner binding differs.'}
    Assert-InboxToken $Ledger.token (Get-InboxRequired $Message 'token')
    $mapping=Get-InboxRequired $Message 'mapping'
    if([string](Get-InboxRequired $mapping 'drive') -cne $Ledger.drive -or [string](Get-InboxRequired $mapping 'unc') -cne $Ledger.share_unc){throw 'Native mapping binding differs.'}
    $mechanism=Get-InboxRequired $Message 'mechanism_trace_complete'
    if($mechanism -isnot [bool] -or $mechanism){throw 'Inbox reference cannot claim a complete mechanism trace.'}
}
function Convert-InboxEnvelope([string]$Line) {
    if(-not $Line.StartsWith($script:ProtocolPrefix,[StringComparison]::Ordinal)){return $null}
    if([Text.Encoding]::UTF8.GetByteCount($Line)+1 -gt 16384){throw 'Native envelope exceeds its bound.'}
    $json=$Line.Substring($script:ProtocolPrefix.Length)
    [InboxReference.Native]::ValidateJSON($json)
    $value=ConvertFrom-Json -InputObject $json -AsHashtable -Depth 32
    if($value -isnot [Collections.IDictionary]){throw 'Tagged native envelope is not an object.'}
    return $value
}
function Send-InboxEnvelope($Child,$Value) {
    $text=ConvertTo-Json -InputObject $Value -Depth 12 -Compress
    $Child.SendLine($script:ProtocolPrefix+$text)
}
function Start-InboxOwnedChild($Ledger) { return [InboxReference.Child]::new($Ledger.executable,(Join-Path $script:Probe 'fixture'),60000,5000) }
function Set-InboxOperation([string]$Kind,[string]$Target) {
    $script:Ledger.operation=[ordered]@{kind=$Kind;target=$Target;started=[DateTime]::UtcNow.ToString('O');settled=$false}
    Save-InboxLedger
}
function Complete-InboxOperation {
    $script:Ledger.operation.settled=$true
    Save-InboxLedger
}
function New-InboxPrivateDirectory([string]$Path,[string]$SID) { [InboxReference.Native]::CreatePrivateDirectory($Path,$SID) }
function Remove-InboxNativeMapping([string]$Drive) { [InboxReference.Native]::RemoveMapping($Drive) }
function New-InboxResources {
    foreach($path in @($script:Ledger.owned_root,$script:Ledger.root_local,(Join-Path $script:Ledger.root_local 'v'))){
        Set-InboxOperation 'create-directory' $path
        New-InboxPrivateDirectory $path $script:Ledger.token.sid
        $record=Get-InboxDirectory $path
        Assert-InboxDACL $path $script:Ledger.token.sid
        $script:Ledger.directories+=,$record
        Complete-InboxOperation
    }
    if($null -ne (Get-InboxShare $script:Ledger.share_name)){throw 'The unpredictable share name is already occupied.'}
    Set-InboxOperation 'create-share' $script:Ledger.share_name
    $null=New-SmbShare -Name $script:Ledger.share_name -Path $script:Ledger.root_local -Description $script:Ledger.description -Temporary -CachingMode None -SecurityDescriptor "D:(A;;FA;;;$($script:Ledger.token.sid))" -ErrorAction Stop
    $share=Get-InboxShare $script:Ledger.share_name
    if($null -eq $share){throw 'Share creation has no read-back object.'}
    $script:Ledger.share=Get-InboxShareFingerprint $share $script:Ledger.token.sid
    if($script:Ledger.share.name -cne $script:Ledger.share_name -or $script:Ledger.share.path -cne $script:Ledger.root_local -or $script:Ledger.share.description -cne $script:Ledger.description){throw 'Created share does not match its exact intent.'}
    Complete-InboxOperation
}
function Remove-InboxMapping($Ledger) {
    Assert-InboxToken $Ledger.token (Get-InboxToken)
    $actual=Get-InboxDrive $Ledger.drive
    if($null -eq $actual.unc -and $null -eq $actual.device -and -not $actual.logical){return}
    if($null -eq $Ledger.mapping -or $actual.unc -cne $Ledger.share_unc -or $actual.device -cne $Ledger.mapping.device -or -not $actual.logical){throw 'Mapping generation is not the recorded owned binding.'}
    Set-InboxOperation 'remove-mapping' $Ledger.drive
    Remove-InboxNativeMapping $Ledger.drive
    $after=Get-InboxDrive $Ledger.drive
    if($null -ne $after.unc -or $null -ne $after.device -or $after.logical){throw 'Owned mapping removal is not confirmed.'}
    Complete-InboxOperation
}
function Remove-InboxResources {
    if($null -eq $script:Ledger){return}
    if($null -ne $script:Ledger.operation -and -not $script:Ledger.operation.settled){throw 'An administrative operation has no terminal result; resources remain owned and unsettled.'}
    if($script:Ledger.child.attempted -and (-not $script:Ledger.child.exit_confirmed -or $script:Ledger.child.unsettled)){throw 'Native child or its owned transport is unsettled; downstream cleanup is forbidden.'}
    Assert-InboxToken $script:Ledger.token (Get-InboxToken)
    if($script:Ledger.child.attempted){Remove-InboxMapping $script:Ledger}
    if(-not $script:Ledger.directory_removed){foreach($directory in $script:Ledger.directories){Assert-InboxDirectory $directory;Assert-InboxDACL $directory.path $script:Ledger.token.sid}}elseif(Test-Path -LiteralPath $script:Ledger.owned_root){throw 'A retired directory path reappeared.'}
    if($script:Ledger.share_removed -and $null -ne (Get-InboxShare $script:Ledger.share_name)){throw 'A retired share name reappeared.'}
    if($null -ne $script:Ledger.share -and -not $script:Ledger.share_removed){
        $share=Assert-InboxShare $script:Ledger
        Set-InboxOperation 'remove-share' $script:Ledger.share_name
        Remove-SmbShare -InputObject $share -Force -Confirm:$false -ErrorAction Stop
        if($null -ne (Get-InboxShare $script:Ledger.share_name)){throw 'Owned share removal is not confirmed.'}
        $script:Ledger.share_removed=$true
        Complete-InboxOperation
    }
    if($null -ne $script:Ledger.share){$residual=Get-InboxOwnedOpens $script:Ledger;if($residual.Count -ne 0){throw 'Owned path retains server opens after share removal.'}}
    if($script:Ledger.directories.Count -ne 0 -and -not $script:Ledger.directory_removed){
        if($null -ne $script:Ledger.share -and -not $script:Ledger.share_removed){throw 'Directory deletion requires confirmed share removal.'}
        $count=0
        Get-ChildItem -LiteralPath $script:Ledger.owned_root -Force -Recurse | ForEach-Object {
            $count++;if($count -gt 16){throw 'Owned directory contents exceed their bound.'}
            if($_.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Owned cleanup tree contains a reparse entry.'}
            if(-not $_.PSIsContainer -and $_.FullName -cnotin @((Join-Path $script:Ledger.root_local 'v/target'),(Join-Path $script:Ledger.root_local 'v/replacement'))){throw 'Owned tree contains an unexpected file.'}
        }
        Set-InboxOperation 'remove-directory' $script:Ledger.owned_root
        Remove-Item -LiteralPath $script:Ledger.owned_root -Recurse -Force -ErrorAction Stop
        if(Test-Path -LiteralPath $script:Ledger.owned_root){throw 'Owned directory removal is not confirmed.'}
        $script:Ledger.directory_removed=$true
        Complete-InboxOperation
    }
    $script:Ledger.cleanup_complete=$true
    Save-InboxLedger
}
function Read-InboxLedger {
    if(-not (Test-Path -LiteralPath $script:LedgerPath)){throw 'Owned-resource ledger is absent.'}
    $raw=[IO.File]::ReadAllText($script:LedgerPath)
    if([Text.Encoding]::UTF8.GetByteCount($raw) -gt 1048576){throw 'Owned-resource ledger exceeds its bound.'}
    [InboxReference.Native]::ValidateJSON($raw)
    $value=ConvertFrom-Json $raw -AsHashtable -Depth 32
    if($value.version -ne 1 -or $value.source_sha -cne $env:RFS_INBOX_SOURCE_SHA -or $value.nonce -cnotmatch '^[0-9a-f]{64}$'){throw 'Owned-resource ledger binding differs.'}
    $expected=Join-Path $script:Probe ('owned-'+$value.nonce.Substring(0,24))
    if($value.owned_root -cne $expected -or $value.root_local -cne (Join-Path $expected 'share') -or $value.share_name -cne ('rfs-inbox-'+$value.nonce.Substring(0,24)) -or $value.share_unc -cne ('\\'+$value.computer+'\'+$value.share_name)){throw 'Owned-resource ledger paths differ from their nonce.'}
    return $value
}
function Invoke-InboxPrepare {
    if($env:GITHUB_RUN_ATTEMPT -cne '1'){throw 'The reference permits only the first invocation of reviewed source.'}
    if(Test-Path -LiteralPath $script:LedgerPath){throw 'This checkout already owns a reference ledger.'}
    $preflight=Get-InboxPreflight
    $nonce=[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
    $owned=Join-Path $script:Probe ('owned-'+$nonce.Substring(0,24));$share='rfs-inbox-'+$nonce.Substring(0,24)
    $script:Ledger=[ordered]@{version=1;source_sha=$env:RFS_INBOX_SOURCE_SHA;nonce=$nonce;computer=$preflight.computer;addresses=$preflight.addresses;token=$preflight.token;settings=$preflight.settings;owned_root=$owned;root_local=Join-Path $owned 'share';share_name=$share;share_unc='\\'+$preflight.computer+'\'+$share;description='inbox-reference:'+ $nonce;drive=Select-InboxDrive;directories=@();share=$null;mapping=$null;operation=$null;share_removed=$false;directory_removed=$false;cleanup_complete=$false;errors=@();cleanup_errors=@();child=[ordered]@{attempted=$false;exit_confirmed=$false;pid=0;start_utc=$null;forced=$false;unsettled=$false};executable=$null;executable_sha256=$null}
    Save-InboxLedger
    Write-InboxJSON (Join-Path $script:Results 'preflight.json') $preflight
}
function Invoke-InboxRun {
    $script:Ledger=Read-InboxLedger
    $guard=[IO.File]::Open((Join-Path $script:Results 'native-attempt.lock'),[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None);$guard.Dispose()
    Assert-InboxToken $script:Ledger.token (Get-InboxToken)
    if((ConvertTo-Json (Get-InboxSettings) -Depth 12 -Compress) -cne (ConvertTo-Json $script:Ledger.settings -Depth 12 -Compress)){throw 'SMB settings changed after preflight.'}
    $script:Ledger.executable=Join-Path $script:Results 'native-inbox-reference.test.exe'
    Assert-InboxNoReparse $script:Ledger.executable
    $script:Ledger.executable_sha256=Get-InboxSHA256 $script:Ledger.executable
    Save-InboxLedger
    $child=$null;$terminal=$null;$stage=1;$primary=$null
    try {
        New-InboxResources
        $free=Get-InboxDrive $script:Ledger.drive
        if($free.logical -or $null -ne $free.device -or $null -ne $free.unc){throw 'The single selected drive became occupied.'}
        $script:Ledger.child.attempted=$true;Save-InboxLedger
        $child=Start-InboxOwnedChild $script:Ledger
        if(-not $child.WaitStarted()){throw 'Native reference launch did not settle.'}
        $script:Ledger.child.pid=$child.PID;$script:Ledger.child.start_utc=$child.StartUTC;Save-InboxLedger
        $directory=@($script:Ledger.directories | Where-Object{$_.path -ceq $script:Ledger.root_local})
        if($directory.Count -ne 1){throw 'The share-root identity is not unique.'}
        Send-InboxEnvelope $child ([ordered]@{kind='start';stage=0;nonce=$script:Ledger.nonce;source_sha=$script:Ledger.source_sha;executable_sha256=$script:Ledger.executable_sha256;parent_pid=$PID;computer=$script:Ledger.computer;root_local=$script:Ledger.root_local;share_unc=$script:Ledger.share_unc;drive=$script:Ledger.drive;expected_token=$script:Ledger.token;directory=$directory[0];cache_seconds=[ordered]@{file=10;directory=10;not_found=5};output_root=$script:Results})
        while($true){
            $line=$child.NextLine()
            if($null -eq $line){break}
            $message=Convert-InboxEnvelope $line
            if($null -eq $message){continue}
            if($null -ne $terminal){throw 'Native process sent another envelope after its terminal result.'}
            Assert-InboxEnvelope $message $script:Ledger $child.PID
            $kind=[string](Get-InboxRequired $message 'kind');$number=Get-InboxRequired $message 'stage'
            if($number -isnot [long] -and $number -isnot [int]){throw 'Native stage is not an exact integer.'}
            if($kind -ceq 'result' -and $number -eq 3){$terminal=$message;continue}
            $expected=if($stage -eq 1){'ready'}elseif($stage -eq 2){'postcheck'}else{throw 'Native process exceeded the fixed stage sequence.'}
            if($kind -cne $expected -or $number -ne $stage){throw 'Native process stage is duplicated or out of order.'}
            $mapping=Get-InboxDrive $script:Ledger.drive
            if(-not $mapping.logical -or $mapping.unc -cne $script:Ledger.share_unc -or [string]::IsNullOrEmpty($mapping.device)){throw 'Native mapping has no exact read-back binding.'}
            if($null -eq $script:Ledger.mapping){$script:Ledger.mapping=$mapping;Save-InboxLedger}elseif((ConvertTo-Json $mapping -Compress) -cne (ConvertTo-Json $script:Ledger.mapping -Compress)){throw 'Owned mapping fingerprint changed between stages.'}
            $origin=Get-InboxOrigin $script:Ledger $stage
            Send-InboxEnvelope $child ([ordered]@{kind='admit';stage=$stage;nonce=$script:Ledger.nonce;pid=$child.PID;executable_sha256=$script:Ledger.executable_sha256;origin=$origin})
            $stage++
        }
        if($null -eq $terminal){throw 'Native process ended without a terminal result.'}
        if($terminal.receipt_file -cne 'native-receipt.json' -or $terminal.receipt_sha256 -cnotmatch '^[0-9a-f]{64}$'){throw 'Native result does not bind its receipt.'}
        $nativePath=Join-Path $script:Results 'native-receipt.json';Assert-InboxNoReparse $nativePath
        if((Get-Item -LiteralPath $nativePath).Length -gt 131072 -or (Get-InboxSHA256 $nativePath) -cne $terminal.receipt_sha256){throw 'Native result receipt hash or size differs.'}
        $native=ConvertFrom-Json ([IO.File]::ReadAllText($nativePath)) -AsHashtable -Depth 32
        if($native.Request.nonce -cne $script:Ledger.nonce -or $native.Request.source_sha -cne $script:Ledger.source_sha -or $native.PID -ne $child.PID -or $native.ExecutableSHA256 -cne $script:Ledger.executable_sha256 -or $native.Outcome -cne $terminal.outcome -or $native.MechanismTraceComplete){throw 'Native receipt owner or outcome differs.'}
        if($stage -ne 3 -or -not $terminal.cleanup_complete -or $terminal.outcome -cne 'native_behavior_pass'){throw 'Native reference did not produce a complete passing behavior result.'}
    } catch {$primary=$_.Exception.Message;$script:Ledger.errors+=,$primary}
    finally {
        if($null -ne $child){
            $settled=$false
            try {$settled=$child.Finish($null -ne $primary)} catch {$script:Ledger.errors+=,$_.Exception.Message}
            $script:Ledger.child.exit_confirmed=$child.ExitConfirmed;$script:Ledger.child.pid=$child.PID;$script:Ledger.child.start_utc=$child.StartUTC;$script:Ledger.child.forced=$child.Forced;$script:Ledger.child.unsettled=$child.Unsettled -or $child.OwnerRetained
            $script:Ledger.child.exit_code=$child.ExitCode;$script:Ledger.child.owner_retained=$child.OwnerRetained;$script:Ledger.child.started=$child.Started
            $script:Ledger.child.error=$child.Error;$script:Ledger.child.wait_error=$child.WaitError;$script:Ledger.child.kill_error=$child.KillError
            $script:Ledger.child.exit_elapsed_ticks=$child.ExitElapsedTicks;$script:Ledger.child.clock_frequency=[Diagnostics.Stopwatch]::Frequency
            $script:Ledger.child.input_elapsed_ticks=$child.InputElapsedTicks;$script:Ledger.child.stdout_elapsed_ticks=$child.OutputElapsedTicks;$script:Ledger.child.stderr_elapsed_ticks=$child.ErrorElapsedTicks
            [IO.File]::WriteAllBytes((Join-Path $script:Results 'native-stdout.bin'),$child.Stdout());[IO.File]::WriteAllBytes((Join-Path $script:Results 'native-stderr.bin'),$child.Stderr())
            if($child.Stderr().Length -ne 0){$script:Ledger.errors+=,'Native reference wrote stderr; exact bytes are preserved.'}
            if(-not $settled -or -not $child.ExitConfirmed -or $child.ExitCode -ne 0 -or $child.Forced -or $child.OwnerRetained -or $null -ne $child.Error){$script:Ledger.errors+=,"Native process failed: exit=$($child.ExitCode) forced=$($child.Forced) unsettled=$($child.OwnerRetained) error=$($child.Error) wait=$($child.WaitError) kill=$($child.KillError)"}
        }
        Save-InboxLedger
        try {Remove-InboxResources} catch {$script:Ledger.cleanup_complete=$false;$script:Ledger.cleanup_errors+=,$_.Exception.Message;Save-InboxLedger}
        Write-InboxJSON (Join-Path $script:Results 'controller-result.json') ([ordered]@{source_sha=$script:Ledger.source_sha;nonce=$script:Ledger.nonce;candidate=$terminal;child=$script:Ledger.child;errors=$script:Ledger.errors;cleanup_errors=$script:Ledger.cleanup_errors;cleanup_complete=$script:Ledger.cleanup_complete;mechanism_trace_complete=$false})
    }
    if($script:Ledger.errors.Count -ne 0 -or $script:Ledger.cleanup_errors.Count -ne 0 -or -not $script:Ledger.cleanup_complete){throw 'Inbox reference failed; original observations and cleanup errors are preserved.'}
}
function Invoke-InboxVerify {
    if(-not (Test-Path -LiteralPath $script:LedgerPath)){Write-InboxJSON (Join-Path $script:Results 'cleanup-audit.json') ([ordered]@{ledger_present=$false;cleanup_complete=$false;mechanism_trace_complete=$false});throw 'No ownership ledger exists for cleanup verification.'}
    $script:Ledger=Read-InboxLedger
    try {
        Remove-InboxResources
        if((ConvertTo-Json (Get-InboxSettings) -Depth 12 -Compress) -cne (ConvertTo-Json $script:Ledger.settings -Depth 12 -Compress)){throw 'SMB settings differ from their recorded unchanged baseline.'}
        if($null -ne (Get-InboxShare $script:Ledger.share_name)){throw 'Owned share remains after cleanup.'}
        $drive=Get-InboxDrive $script:Ledger.drive
        if($drive.logical -or $null -ne $drive.device -or $null -ne $drive.unc){throw 'Selected drive is not absent after cleanup.'}
        if(Test-Path -LiteralPath $script:Ledger.owned_root){throw 'Owned directory remains after cleanup.'}
    } catch {$script:Ledger.cleanup_complete=$false;$script:Ledger.cleanup_errors+=,$_.Exception.Message;Save-InboxLedger}
    Write-InboxJSON (Join-Path $script:Results 'cleanup-audit.json') ([ordered]@{child=$script:Ledger.child;cleanup_complete=$script:Ledger.cleanup_complete;errors=$script:Ledger.errors;cleanup_errors=$script:Ledger.cleanup_errors;mechanism_trace_complete=$false})
    if($script:Ledger.errors.Count -ne 0 -or $script:Ledger.cleanup_errors.Count -ne 0 -or -not $script:Ledger.cleanup_complete){throw 'Reference or cleanup failure remains unresolved; an empty snapshot cannot erase it.'}
}

function Initialize-InboxScratch {
    foreach($directory in @($script:Probe,$script:Results,(Join-Path $script:Probe 'tmp'),(Join-Path $script:Probe 'dotnet-home'))){New-Item -ItemType Directory -Path $directory -Force | Out-Null}
    foreach($name in @('TEMP','TMP','TMPDIR')){[Environment]::SetEnvironmentVariable($name,(Join-Path $script:Probe 'tmp'),'Process')}
    $env:DOTNET_CLI_HOME=Join-Path $script:Probe 'dotnet-home'
    $env:POWERSHELL_CLI_TELEMETRY_OPTOUT='1'
    foreach($pair in @{GOCACHE='go-build';GOMODCACHE='go-mod';GOPATH='go-path'}.GetEnumerator()){
        $directory=Join-Path $script:Probe $pair.Value;New-Item -ItemType Directory -Path $directory -Force | Out-Null
        [Environment]::SetEnvironmentVariable($pair.Key,$directory,'Process')
        if($env:GITHUB_ENV){Add-Content -LiteralPath $env:GITHUB_ENV -Value ($pair.Key+'='+$directory)}
    }
    if($env:GITHUB_ENV){foreach($name in @('TEMP','TMP','TMPDIR','DOTNET_CLI_HOME')){Add-Content -LiteralPath $env:GITHUB_ENV -Value ($name+'='+[Environment]::GetEnvironmentVariable($name,'Process'))}}
}
function Assert-InboxSource {
    if($env:RFS_INBOX_SOURCE_SHA -cnotmatch '^[0-9a-f]{40}$'){throw 'The reviewed source commit is required.'}
    $actual=(& git -C $script:Workspace rev-parse HEAD).Trim()
    if($LASTEXITCODE -ne 0 -or $actual -cne $env:RFS_INBOX_SOURCE_SHA){throw 'Checkout differs from the reviewed source commit.'}
    & git -C $script:Workspace diff --exit-code --quiet
    if($LASTEXITCODE -ne 0){throw 'Reviewed source has tracked modifications.'}
    if($Phase -ne 'Verify'){
        $version=(& go version) -join ' '
        if($LASTEXITCODE -ne 0 -or $version -cnotmatch '^go version go1\.26\.8 windows/arm64$'){throw 'The native reference requires Go 1.26.8 for Windows ARM64.'}
    }
}

try {
    Initialize-InboxScratch
    if(-not $IsWindows){throw 'The native controller requires Windows PowerShell 7.'}
    Initialize-InboxNative
    Initialize-InboxChild
    Assert-InboxSource
    switch($Phase){
        'Prepare' {Invoke-InboxPrepare}
        'Run' {Invoke-InboxRun}
        'Verify' {Invoke-InboxVerify}
    }
} catch {
    if(Test-Path -LiteralPath $script:Results){
        Write-InboxJSON (Join-Path $script:Results ("controller-$($Phase.ToLowerInvariant())-failure.json")) ([ordered]@{phase=$Phase;error=$_.Exception.Message;mechanism_trace_complete=$false})
    }
    Write-Error $_
    exit 1
}
exit 0
