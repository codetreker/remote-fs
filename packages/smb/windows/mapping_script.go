package windows

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf16"
)

type mappingCommand struct {
	Operation  string `json:"operation"`
	LocalPath  string `json:"localPath"`
	RemotePath string `json:"remotePath"`
	TCPPort    uint16 `json:"tcpPort"`
	OwnerSID   string `json:"ownerSID"`
	SessionID  uint32 `json:"sessionID"`
}

type mappingReply struct {
	Found   *bool          `json:"found"`
	Created *bool          `json:"created"`
	Record  *mappingRecord `json:"record"`
	Error   string         `json:"error"`
	Code    uint32         `json:"code"`
}

func decodeMappingReply(body []byte) (mappingReply, error) {
	var reply mappingReply
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return reply, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return reply, errors.New("mapping command returned trailing output")
	}
	if reply.Found == nil || reply.Created == nil || *reply.Found && reply.Record == nil || *reply.Created && reply.Record == nil {
		return reply, errors.New("mapping command omitted its outcome")
	}
	return reply, nil
}

func encodedMappingScript() string {
	words := utf16.Encode([]rune(mappingScript))
	data := make([]byte, 2*len(words))
	for i, word := range words {
		binary.LittleEndian.PutUint16(data[2*i:], word)
	}
	return base64.StdEncoding.EncodeToString(data)
}

// Only this fixed program enters the command line. Values arrive as JSON on
// stdin, so a share name can never become PowerShell syntax or an argument.
const mappingScript = `
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::InputEncoding = [Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$answer = @{found=$false; created=$false; record=$null; error=''; code=0}
function Record($mapping) {
    return @{localPath=[string]$mapping.LocalPath; remotePath=[string]$mapping.RemotePath;
        status=$mapping.CimInstanceProperties['Status'].Value}
}
try {
    $request = [Console]::In.ReadToEnd() | ConvertFrom-Json
    if ([Security.Principal.WindowsIdentity]::GetCurrent().User.Value -cne $request.ownerSID -or
        [Diagnostics.Process]::GetCurrentProcess().SessionId -ne $request.sessionID) {
        $answer.error = 'owner'
    } elseif ($request.operation -notin @('query','create') -or $request.localPath -cnotmatch '^[A-Z]:$') {
        $answer.error = 'protocol'
    } else {
        $moduleRoot = [IO.Path]::Combine([Environment]::GetFolderPath('System'),'WindowsPowerShell\v1.0\Modules')
        Import-Module -Name ([IO.Path]::Combine($moduleRoot,'SmbShare\SmbShare.psd1')) -ErrorAction Stop
        Import-Module -Name ([IO.Path]::Combine($moduleRoot,'CimCmdlets\CimCmdlets.psd1')) -ErrorAction Stop
        $filter = "LocalPath = '" + $request.localPath + "'"
        $mappings = @(CimCmdlets\Get-CimInstance -Namespace 'ROOT/Microsoft/Windows/SMB' -ClassName 'MSFT_SmbMapping' -Filter $filter)
        if ($mappings.Count -gt 1) { throw 'mapping query returned multiple instances' }
        if ($mappings.Count -eq 1) {
            $answer.found = $true
            $answer.record = Record $mappings[0]
        }
        if ($request.operation -eq 'create') {
            if ($answer.found) {
                $answer.error = 'busy'
            } else {
                $parameters = (Get-Command 'SmbShare\New-SmbMapping').Parameters
                $required = @{TcpPort=[uint16];UseWriteThrough=[bool];RequireIntegrity=[bool];Persistent=[bool]}
                foreach ($name in $required.Keys) {
                    $parameter = $parameters[$name]
                    if ($null -eq $parameter) { $answer.error = 'verification'; continue }
                    $type = $parameter.ParameterType
                    $underlying = [Nullable]::GetUnderlyingType($type)
                    if ($null -ne $underlying) { $type = $underlying }
                    if ($type -ne $required[$name]) { $answer.error = 'verification' }
                }
                foreach ($name in @('TransportType','GlobalMapping','SaveCredentials')) {
                    if (-not $parameters.ContainsKey($name)) { $answer.error = 'verification' }
                }
                if ($answer.error -eq '') {
                    $options = @{LocalPath=[string]$request.localPath; RemotePath=[string]$request.remotePath;
                        TcpPort=[uint16]$request.tcpPort; UseWriteThrough=$true; RequireIntegrity=$true;
                        Persistent=$false; TransportType='TCP'; GlobalMapping=$false; SaveCredentials=$false;
                        Confirm=$false; ErrorAction='Stop'}
                    $created = SmbShare\New-SmbMapping @options
                    $answer.created = $true
                    $answer.found = $true
                    $answer.record = Record $created
                    $confirmed = @(CimCmdlets\Get-CimInstance -Namespace 'ROOT/Microsoft/Windows/SMB' -ClassName 'MSFT_SmbMapping' -Filter $filter)
                    if ($confirmed.Count -ne 1) { throw 'created mapping was not observable' }
                    $current = Record $confirmed[0]
                    foreach ($field in @('localPath','remotePath')) {
                        if ($answer.record[$field] -cne $current[$field]) { $answer.error = 'owner' }
                    }
                    if ($answer.error -eq '') { $answer.record = $current }
                }
            }
        }
    }
} catch {
    $answer.error = 'native'
    $answer.code = [BitConverter]::ToUInt32([BitConverter]::GetBytes([int]$_.Exception.HResult),0)
}
[Console]::Out.WriteLine(($answer | ConvertTo-Json -Compress -Depth 4))
[Console]::Out.Flush()
`
