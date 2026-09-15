package windows

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	Found      *bool              `json:"found"`
	Created    *bool              `json:"created"`
	Record     *mappingRecord     `json:"record"`
	Error      string             `json:"error"`
	Code       uint32             `json:"code"`
	Diagnostic *mappingDiagnostic `json:"diagnostic,omitempty"`
}

type mappingDiagnostic struct {
	Phase      string             `json:"phase"`
	Line       uint32             `json:"line"`
	Category   uint32             `json:"category"`
	ErrorID    string             `json:"errorID"`
	Detail     string             `json:"detail"`
	Exceptions []mappingException `json:"exceptions"`
	Truncated  bool               `json:"truncated"`
}

type mappingException struct {
	Type          string  `json:"type"`
	Code          uint32  `json:"code"`
	Message       string  `json:"message"`
	MIResult      *uint32 `json:"miResult"`
	CIMStatusCode *uint32 `json:"cimStatusCode"`
	Win32Code     *int32  `json:"win32Code"`
}

type mappingExceptionCause struct {
	value mappingException
	next  error
}

func (e *mappingExceptionCause) Error() string {
	text := fmt.Sprintf("%s HRESULT 0x%08x: %s", e.value.Type, e.value.Code, e.value.Message)
	if e.value.MIResult != nil {
		text += fmt.Sprintf("; MI result=%d", *e.value.MIResult)
	}
	if e.value.CIMStatusCode != nil {
		text += fmt.Sprintf("; CIM status=%d", *e.value.CIMStatusCode)
	}
	if e.value.Win32Code != nil {
		text += fmt.Sprintf("; Win32 code=%d", *e.value.Win32Code)
	}
	if e.next != nil {
		text += ": " + e.next.Error()
	}
	return text
}

func (e *mappingExceptionCause) Unwrap() error { return e.next }

func mappingCommandFailure(reply mappingReply) error {
	var cause error
	switch reply.Error {
	case "":
		return nil
	case "owner":
		cause = ErrMappingOwnership
	case "busy":
		cause = ErrMappingBusy
	case "verification":
		cause = ErrMappingVerification
	case "native":
		cause = fmt.Errorf("Windows SMB mapping command failed with HRESULT 0x%08x", reply.Code)
	default:
		cause = ErrMappingVerification
	}
	d := reply.Diagnostic
	if d == nil {
		return cause
	}
	var inner error
	for i := len(d.Exceptions) - 1; i >= 0; i-- {
		inner = &mappingExceptionCause{value: d.Exceptions[i], next: inner}
	}
	return fmt.Errorf("Windows SMB mapping phase=%s line=%d category=%d error_id=%q detail=%q truncated=%t: %w",
		d.Phase, d.Line, d.Category, d.ErrorID, d.Detail, d.Truncated, errors.Join(cause, inner))
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
	if d := reply.Diagnostic; d != nil {
		if len(d.Phase) > 64 || len(d.ErrorID) > 256 || len(d.Detail) > 1024 || len(d.Exceptions) > 4 {
			return reply, errors.New("mapping command diagnostic exceeds its bound")
		}
		for _, e := range d.Exceptions {
			if len(e.Type) > 256 || len(e.Message) > 1024 {
				return reply, errors.New("mapping command exception exceeds its bound")
			}
		}
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
$answer = @{found=$false; created=$false; record=$null; error=''; code=0;
    diagnostic=@{phase='read-input'; line=0; category=0; errorID=''; detail=''; exceptions=@(); truncated=$false}}
function DiagnosticText([string]$text, [int]$limit) {
    $end = [Math]::Min($text.Length, $limit)
    while ($end -gt 0 -and [Text.Encoding]::UTF8.GetByteCount($text.Substring(0,$end)) -gt $limit) { $end-- }
    if ($end -gt 0 -and [char]::IsHighSurrogate($text[$end-1])) { $end-- }
    if ($end -lt $text.Length) { $answer.diagnostic.truncated = $true }
    return $text.Substring(0,$end)
}
function Record($mapping) {
    return @{localPath=[string]$mapping.LocalPath; remotePath=[string]$mapping.RemotePath;
        status=$mapping.CimInstanceProperties['Status'].Value}
}
try {
    $request = [Console]::In.ReadToEnd() | ConvertFrom-Json
    $answer.diagnostic.phase = 'identity'
    if ([Security.Principal.WindowsIdentity]::GetCurrent().User.Value -cne $request.ownerSID -or
        [Diagnostics.Process]::GetCurrentProcess().SessionId -ne $request.sessionID) {
        $answer.error = 'owner'
    } elseif ($request.operation -notin @('query','create') -or $request.localPath -cnotmatch '^[A-Z]:$') {
        $answer.error = 'protocol'
    } else {
        $moduleRoot = [IO.Path]::Combine([Environment]::GetFolderPath('System'),'WindowsPowerShell\v1.0\Modules')
        $answer.diagnostic.phase = 'import-smb'
        Import-Module -Name ([IO.Path]::Combine($moduleRoot,'SmbShare\SmbShare.psd1')) -ErrorAction Stop
        $answer.diagnostic.phase = 'import-cim'
        Import-Module -Name ([IO.Path]::Combine($moduleRoot,'CimCmdlets\CimCmdlets.psd1')) -ErrorAction Stop
        $filter = "LocalPath = '" + $request.localPath + "'"
        $answer.diagnostic.phase = 'query'
        $mappings = @(CimCmdlets\Get-CimInstance -Namespace 'ROOT/Microsoft/Windows/SMB' -ClassName 'MSFT_SmbMapping' -Filter $filter)
        if ($mappings.Count -gt 1) { throw 'mapping query returned multiple instances' }
        if ($mappings.Count -eq 1) {
            $answer.found = $true
            $answer.diagnostic.phase = 'query-record'
            $answer.record = Record $mappings[0]
        }
        if ($request.operation -eq 'create') {
            if ($answer.found) {
                $answer.error = 'busy'
            } else {
                $answer.diagnostic.phase = 'parameters'
                $parameters = (Get-Command 'SmbShare\New-SmbMapping').Parameters
                $required = @{TcpPort=[uint16];UseWriteThrough=[bool];RequireIntegrity=[bool];Persistent=[bool]}
                foreach ($name in $required.Keys) {
                    $parameter = $parameters[$name]
                    if ($null -eq $parameter) {
                        $answer.error = 'verification'
                        $answer.diagnostic.detail = DiagnosticText ($answer.diagnostic.detail + " Missing parameter $name.") 1024
                        continue
                    }
                    $type = $parameter.ParameterType
                    $underlying = [Nullable]::GetUnderlyingType($type)
                    if ($null -ne $underlying) { $type = $underlying }
                    if ($type -ne $required[$name]) {
                        $answer.error = 'verification'
                        $answer.diagnostic.detail = DiagnosticText ($answer.diagnostic.detail + " Parameter $name has type $($type.FullName), expected $($required[$name].FullName).") 1024
                    }
                }
                foreach ($name in @('TransportType','GlobalMapping','SaveCredentials')) {
                    if (-not $parameters.ContainsKey($name)) {
                        $answer.error = 'verification'
                        $answer.diagnostic.detail = DiagnosticText ($answer.diagnostic.detail + " Missing parameter $name.") 1024
                    }
                }
                if ($answer.error -eq '') {
                    $options = @{LocalPath=[string]$request.localPath; RemotePath=[string]$request.remotePath;
                        TcpPort=[uint16]$request.tcpPort; UseWriteThrough=$true; RequireIntegrity=$true;
                        Persistent=$false; TransportType='TCP'; GlobalMapping=$false; SaveCredentials=$false;
                        Confirm=$false; ErrorAction='Stop'}
                    $answer.diagnostic.phase = 'create'
                    $created = SmbShare\New-SmbMapping @options
                    $answer.created = $true
                    $answer.found = $true
                    $answer.diagnostic.phase = 'created-record'
                    $answer.record = Record $created
                    $answer.diagnostic.phase = 'query-created'
                    $confirmed = @(CimCmdlets\Get-CimInstance -Namespace 'ROOT/Microsoft/Windows/SMB' -ClassName 'MSFT_SmbMapping' -Filter $filter)
                    if ($confirmed.Count -ne 1) { throw 'created mapping was not observable' }
                    $answer.diagnostic.phase = 'verify-created'
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
    $failure = $_
    $answer.error = 'native'
    $answer.code = [BitConverter]::ToUInt32([BitConverter]::GetBytes([int]$failure.Exception.HResult),0)
    $answer.diagnostic.line = [uint32]$failure.InvocationInfo.ScriptLineNumber
    $answer.diagnostic.category = [uint32]$failure.CategoryInfo.Category
    $answer.diagnostic.errorID = DiagnosticText ([string]$failure.FullyQualifiedErrorId) 256
    $answer.diagnostic.detail = DiagnosticText ([string]$failure.ErrorDetails.Message) 1024
    $exception = $failure.Exception
    while ($null -ne $exception -and $answer.diagnostic.exceptions.Count -lt 4) {
        $item = @{type=(DiagnosticText ($exception.GetType().FullName) 256);
            code=[BitConverter]::ToUInt32([BitConverter]::GetBytes([int]$exception.HResult),0);
            message=(DiagnosticText $exception.Message 1024); miResult=$null; cimStatusCode=$null; win32Code=$null}
        if ($exception -is [ComponentModel.Win32Exception]) {
            $item.win32Code = [int32]$exception.NativeErrorCode
        } elseif ($exception.GetType().FullName -eq 'Microsoft.Management.Infrastructure.CimException') {
            $item.miResult = [uint32]$exception.NativeErrorCode
            if ($null -ne $exception.ErrorData) {
                $status = $exception.ErrorData.CimInstanceProperties['CIMStatusCode']
                if ($null -ne $status -and $null -ne $status.Value) { $item.cimStatusCode = [uint32]$status.Value }
            }
        }
        $answer.diagnostic.exceptions += $item
        $exception = $exception.InnerException
    }
    if ($null -ne $exception) { $answer.diagnostic.truncated = $true }
}
[Console]::Out.WriteLine(($answer | ConvertTo-Json -Compress -Depth 6))
[Console]::Out.Flush()
`
