package httprest

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func transferClientCommand(content []byte) storage.ImportCommand {
	command := storage.ImportCommand{
		Action:   storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "import"},
		Target:   storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}},
		Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate,
		DeclaredBytes: int64(len(content)), Digest: sha256.Sum256(content),
	}
	command.Fingerprint = command.EffectFingerprint()
	return command
}

func transferClientDial(t *testing.T, handler http.HandlerFunc) *Storage {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func transferClientJSON(t *testing.T, response transferResponse) []byte {
	t.Helper()
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestTransferClientRejectsMalformedResponses(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	declared := command.DeclaredBytes
	seal := storage.ImportSeal{Bytes: declared, Digest: command.Digest}
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: declared}}
	valid := map[string]transferResponse{
		"check":        {Capabilities: &transferCapabilities{MaxBytes: 1024, Replication: true}},
		"begin":        {Token: strings.Repeat("a", 64), DeclaredBytes: &declared},
		"resume":       {Token: strings.Repeat("a", 64)},
		"receive":      {Seal: &seal},
		"query":        {Receipt: transferReceiptOf(receipt)},
		"publish":      {Receipt: transferReceiptOf(receipt), Barrier: &MutationBarrier{Incarnation: "incarnation", Position: 1}},
		"export":       {Token: strings.Repeat("b", 64), Info: &transferExportInfo{Attr: AttrOf(receipt.Attr), Digest: command.Digest[:]}},
		"cancel":       {Close: &referenceCloseResult{Released: true, Determined: true}},
		"close-export": {Close: &referenceCloseResult{Released: true, Determined: true}},
	}
	for op, response := range valid {
		t.Run(op, func(t *testing.T) {
			encoded := transferClientJSON(t, response)
			if _, err := decodeTransferResponse(encoded, op); err != nil {
				t.Fatalf("valid response: %v", err)
			}
			if _, err := decodeTransferResponse([]byte(`{}`), op); err == nil {
				t.Fatal("absent result accepted")
			}
			extra := bytes.TrimSuffix(encoded, []byte("}"))
			extra = append(extra, []byte(`,"unrecognized":true}`)...)
			if _, err := decodeTransferResponse(extra, op); err == nil {
				t.Fatal("unknown member accepted")
			}
		})
	}
	for name, failure := range map[string]struct{ op, body string }{
		"omitted replication":         {"check", `{"capabilities":{"max_bytes":1024}}`},
		"zero ceiling":                {"check", `{"capabilities":{"max_bytes":0,"replication":false}}`},
		"invalid capability":          {"begin", `{"token":"short","declared_bytes":3}`},
		"negative length":             {"resume", `{"token":"` + strings.Repeat("a", 64) + `","declared_bytes":-1}`},
		"omitted close proof":         {"cancel", `{"close":{"released":true}}`},
		"contradictory close proof":   {"close-export", `{"close":{"released":true,"determined":false}}`},
		"unknown close without error": {"cancel", `{"close":{"released":false,"determined":false}}`},
		"truncated seal":              {"receive", `{"seal":{"Bytes":0,"Digest":[]}}`},
		"unrelated result":            {"check", `{"capabilities":{"max_bytes":1024,"replication":false},"token":"` + strings.Repeat("a", 64) + `"}`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTransferResponse([]byte(failure.body), failure.op); err == nil {
				t.Fatal("malformed response accepted")
			}
		})
	}
}

func TestTransferClientImportStreamsAndChecksPublication(t *testing.T) {
	content := bytes.Repeat([]byte("content"), 25000)
	command := transferClientCommand(content)
	declared := command.DeclaredBytes
	token := strings.Repeat("a", 64)
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: declared}}
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderProtocol, Version)
		if strings.HasSuffix(r.URL.Path, "/file-transfer-data") {
			if r.Method != http.MethodPost || r.URL.Query().Get("path") != token || r.Header.Get("Content-Type") != contentOctets {
				t.Error("invalid import stream request")
			}
			seal, err := storage.CopyTransferContent(r.Context(), io.Discard, r.Body, declared, command.Digest)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write(transferClientJSON(t, transferResponse{Seal: &seal}))
			return
		}
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		var response transferResponse
		switch request.Op {
		case "check":
			response.Capabilities = &transferCapabilities{MaxBytes: declared, Replication: true}
		case "begin":
			response.Token, response.DeclaredBytes = token, &declared
		case "publish":
			response.Receipt, response.Barrier = transferReceiptOf(receipt), &MutationBarrier{Incarnation: "epoch", Position: 4}
		case "query":
			response.Receipt = transferReceiptOf(receipt)
		case "cancel":
			response.Close = &referenceCloseResult{Released: true, Determined: true}
		default:
			t.Errorf("unexpected operation %q", request.Op)
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	if err := client.CheckFileTransfer(); err != nil {
		t.Fatal(err)
	}
	imported, err := client.BeginImport(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	reader := &transferClientBoundedReader{Reader: bytes.NewReader(content)}
	seal, err := imported.Receive(t.Context(), reader)
	if err != nil || seal.Bytes != declared || seal.Digest != command.Digest {
		t.Fatalf("receive = %+v, %v", seal, err)
	}
	if reader.maximum > 64<<10 {
		t.Fatalf("request buffer grew to %d", reader.maximum)
	}
	result, barrier, err := imported.(*remoteImport).PublishWithBarrier(t.Context())
	if err != nil || result.Action != command.Action || result.Attr.ID != 2 || barrier == nil || barrier.Position != 4 {
		t.Fatalf("publication = %+v, %+v, %v", result, barrier, err)
	}
	queried, err := client.QueryImport(t.Context(), command.Action)
	if err != nil || queried.Action != command.Action || queried.Fingerprint != command.Fingerprint {
		t.Fatalf("query = %+v, %v", queried, err)
	}
	closed, err := imported.Cancel(t.Context())
	if err != nil || !closed.Released || !closed.Determined {
		t.Fatalf("cancel = %+v, %v", closed, err)
	}
}

type transferClientBoundedReader struct {
	io.Reader
	maximum int
}

func (r *transferClientBoundedReader) Read(buffer []byte) (int, error) {
	r.maximum = max(r.maximum, len(buffer))
	return r.Reader.Read(buffer)
}

func TestTransferClientRejectsUnrelatedPublicationAndMissingBarrier(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	valid := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	for _, test := range []struct {
		name    string
		mutate  func(*storage.ImportReceipt)
		barrier *MutationBarrier
	}{
		{name: "missing barrier"},
		{name: "different actor", mutate: func(r *storage.ImportReceipt) { r.Action.SubjectID = "other" }, barrier: &MutationBarrier{Incarnation: "epoch", Position: 1}},
		{name: "different command", mutate: func(r *storage.ImportReceipt) { r.Fingerprint[0]++ }, barrier: &MutationBarrier{Incarnation: "epoch", Position: 1}},
		{name: "different size", mutate: func(r *storage.ImportReceipt) { r.Attr.Size++ }, barrier: &MutationBarrier{Incarnation: "epoch", Position: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := valid
			if test.mutate != nil {
				test.mutate(&receipt)
			}
			client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(HeaderProtocol, Version)
				_, _ = w.Write(transferClientJSON(t, transferResponse{Receipt: transferReceiptOf(receipt), Barrier: test.barrier}))
			})
			file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command, replication: true}
			if result, err := file.Publish(t.Context()); err == nil || test.name != "missing barrier" && result.Action != (storage.ActionKey{}) {
				t.Fatalf("unverified publication accepted: %+v, %v", result, err)
			}
		})
	}
}

func TestTransferClientExportChecksCapturedContent(t *testing.T) {
	content := []byte("captured content")
	digest := sha256.Sum256(content)
	for _, test := range []struct {
		name    string
		content []byte
		marker  bool
		length  bool
		want    error
	}{
		{name: "complete", content: content, marker: true, length: true},
		{name: "changed digest", content: []byte("modified content"), marker: true, want: syscall.EIO},
		{name: "truncated", content: content[:len(content)-1], marker: true, want: syscall.EINVAL},
		{name: "trailing byte", content: append(bytes.Clone(content), 'x'), marker: true, want: syscall.EFBIG},
		{name: "foreign response", content: content, want: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
				if test.marker {
					w.Header().Set(HeaderProtocol, Version)
				}
				if test.length {
					w.Header().Set("Content-Length", strconv.Itoa(len(test.content)))
				} else {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write(test.content)
			})
			file := &remoteExport{storage: client, token: strings.Repeat("b", 64), info: storage.ExportInfo{Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: int64(len(content))}, Digest: digest, DigestAvailable: true}}
			var destination bytes.Buffer
			err := file.WriteTo(t.Context(), &destination)
			if test.want == nil {
				if err != nil || !bytes.Equal(destination.Bytes(), content) {
					t.Fatalf("export = %q, %v", destination.Bytes(), err)
				}
			} else if err == nil {
				t.Fatal("invalid export stream accepted")
			} else if test.name != "foreign response" && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestTransferClientExportAdmissionAndCloseFacts(t *testing.T) {
	content := []byte("captured")
	digest := sha256.Sum256(content)
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderProtocol, Version)
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		var response transferResponse
		switch request.Op {
		case "export":
			response = transferResponse{Token: strings.Repeat("a", 64), Info: &transferExportInfo{Attr: AttrOf(storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 8}), Digest: digest[:]}}
		case "close-export", "cancel":
			response = transferResponse{Close: &referenceCloseResult{Released: false, Determined: true}, Error: &ErrorResponse{Errno: "EIO", Message: "native release failed"}}
		default:
			t.Errorf("unexpected op %q", request.Op)
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file, err := client.ExportFile(t.Context(), storage.ExportCommand{NodeID: 2})
	if err != nil || !file.Info().DigestAvailable || file.Info().Digest != digest {
		t.Fatalf("export admission = %v, %v", file, err)
	}
	result, err := file.CloseWithResult(t.Context())
	if result.Released || !result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("close fact lost: %+v, %v", result, err)
	}
	imported := &remoteImport{storage: client, token: strings.Repeat("a", 64)}
	result, err = imported.Cancel(t.Context())
	if result.Released || !result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("cancel fact lost: %+v, %v", result, err)
	}
	if file, err := client.ExportFile(t.Context(), storage.ExportCommand{NodeID: 3}); err == nil || file != nil {
		t.Fatal("different captured node accepted")
	}
}

func TestTransferClientContentSealRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name, input string
		declared    int64
		digest      [sha256.Size]byte
		want        error
	}{
		{name: "correct", input: "abc", declared: 3, digest: sha256.Sum256([]byte("abc"))},
		{name: "empty", declared: 0, digest: sha256.Sum256(nil)},
		{name: "short", input: "ab", declared: 3, digest: sha256.Sum256([]byte("abc")), want: syscall.EINVAL},
		{name: "long", input: "abcd", declared: 3, digest: sha256.Sum256([]byte("abc")), want: syscall.EFBIG},
		{name: "digest", input: "abd", declared: 3, digest: sha256.Sum256([]byte("abc")), want: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &transferSealingReader{source: strings.NewReader(test.input), remaining: test.declared, digest: test.digest, hash: sha256.New()}
			_, err := io.Copy(io.Discard, reader)
			if test.want == nil {
				if err != nil || reader.check() != nil {
					t.Fatalf("valid seal rejected: %v, %v", err, reader.check())
				}
			} else if !errors.Is(err, test.want) || !errors.Is(reader.check(), test.want) {
				t.Fatalf("invalid stream accepted: %v, %v", err, reader.check())
			}
		})
	}
	reader := &transferSealingReader{source: strings.NewReader("abc"), remaining: 3, digest: sha256.Sum256([]byte("abc")), hash: sha256.New()}
	if reader.check() == nil {
		t.Fatal("unconsumed source acknowledged")
	}
	resumed := &remoteImport{}
	if _, err := resumed.Receive(t.Context(), strings.NewReader("abc")); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("resumed receive: %v", err)
	}
	stalled := &transferSealingReader{source: transferClientStalledReader{}, hash: sha256.New(), digest: sha256.Sum256(nil)}
	if _, err := io.Copy(io.Discard, stalled); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("stalled source error = %v", err)
	}
}

type transferClientStalledReader struct{}

func (transferClientStalledReader) Read([]byte) (int, error) { return 0, nil }

func TestTransferClientPreservesUnsettledReleasedClose(t *testing.T) {
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, transferResponse{
			Close: &referenceCloseResult{Released: true, Determined: false},
			Error: &ErrorResponse{Errno: "EIO", Message: "settlement is unknown"},
		}))
	})
	file := &remoteExport{storage: client, token: strings.Repeat("a", 64)}
	result, err := file.CloseWithResult(t.Context())
	if !result.Released || result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("settlement facts lost: %+v, %v", result, err)
	}
	if file.closed != nil {
		t.Fatal("unsettled release cached as completed close")
	}
}

func TestTransferClientPublicationSettlesImportLocally(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			t.Error("terminal publication replay contacted its retired token")
		}
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Op != "publish" {
			t.Errorf("settled import sent unnecessary request: %q", request.Op)
		}
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, transferResponse{Receipt: transferReceiptOf(receipt), Close: &referenceCloseResult{Released: true, Determined: true}}))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command}
	if _, err := file.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Publish(t.Context()); err != nil || result.Action != command.Action {
		t.Fatalf("terminal replay = %+v, %v", result, err)
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("settled cancellation = %+v, %v", result, err)
	}
}

func TestTransferClientResumesOnlySealedWork(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set(HeaderProtocol, Version)
		var response transferResponse
		switch request.Op {
		case "check":
			response.Capabilities = &transferCapabilities{MaxBytes: 1024}
		case "resume":
			if request.Action == nil || *request.Action != command.Action {
				t.Error("resume changed durable identity")
			}
			response.Token = strings.Repeat("a", 64)
		case "publish":
			response.Receipt = transferReceiptOf(receipt)
			response.Close = &referenceCloseResult{Released: true, Determined: true}
		default:
			t.Errorf("unexpected op %q", request.Op)
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file, err := client.ResumeImport(t.Context(), command.Action)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Receive(t.Context(), strings.NewReader("abc")); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("resumed file received unknown declaration: %v", err)
	}
	if result, err := file.Publish(t.Context()); err != nil || result.Action != command.Action {
		t.Fatalf("resumed publication = %+v, %v", result, err)
	}
}

func TestTransferClientReleasedExportClosesLocally(t *testing.T) {
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			t.Error("repeated export close contacted retired token")
		}
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}}))
	})
	file := &remoteExport{storage: client, token: strings.Repeat("a", 64)}
	for range 2 {
		if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
			t.Fatalf("close = %+v, %v", result, err)
		}
	}
}

func TestTransferClientRecoversPublicationBarrierByDurableAction(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set(HeaderProtocol, Version)
		response := transferResponse{Receipt: transferReceiptOf(receipt)}
		switch calls.Add(1) {
		case 1:
			if request.Op != "publish" {
				t.Errorf("first operation = %q", request.Op)
			}
			response.Close = &referenceCloseResult{Released: true, Determined: true}
			response.Error = &ErrorResponse{Errno: "EIO", Message: "barrier capture failed"}
		case 2:
			if request.Op != "query" || request.Action == nil || *request.Action != command.Action {
				t.Errorf("barrier recovery did not query original action: %+v", request)
			}
			response.Barrier = &MutationBarrier{Incarnation: "epoch", Position: 2}
		default:
			t.Error("settled receipt contacted retired token")
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command, replication: true}
	result, barrier, err := file.PublishWithBarrier(t.Context())
	if result.Action != command.Action || barrier != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("publication failure lost receipt = %+v, %+v, %v", result, barrier, err)
	}
	for range 2 {
		result, barrier, err = file.PublishWithBarrier(t.Context())
		if err != nil || result.Action != command.Action || barrier == nil || barrier.Position != 2 {
			t.Fatalf("barrier recovery = %+v, %+v, %v", result, barrier, err)
		}
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("known settlement = %+v, %v", result, err)
	}
}

func TestTransferClientDetachRetiresHandleWithoutCancel(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Error("detached handle contacted its retired token")
		}
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Op != "detach" {
			t.Errorf("detach sent %q", request.Op)
		}
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}}))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command}
	for range 2 {
		result, err := file.Detach(t.Context())
		if err != nil || !result.Released || !result.Determined {
			t.Fatalf("detach = %+v, %v", result, err)
		}
	}
	if _, err := file.Receive(t.Context(), strings.NewReader("abc")); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached receive = %v", err)
	}
	if _, err := file.Publish(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached publication = %v", err)
	}
	if result, err := file.Cancel(t.Context()); !errors.Is(err, syscall.ESTALE) || !result.Released || !result.Determined {
		t.Fatalf("detached cancel = %+v, %v", result, err)
	}
}

func TestTransferClientFailedDetachKeepsCancelResponsibility(t *testing.T) {
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set(HeaderProtocol, Version)
		var response transferResponse
		switch calls.Add(1) {
		case 1:
			if request.Op != "detach" {
				t.Errorf("first cleanup = %q", request.Op)
			}
			response.Close = &referenceCloseResult{Released: false, Determined: true}
			response.Error = &ErrorResponse{Errno: "EBUSY", Message: "unsealed import stays owned"}
		case 2:
			if request.Op != "cancel" {
				t.Errorf("owned cleanup = %q", request.Op)
			}
			response.Close = &referenceCloseResult{Released: true, Determined: true}
		default:
			t.Error("settled cancellation repeated native cleanup")
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64)}
	if result, err := file.Detach(t.Context()); result.Released || !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unsealed detach = %+v, %v", result, err)
	}
	if file.detached || file.closed != nil {
		t.Fatal("failed detach discarded responsibility")
	}
	for range 2 {
		if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
			t.Fatalf("cancel after failed detach = %+v, %v", result, err)
		}
	}
}

func TestTransferClientSettlementMarkersKeepCleanupResponsibility(t *testing.T) {
	for _, op := range []string{"cancel", "detach", "close-export"} {
		t.Run(op, func(t *testing.T) {
			var calls atomic.Int32
			client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
				var request transferRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request.Op != op {
					t.Errorf("cleanup operation = %q, want %q", request.Op, op)
				}
				w.Header().Set(HeaderProtocol, Version)
				response := transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}}
				if calls.Add(1) == 1 {
					response.Error = &ErrorResponse{Errno: "EIO", Message: "settlement is unknown"}
					response.Settlement = &transferCloseSettlement{State: storage.CloseSettlementUnknown,
						Semantic: &ErrorResponse{Errno: "ENOTEMPTY", Message: "native semantic failure"},
						Cause:    &ErrorResponse{Errno: "EIO", Message: "lower settlement failure"}}
				}
				_, _ = w.Write(transferClientJSON(t, response))
			})
			imported := &remoteImport{storage: client, token: strings.Repeat("a", 64)}
			exported := &remoteExport{storage: client, token: strings.Repeat("a", 64)}
			close := imported.Cancel
			if op == "detach" {
				close = imported.Detach
			} else if op == "close-export" {
				close = exported.CloseWithResult
			}
			result, err := close(t.Context())
			var marker *storage.CloseSettlementError
			if !result.Released || !result.Determined || !errors.As(err, &marker) || marker.State != storage.CloseSettlementUnknown || !errors.Is(marker.SemanticErr, syscall.ENOTEMPTY) || !errors.Is(marker.Cause, syscall.EIO) {
				t.Fatalf("settlement marker lost: %+v, %v", result, err)
			}
			if imported.closed != nil || imported.detached || exported.closed != nil {
				t.Fatal("released unsettled cleanup cached as settled")
			}
			result, err = close(t.Context())
			if err != nil || !result.Released || !result.Determined || calls.Load() != 2 {
				t.Fatalf("lower settlement was not retried: %+v, %v, calls %d", result, err, calls.Load())
			}
		})
	}
}

func TestTransferClientPublicationSettlementMarkerDoesNotCacheCleanup(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set(HeaderProtocol, Version)
		response := transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}}
		if calls.Add(1) == 1 {
			if request.Op != "publish" {
				t.Errorf("publication = %q", request.Op)
			}
			response.Receipt = transferReceiptOf(receipt)
			response.Error = &ErrorResponse{Errno: "EIO", Message: "lower settlement pending"}
			response.Settlement = &transferCloseSettlement{State: storage.CloseSettlementPending, Cause: &ErrorResponse{Errno: "EIO", Message: "lower settlement pending"}}
		} else if request.Op != "cancel" {
			t.Errorf("publication cleanup = %q", request.Op)
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command}
	result, err := file.Publish(t.Context())
	var marker *storage.CloseSettlementError
	if result.Action != command.Action || !errors.As(err, &marker) || marker.State != storage.CloseSettlementPending {
		t.Fatalf("publication marker lost: %+v, %v", result, err)
	}
	if file.closed != nil || file.published != nil {
		t.Fatal("unsettled publication released local cleanup responsibility")
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined || calls.Load() != 2 {
		t.Fatalf("publication cleanup = %+v, %v, calls %d", result, err, calls.Load())
	}
}

func TestTransferClientRejectsMalformedSettlementMarkers(t *testing.T) {
	for name, mutate := range map[string]func(*transferResponse){
		"no release":       func(r *transferResponse) { r.Close.Released = false },
		"no error":         func(r *transferResponse) { r.Error = nil },
		"invalid state":    func(r *transferResponse) { r.Settlement.State = 0 },
		"missing cause":    func(r *transferResponse) { r.Settlement.Cause = nil },
		"unknown cause":    func(r *transferResponse) { r.Settlement.Cause.Errno = "UNKNOWN" },
		"unknown semantic": func(r *transferResponse) { r.Settlement.Semantic = &ErrorResponse{Errno: "UNKNOWN"} },
	} {
		t.Run(name, func(t *testing.T) {
			response := transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}, Error: &ErrorResponse{Errno: "EIO", Message: "settlement pending"}, Settlement: &transferCloseSettlement{State: storage.CloseSettlementPending, Cause: &ErrorResponse{Errno: "EIO", Message: "settlement pending"}}}
			mutate(&response)
			if _, err := decodeTransferResponse(transferClientJSON(t, response), "detach"); err == nil {
				t.Fatal("malformed settlement marker accepted")
			}
		})
	}
}

func TestTransferClientControlsRemainAvailableDuringDataAdmission(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/file-transfer-control") {
			t.Errorf("control used data endpoint %q", r.URL.Path)
		}
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		var response transferResponse
		switch request.Op {
		case "check":
			response.Capabilities = &transferCapabilities{MaxBytes: 1024}
		case "resume":
			response.Token = strings.Repeat("a", 64)
		case "query", "cancel-action", "publish":
			response.Receipt = transferReceiptOf(storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionPending})
		case "cancel", "detach", "close-export":
			response.Close = &referenceCloseResult{Released: true, Determined: true}
		default:
			t.Errorf("data request passed occupied admission: %q", request.Op)
		}
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, response))
	})
	client.responses = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 0)
	release, err := client.responses.acquire(t.Context(), retainedResponseMultiplier*client.maxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, request := range []transferRequest{
		{Op: "check"}, {Op: "resume", Action: &command.Action}, {Op: "query", Action: &command.Action},
		{Op: "cancel-action", Action: &command.Action},
		{Op: "publish", Token: strings.Repeat("a", 64)}, {Op: "cancel", Token: strings.Repeat("a", 64)},
		{Op: "detach", Token: strings.Repeat("a", 64)}, {Op: "close-export", Token: strings.Repeat("a", 64)},
	} {
		if _, err := client.transferCall(t.Context(), request); err != nil {
			t.Fatalf("reserved control %s was blocked by data admission: %v", request.Op, err)
		}
	}
	for _, request := range []transferRequest{{Op: "begin", Command: &command}, {Op: "export", Export: &storage.ExportCommand{NodeID: 2}}} {
		if _, err := client.transferCall(t.Context(), request); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("data admission %s bypassed saturation: %v", request.Op, err)
		}
	}
}

func TestTransferClientQueryCannotClearCleanupSettlement(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}}
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		response := transferResponse{}
		switch calls.Add(1) {
		case 1:
			if request.Op != "publish" {
				t.Errorf("first operation = %q", request.Op)
			}
			response.Receipt = transferReceiptOf(receipt)
			response.Close = &referenceCloseResult{Released: true, Determined: true}
			response.Error = &ErrorResponse{Errno: "EIO", Message: "cleanup pending"}
			response.Settlement = &transferCloseSettlement{State: storage.CloseSettlementPending, Cause: &ErrorResponse{Errno: "EIO", Message: "cleanup pending"}}
		case 2, 5:
			if request.Op != "query" {
				t.Errorf("receipt recovery = %q", request.Op)
			}
			response.Receipt = transferReceiptOf(receipt)
		case 3:
			if request.Op != "query" {
				t.Errorf("failed query operation = %q", request.Op)
			}
			w.Header().Set(HeaderProtocol, Version)
			w.WriteHeader(http.StatusBadGateway)
			return
		case 4:
			if request.Op != "cancel" {
				t.Errorf("cleanup recovery = %q", request.Op)
			}
			response.Close = &referenceCloseResult{Released: true, Determined: true}
		default:
			t.Error("settled cached result repeated transport work")
		}
		w.Header().Set(HeaderProtocol, Version)
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64), action: command.Action, command: &command}
	for range 2 {
		result, err := file.Publish(t.Context())
		var marker *storage.CloseSettlementError
		if result.Action != command.Action || !errors.As(err, &marker) || marker.State != storage.CloseSettlementPending {
			t.Fatalf("durable query cleared volatile settlement: %+v, %v", result, err)
		}
	}
	if result, err := file.Publish(t.Context()); result.Action != (storage.ActionKey{}) || err == nil {
		t.Fatalf("failed durable query = %+v, %v", result, err)
	} else {
		var marker *storage.CloseSettlementError
		if !errors.As(err, &marker) || marker.State != storage.CloseSettlementPending {
			t.Fatalf("failed query discarded prior settlement: %v", err)
		}
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("exact cleanup = %+v, %v", result, err)
	}
	for range 2 {
		if result, err := file.Publish(t.Context()); err != nil || result.Action != command.Action {
			t.Fatalf("settled publication = %+v, %v", result, err)
		}
	}
}

func TestTransferClientInconclusiveClosePreservesReleaseAndOwnership(t *testing.T) {
	var calls atomic.Int32
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
		var request transferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Op != "cancel" {
			t.Errorf("cleanup operation = %q", request.Op)
		}
		w.Header().Set(HeaderProtocol, Version)
		response := transferResponse{Close: &referenceCloseResult{Released: true, Determined: true}}
		switch calls.Add(1) {
		case 1:
			response.Error = &ErrorResponse{Errno: "EIO", Message: "settlement pending"}
			response.Settlement = &transferCloseSettlement{State: storage.CloseSettlementPending, Semantic: &ErrorResponse{Errno: "ENOTEMPTY", Message: "native outcome"}, Cause: &ErrorResponse{Errno: "EIO", Message: "settlement pending"}}
		case 2:
			w.WriteHeader(http.StatusBadGateway)
			return
		case 3:
		default:
			t.Error("settled close repeated its retired token")
		}
		_, _ = w.Write(transferClientJSON(t, response))
	})
	file := &remoteImport{storage: client, token: strings.Repeat("a", 64)}
	if result, err := file.Cancel(t.Context()); !result.Released || !result.Determined || err == nil {
		t.Fatalf("pending close = %+v, %v", result, err)
	}
	result, err := file.Cancel(t.Context())
	var marker *storage.CloseSettlementError
	if !result.Released || !result.Determined || !errors.As(err, &marker) || marker.State != storage.CloseSettlementUnknown || !errors.Is(marker.SemanticErr, syscall.ENOTEMPTY) {
		t.Fatalf("inconclusive retry lost released facts: %+v, %v", result, err)
	}
	if file.closed != nil {
		t.Fatal("inconclusive retry cached settlement")
	}
	for range 2 {
		if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
			t.Fatalf("confirmed settlement = %+v, %v", result, err)
		}
	}
	if file.cleanupErr != nil || file.released != nil {
		t.Fatal("confirmed settlement retained old ownership marker")
	}
}

func TestTransferClientCancelsDurableActionWithoutVolatileCapability(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	for _, test := range []struct {
		name      string
		outcome   storage.DurableActionOutcome
		failure   syscall.Errno
		wireError *ErrorResponse
		want      error
	}{
		{name: "canceled", outcome: storage.DurableActionNotExecuted, failure: syscall.ECANCELED},
		{name: "canceled errno", outcome: storage.DurableActionNotExecuted, failure: syscall.ECANCELED, wireError: &ErrorResponse{Errno: "ECANCELED", Message: "action canceled"}, want: syscall.ECANCELED},
		{name: "completed publication", outcome: storage.DurableActionCompleted},
		{name: "live receive", outcome: storage.DurableActionPending, wireError: &ErrorResponse{Errno: "EBUSY", Message: "receive is live"}, want: syscall.EBUSY},
		{name: "unknown cleanup", outcome: storage.DurableActionUnknown, wireError: &ErrorResponse{Errno: "EIO", Message: "cleanup remains owned"}, want: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: test.outcome, Failure: test.failure}
			if test.outcome == storage.DurableActionCompleted {
				receipt.Attr = storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: 3}
			}
			client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/file-transfer-control") || r.Method != http.MethodPost {
					t.Errorf("durable cancellation used wrong endpoint: %s %s", r.Method, r.URL.Path)
				}
				content, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var request transferRequest
				if err := decodeFileJSON(content, &request); err != nil {
					t.Error(err)
					return
				}
				if request.Op != "cancel-action" || request.Action == nil || *request.Action != command.Action || request.Token != "" || request.Command != nil || request.Export != nil {
					t.Errorf("durable cancellation changed its action: %+v", request)
				}
				if strings.Contains(string(content), "replacement-access") {
					t.Error("durable cancellation serialized credential identity")
				}
				w.Header().Set(HeaderProtocol, Version)
				_, _ = w.Write(transferClientJSON(t, transferResponse{Receipt: transferReceiptOf(receipt), Error: test.wireError}))
			})
			ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: command.Action.SubjectID, AccessID: "replacement-access"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.CancelImport(ctx, command.Action)
			if result.Action != command.Action || result.Fingerprint != command.Fingerprint || result.Outcome != test.outcome || result.Failure != test.failure || !errors.Is(err, test.want) {
				t.Fatalf("durable cancellation lost its native receipt: %+v, %v", result, err)
			}
			if test.outcome == storage.DurableActionCompleted && result.Attr.ID != 2 {
				t.Fatal("durable cancellation discarded completed publication")
			}
		})
	}
}

func TestTransferClientDurableCancellationRejectsUnrelatedAndUnknownReceipts(t *testing.T) {
	command := transferClientCommand([]byte("abc"))
	for name, response := range map[string]transferResponse{
		"unrelated action":        {Receipt: transferReceiptOf(storage.ImportReceipt{Action: storage.ActionKey{SubjectID: "other", Namespace: 1, Nonce: "import"}, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionNotExecuted})},
		"unknown without error":   {Receipt: transferReceiptOf(storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionUnknown})},
		"volatile cleanup result": {Receipt: transferReceiptOf(storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionNotExecuted}), Close: &referenceCloseResult{Released: true, Determined: true}},
	} {
		t.Run(name, func(t *testing.T) {
			client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(HeaderProtocol, Version)
				_, _ = w.Write(transferClientJSON(t, response))
			})
			if result, err := client.CancelImport(t.Context(), command.Action); err == nil || result.Action != (storage.ActionKey{}) {
				t.Fatalf("invalid cancellation receipt accepted: %+v, %v", result, err)
			}
		})
	}
	client := transferClientDial(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid action reached remote cancellation") })
	if _, err := client.CancelImport(t.Context(), storage.ActionKey{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid durable action = %v", err)
	}
}
