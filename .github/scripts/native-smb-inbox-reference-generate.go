//go:build ignore

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type selection struct{ Origin, Path, SHA256, Portable, Windows string }
type declarationReceipt struct{ Name, SHA256, Destination string }
type sourceReceipt struct {
	Origin, Path, SHA256 string
	Declarations         []declarationReceipt
}
type generationReceipt struct {
	Policy                generationPolicy
	NativeEntry           string
	Transformations       []transformationReceipt
	ReusedBodiesRewritten bool
	InverseASTVerified    bool
	Prototype             string
	Sources               []sourceReceipt
	Outputs               map[string]string
	BodiesRewritten       bool
}

var selections = []selection{
	{"prototype", "packages/smb/windows/native_acceptance_windows_test.go", "e842bc33ef668a0d103a8a7d4b9d5fa3f3a98d4721d95fdb32f4dc63d100b6f6", "", "nativeHandle nativeHandle.close"},
	{"prototype", "packages/smb/windows/mapping_windows.go", "d0dac41d96da796d192fd50921f07971f7e31abe31353be1a99260e200360917", "", "windowsMappings tokenStatistics tokenIdentity windowsMappings.identity"},
	{"prototype", "packages/smb/windows/mapping.go", "debec0a5bb7f75470e0f5c84110c0391989b8c56869a8756d9d5f7802f7bd265", "logonIdentity ErrMappingOwnership", ""},
	{"prototype", "packages/smb/windows/auth.go", "c82c4332e7eec606edfec4e21ae04fec4a326e8ce6a148b6574ed845f73330e9", "ErrInvalidSID", ""},
	{"checkout", ".github/scripts/native-smb-positive-cache_test.go.txt", "4f2395a7153dbdd8bce8cb1b4327bebf6fbd20a05a3861ff434a97d27ff5622b", "cacheGatePositiveValue cacheGatePositiveDigest cacheGatePositiveError cacheGatePositivePayload", "cacheGatePositiveTime cacheGatePositivePath cacheGatePositiveIdentity cacheGatePositiveOpenSync cacheGatePositiveReadSync cacheGatePositiveSeek"},
	{"checkout", ".github/scripts/native-smb-parent-invalidation_test.go.txt", "7f70553d74ac9811b9680deb0b36ab5b63787889911735c76ed53c7c13ef0395", "parentNotifyFilter parentNotifyLimit parentNotifyItem parentNotifyDecode parentNativeCompletion parentErrorNotifyEnum parentCleanupDrained parentRelativeIdentity parentWithinWindow", "parentNativeWatch parentWatchStart parentNativeWatch.complete parentNativeWatch.completeNative parentNativeWatch.rearm"},
	{"checkout", ".github/scripts/native-smb-cache-gate_test.go.txt", "fecf5000fcb8545713b34e43c36768beb1b89a9dd40c9f2e85cc89d216083db0", "cacheGateEvent cacheGateTrace cacheGateActive cacheGateObserve", "cacheGateSharingOpen"},
}

const portableHeader = `package windows
import (
 "crypto/sha256"
 "encoding/binary"
 "encoding/hex"
 "encoding/json"
 "errors"
 "sync"
 "sync/atomic"
 "syscall"
 "time"
 "unicode/utf16"
)
`
const windowsHeader = `//go:build windows

package windows
import (
 "context"
 "errors"
 "fmt"
 "runtime"
 "syscall"
 "testing"
 "time"
 "unsafe"
 win "golang.org/x/sys/windows"
)
`

func main() {
	root := flag.String("root", "", "exact checked-out source root")
	prototype := flag.String("prototype", "", "read-only pinned utility checkout")
	output := flag.String("output", "", "new empty fixture directory under the task temporary directory")
	policy := flag.String("application-sharing", "original", "original or write-only application directory sharing")
	flag.Parse()
	if flag.NArg() != 0 {
		fail(errors.New("unexpected positional arguments"))
	}
	if err := generatePolicy(*root, *prototype, *output, *policy); err != nil {
		fail(err)
	}
}
func fail(err error)            { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func digest(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func canonical(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return nil, fmt.Errorf("source is not a bounded regular file: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n")), nil
}
func contained(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
func noLinks(path string) error {
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	current := volume + string(filepath.Separator)
	for _, part := range strings.Split(rest, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in fixture ownership path: %s", current)
		}
	}
	return nil
}

func generate(root, prototype, output string) error {
	return generatePolicy(root, prototype, output, "original")
}
func generatePolicy(root, prototype, output, sharing string) error {
	selected, err := generationSelection(sharing)
	if err != nil {
		return err
	}
	if root == "" || prototype == "" || output == "" {
		return errors.New("root, prototype and output are required")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	prototype, err = filepath.Abs(prototype)
	if err != nil {
		return err
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return err
	}
	task := filepath.Join(root, ".tmp", selected.Root)
	if !contained(filepath.Join(root, ".tmp"), prototype) || !contained(task, output) {
		return errors.New("prototype or output is outside the owned temporary trees")
	}
	for _, path := range []string{root, prototype, output} {
		if err := noLinks(path); err != nil {
			return err
		}
	}
	if entries, err := os.ReadDir(output); err == nil {
		if len(entries) != 0 {
			return errors.New("fixture output is not empty")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	var portable, windows bytes.Buffer
	portable.WriteString(portableHeader)
	windows.WriteString(windowsHeader)
	receipt := generationReceipt{Policy: selected.Policy, NativeEntry: selected.Entry, BodiesRewritten: true, InverseASTVerified: true, Prototype: "1cb9ad7f49d998de4daa4d562d766b18cf06ce16", Outputs: map[string]string{}}
	for _, item := range selections {
		origin := root
		if item.Origin == "prototype" {
			origin = prototype
		}
		data, err := canonical(filepath.Join(origin, filepath.FromSlash(item.Path)))
		if err != nil {
			return err
		}
		if digest(data) != item.SHA256 {
			return fmt.Errorf("immutable helper source differs: %s", item.Path)
		}
		result := sourceReceipt{Origin: item.Origin, Path: item.Path, SHA256: item.SHA256}
		for _, group := range []struct {
			names, destination string
			out                *bytes.Buffer
		}{{item.Portable, "reuse_portable_test.go", &portable}, {item.Windows, "reuse_windows_test.go", &windows}} {
			if group.names == "" {
				continue
			}
			records, err := extract(data, item.Path, strings.Fields(group.names), group.destination, group.out)
			if err != nil {
				return err
			}
			result.Declarations = append(result.Declarations, records...)
		}
		receipt.Sources = append(receipt.Sources, result)
	}
	outputs := map[string][]byte{}
	for name, data := range map[string][]byte{"reuse_portable_test.go": portable.Bytes(), "reuse_windows_test.go": windows.Bytes()} {
		formatted, err := format.Source(data)
		if err != nil {
			return fmt.Errorf("format %s: %w", name, err)
		}
		outputs[name] = formatted
	}
	for _, item := range []struct{ source, destination string }{{"native-smb-inbox-reference_test.go.txt", "reference_windows_test.go"}, {"native-smb-inbox-reference-controls_test.go.txt", "reference_controls_test.go"}, {"native-smb-inbox-sharing-controls_test.go.txt", "sharing_controls_test.go"}} {
		path := filepath.Join(".github", "scripts", item.source)
		data, err := canonical(filepath.Join(root, path))
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ParseComments)
		if err != nil {
			return err
		}
		if parsed.Name.Name != "windows" {
			return errors.New("reference template package differs")
		}
		if item.destination == "reference_windows_test.go" && !bytes.HasPrefix(data, []byte("//go:build windows\n")) {
			return errors.New("native reference lacks its Windows build constraint")
		}
		if item.destination != "reference_windows_test.go" && bytes.Contains(data, []byte("//go:build")) {
			return errors.New("portable controls cannot be platform excluded")
		}
		original := data
		if item.destination != "sharing_controls_test.go" {
			var changes []transformationReceipt
			data, changes, err = transformReference(data, item.destination, selected)
			if err != nil {
				return err
			}
			receipt.Transformations = append(receipt.Transformations, changes...)
		}
		formatted, err := format.Source(data)
		if err != nil {
			return err
		}
		outputs[item.destination] = formatted
		receipt.Sources = append(receipt.Sources, sourceReceipt{Origin: "checkout", Path: filepath.ToSlash(path), SHA256: digest(original)})
	}
	outputs["sharing_policy_test.go"] = []byte(fmt.Sprintf("package windows\n\nconst inboxCompiledSharing = %q\n", sharing))
	outputs["go.mod"] = []byte("module github.com/codetreker/remote-fs/inbox-reference\n\ngo 1.26.0\n\nrequire golang.org/x/sys v0.47.0\n")
	outputs["go.sum"] = []byte("golang.org/x/sys v0.47.0 h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs=\ngolang.org/x/sys v0.47.0/go.mod h1:4GL1E5IUh+htKOUEOaiffhrAeqysfVGipDYzABqnCmw=\n")
	for name, data := range outputs {
		receipt.Outputs[name] = digest(data)
	}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	outputs["reuse-manifest.json"] = append(encoded, '\n')
	if err := os.MkdirAll(output, 0700); err != nil {
		return err
	}
	names := make([]string, 0, len(outputs))
	for name := range outputs {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		file, err := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(outputs[name])
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return err
		}
	}
	fmt.Printf("Generated standalone inbox fixture: %s\n", output)
	return nil
}

func extract(data []byte, path string, names []string, destination string, out *bytes.Buffer) ([]declarationReceipt, error) {
	fs := token.NewFileSet()
	tree, err := parser.ParseFile(fs, path, data, 0)
	if err != nil {
		return nil, err
	}
	wanted := map[string]int{}
	for _, name := range names {
		if _, ok := wanted[name]; ok {
			return nil, fmt.Errorf("duplicate selector: %s", name)
		}
		wanted[name] = 0
	}
	var receipts []declarationReceipt
	emit := func(name string, start, end token.Pos, prefix string) error {
		count, ok := wanted[name]
		if !ok {
			return nil
		}
		if count != 0 {
			return fmt.Errorf("helper declaration is ambiguous: %s", name)
		}
		wanted[name]++
		body := data[fs.Position(start).Offset:fs.Position(end).Offset]
		out.WriteString(prefix)
		out.Write(body)
		out.WriteString("\n\n")
		receipts = append(receipts, declarationReceipt{Name: name, SHA256: digest(body), Destination: destination})
		return nil
	}
	for _, decl := range tree.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			name := d.Name.Name
			if d.Recv != nil {
				if len(d.Recv.List) != 1 {
					return nil, errors.New("unexpected helper receiver")
				}
				typ := d.Recv.List[0].Type
				if star, ok := typ.(*ast.StarExpr); ok {
					typ = star.X
				}
				id, ok := typ.(*ast.Ident)
				if !ok {
					return nil, errors.New("unsupported helper receiver")
				}
				name = id.Name + "." + name
			}
			if err := emit(name, d.Pos(), d.End(), ""); err != nil {
				return nil, err
			}
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			for _, spec := range d.Specs {
				switch x := spec.(type) {
				case *ast.TypeSpec:
					if err := emit(x.Name.Name, x.Pos(), x.End(), d.Tok.String()+" "); err != nil {
						return nil, err
					}
				case *ast.ValueSpec:
					selected := ""
					for _, id := range x.Names {
						if _, ok := wanted[id.Name]; ok {
							if selected != "" {
								return nil, errors.New("multiple selected names in one helper value")
							}
							selected = id.Name
						}
					}
					if selected != "" {
						if len(x.Names) != 1 || len(x.Values) > 1 || len(x.Values) == 0 && (d.Tok != token.VAR || x.Type == nil) {
							return nil, errors.New("helper value depends on implicit or grouped initialization")
						}
						if err := emit(selected, x.Pos(), x.End(), d.Tok.String()+" "); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	for name, count := range wanted {
		if count != 1 {
			return nil, fmt.Errorf("required helper declaration absent: %s", name)
		}
	}
	return receipts, nil
}

type generationPolicy struct {
	ApplicationSharing string `json:"application_sharing"`
	Cell               string `json:"cell"`
	ApplicationAccess  uint32 `json:"application_access"`
	ApplicationShare   uint32 `json:"application_share"`
}
type generationChoice struct {
	Policy             generationPolicy
	Root, Entry, Share string
}

func generationSelection(name string) (generationChoice, error) {
	switch name {
	case "original":
		return generationChoice{generationPolicy{"original", "inbox-reference", 1, 0}, "native-inbox-reference", "TestNativeInboxReference", "0"}, nil
	case "write-only":
		return generationChoice{generationPolicy{"write-only", "inbox-share-write", 1, 2}, "native-inbox-share-write", "TestNativeInboxShareWriteControl", "win.FILE_SHARE_WRITE"}, nil
	default:
		return generationChoice{}, errors.New("unknown application sharing policy")
	}
}

type transformationReceipt struct {
	Source, Declaration, Action                           string
	BeforeSHA256, AfterSHA256                             string
	OriginalDeclarationSHA256, GeneratedDeclarationSHA256 string
}
type referenceEdit struct {
	start, end                 int
	before, after, decl, label string
}

func astText(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}
func referenceTree(data []byte) (*ast.File, *token.FileSet, error) {
	fs := token.NewFileSet()
	tree, err := parser.ParseFile(fs, "reference.go", data, parser.ParseComments)
	return tree, fs, err
}
func transformReference(data []byte, path string, choice generationChoice) ([]byte, []transformationReceipt, error) {
	expected := "41f003a9fd9db393bdcea0d8a26511c22b038da1208eb3b0019aad87bfeca35f"
	native := path == "reference_windows_test.go"
	if native {
		expected = "a96de8aadbd8d3ba6013fa3db3669a5345ab3467a285331b1d487b898e5a8b63"
	}
	if digest(data) != expected {
		return nil, nil, fmt.Errorf("frozen reference template differs: %s", path)
	}
	tree, fs, err := referenceTree(data)
	if err != nil {
		return nil, nil, err
	}
	funcs := map[string]*ast.FuncDecl{}
	types := map[string]*ast.TypeSpec{}
	for _, decl := range tree.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				funcs[d.Name.Name] = d
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if typ, ok := spec.(*ast.TypeSpec); ok {
					types[typ.Name.Name] = typ
				}
			}
		}
	}
	var edits []referenceEdit
	add := func(decl, label string, start, end token.Pos, after string) {
		a, b := fs.Position(start).Offset, fs.Position(end).Offset
		edits = append(edits, referenceEdit{a, b, string(data[a:b]), after, decl, label})
	}
	statement := func(name, want string) (ast.Stmt, error) {
		anchor, _, parseErr := referenceTree([]byte("package p\nfunc anchor(){" + want + "\n}"))
		if parseErr != nil {
			return nil, parseErr
		}
		want = astText(anchor.Decls[0].(*ast.FuncDecl).Body.List[0])
		fn := funcs[name]
		if fn == nil {
			return nil, fmt.Errorf("missing function %s", name)
		}
		var found ast.Stmt
		count := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if stmt, ok := node.(ast.Stmt); ok && astText(stmt) == want {
				found = stmt
				count++
			}
			return true
		})
		if count != 1 {
			return nil, fmt.Errorf("statement anchor %s: found %d for %q", name, count, want)
		}
		return found, nil
	}
	after := func(name, want, label, code string) error {
		stmt, err := statement(name, want)
		if err != nil {
			return err
		}
		add(name, label, stmt.End(), stmt.End(), "\n"+code)
		return nil
	}
	field := func(name, code string) error {
		typ := types[name]
		if typ == nil {
			return fmt.Errorf("missing type %s", name)
		}
		body, ok := typ.Type.(*ast.StructType)
		if !ok {
			return fmt.Errorf("not a struct: %s", name)
		}
		add(name, "policy/evidence fields", body.Fields.Closing, body.Fields.Closing, "\n"+code+"\n")
		return nil
	}
	composite := func(fnName, typeName, code string) error {
		fn := funcs[fnName]
		if fn == nil {
			return fmt.Errorf("missing function %s", fnName)
		}
		var found *ast.CompositeLit
		count := 0
		ast.Inspect(fn, func(node ast.Node) bool {
			if lit, ok := node.(*ast.CompositeLit); ok && astText(lit.Type) == typeName {
				found = lit
				count++
			}
			return true
		})
		if count != 1 {
			return fmt.Errorf("composite anchor %s/%s: %d", fnName, typeName, count)
		}
		add(fnName, "policy/evidence initialization", found.Lbrace+1, found.Lbrace+1, code)
		return nil
	}
	guard := func(name, code string) error {
		fn := funcs[name]
		if fn == nil {
			return fmt.Errorf("missing function %s", name)
		}
		add(name, "policy admission", fn.Body.Lbrace+1, fn.Body.Lbrace+1, "\n"+code+"\n")
		return nil
	}
	if native {
		if err = guard("TestNativeInboxReference", `if err := inboxRequireSelectedPolicy(os.Getenv("RFS_INBOX_APPLICATION_SHARING")); err != nil { t.Fatal(err) }`); err != nil {
			return nil, nil, err
		}
		if err = composite("TestNativeInboxReference", "inboxReceipt", `Policy: s.Policy, SourceOpenResult: inboxReturnedErrorEvidence{State:"not_attempted"}, RenameResult: inboxReturnedErrorEvidence{State:"not_attempted"},`); err != nil {
			return nil, nil, err
		}
		if err = composite("inboxStage", "inboxEnvelope", `Policy:s.Policy,`); err != nil {
			return nil, nil, err
		}
		app, err := statement("inboxNativeCell", "app, err := cacheGateSharingOpen(t, s.ShareUNC+`\\v`, win.FILE_LIST_DIRECTORY, 0, true)")
		if err != nil {
			return nil, nil, err
		}
		assign := app.(*ast.AssignStmt)
		call := assign.Rhs[0].(*ast.CallExpr)
		if len(call.Args) != 5 || astText(call.Args[3]) != "0" {
			return nil, nil, errors.New("application share anchor differs")
		}
		if choice.Policy.ApplicationSharing == "write-only" {
			add("inboxNativeCell", "application ShareAccess 0 to FILE_SHARE_WRITE", call.Args[3].Pos(), call.Args[3].End(), choice.Share)
		}
		add("inboxNativeCell", "application actual returned call evidence", app.End(), app.End(), "\nr.ApplicationOpen = inboxApplicationResult(win.FILE_LIST_DIRECTORY, "+choice.Share+", err)")
		if err = after("inboxNativeCell", `inboxRecord(r, "mutator_open", start, err)`, "source-open returned errno", `r.SourceOpenResult = inboxReturnedError(err)`); err != nil {
			return nil, nil, err
		}
		if err = after("inboxNativeCell", `inboxRecord(r, "rename_flags3", start, renameErr)`, "rename returned errno before close", `r.RenameResult = inboxReturnedError(renameErr)`); err != nil {
			return nil, nil, err
		}
		stmt, err := statement("TestNativeInboxReference", `r.Outcome = inboxClassify(&r, a, b)`)
		if err != nil {
			return nil, nil, err
		}
		add("TestNativeInboxReference", "separate native and policy outcome", stmt.Pos(), stmt.End(), "r.NativeOutcome = inboxClassify(&r, a, b)\nr.Outcome = inboxPolicyOutcome(r.Policy, r.NativeOutcome)")
		comparison := `if envelope.Outcome != "native_behavior_pass" {\n\tt.Errorf("inbox reference: %s: %s; native=%s", envelope.Outcome, envelope.Error, r.NativeOracle)\n}`
		comparison = strings.ReplaceAll(comparison, `\n`, "\n")
		comparison = strings.ReplaceAll(comparison, `\t`, "\t")
		stmt, err = statement("TestNativeInboxReference", comparison)
		if err != nil {
			return nil, nil, err
		}
		branch := stmt.(*ast.IfStmt)
		condition := branch.Cond.(*ast.BinaryExpr)
		add("TestNativeInboxReference", "selected exact success", condition.Y.Pos(), condition.Y.End(), "inboxSelectedSuccess()")
		stmt, err = statement("TestNativeInboxReference", `envelope.Outcome = "receipt_failure"`)
		if err != nil {
			return nil, nil, err
		}
		add("TestNativeInboxReference", "selected receipt failure outcome", stmt.Pos(), stmt.End(), `envelope.Outcome = inboxPolicyOutcome(s.Policy, "receipt_failure")`)
		if choice.Entry != "TestNativeInboxReference" {
			fn := funcs["TestNativeInboxReference"]
			add(fn.Name.Name, "selected native entry", fn.Name.Pos(), fn.Name.End(), choice.Entry)
		}
	} else {
		for _, name := range []string{"inboxStart", "inboxAdmission", "inboxEnvelope"} {
			if err = field(name, "Policy inboxSharingPolicy `json:\"policy\"`"); err != nil {
				return nil, nil, err
			}
		}
		if err = field("inboxReceipt", "Policy inboxSharingPolicy\nApplicationOpen inboxApplicationOpen\nSourceOpenResult, RenameResult inboxReturnedErrorEvidence\nNativeOutcome string"); err != nil {
			return nil, nil, err
		}
		if err = guard("inboxValidateStart", `if err := inboxValidatePolicy(s.Policy, inboxCompiledPolicy()); err != nil { return err }`); err != nil {
			return nil, nil, err
		}
		if err = guard("inboxValidateAdmission", `if err := inboxValidatePolicy(s.Policy, inboxCompiledPolicy()); err != nil { return err }; if err := inboxValidatePolicy(a.Policy, s.Policy); err != nil { return err }`); err != nil {
			return nil, nil, err
		}
		if err = composite("inboxTestStart", "inboxStart", `Policy:inboxCompiledPolicy(),`); err != nil {
			return nil, nil, err
		}
		if err = composite("inboxTestAdmission", "inboxAdmission", `Policy:s.Policy,`); err != nil {
			return nil, nil, err
		}
	}
	return applyReferenceEdits(data, path, edits, choice)
}
func applyReferenceEdits(data []byte, path string, edits []referenceEdit, choice generationChoice) ([]byte, []transformationReceipt, error) {
	slices.SortFunc(edits, func(a, b referenceEdit) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		return a.end - b.end
	})
	var out bytes.Buffer
	cursor := 0
	var reverse []referenceEdit
	for _, edit := range edits {
		if edit.start < cursor || edit.end < edit.start || edit.end > len(data) || string(data[edit.start:edit.end]) != edit.before {
			return nil, nil, errors.New("overlapping or invalid reference edit")
		}
		out.Write(data[cursor:edit.start])
		start := out.Len()
		out.WriteString(edit.after)
		reverse = append(reverse, referenceEdit{start, out.Len(), edit.after, edit.before, edit.decl, edit.label})
		cursor = edit.end
	}
	out.Write(data[cursor:])
	generated := out.Bytes()
	restored := append([]byte(nil), generated...)
	for i := len(reverse) - 1; i >= 0; i-- {
		edit := reverse[i]
		if string(restored[edit.start:edit.end]) != edit.before {
			return nil, nil, errors.New("inverse reference edit differs")
		}
		restored = append(append(append([]byte(nil), restored[:edit.start]...), []byte(edit.after)...), restored[edit.end:]...)
	}
	originalTree, _, err := referenceTree(data)
	if err != nil {
		return nil, nil, err
	}
	inverseTree, _, err := referenceTree(restored)
	if err != nil {
		return nil, nil, err
	}
	if astText(originalTree) != astText(inverseTree) {
		return nil, nil, errors.New("inverse AST differs from frozen template")
	}
	generatedTree, _, err := referenceTree(generated)
	if err != nil {
		return nil, nil, err
	}
	formatted, err := format.Source(generated)
	if err != nil {
		return nil, nil, err
	}
	formattedTree, _, err := referenceTree(formatted)
	if err != nil {
		return nil, nil, err
	}
	if astText(generatedTree) != astText(formattedTree) {
		return nil, nil, errors.New("emitted AST differs from inverse-verified source")
	}
	declHash := func(tree *ast.File, name string) (string, error) {
		for _, decl := range tree.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.Name == name {
					return digest([]byte(astText(d))), nil
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok && typ.Name.Name == name {
						return digest([]byte(astText(typ))), nil
					}
				}
			}
		}
		return "", fmt.Errorf("missing transformed declaration %s", name)
	}
	var receipts []transformationReceipt
	for _, edit := range edits {
		before, err := declHash(originalTree, edit.decl)
		if err != nil {
			return nil, nil, err
		}
		name := edit.decl
		if name == "TestNativeInboxReference" {
			name = choice.Entry
		}
		after, err := declHash(generatedTree, name)
		if err != nil {
			return nil, nil, err
		}
		receipts = append(receipts, transformationReceipt{path, edit.decl, edit.label, digest([]byte(edit.before)), digest([]byte(edit.after)), before, after})
	}
	return formatted, receipts, nil
}
