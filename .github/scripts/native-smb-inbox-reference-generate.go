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
	Prototype       string
	Sources         []sourceReceipt
	Outputs         map[string]string
	BodiesRewritten bool
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
	flag.Parse()
	if flag.NArg() != 0 {
		fail(errors.New("unexpected positional arguments"))
	}
	if err := generate(*root, *prototype, *output); err != nil {
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
	if root == "" || prototype == "" || output == "" {
		return errors.New("root, prototype and output are required")
	}
	var err error
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
	task := filepath.Join(root, ".tmp", "native-inbox-reference")
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
	receipt := generationReceipt{Prototype: "1cb9ad7f49d998de4daa4d562d766b18cf06ce16", Outputs: map[string]string{}}
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
	for _, item := range []struct{ source, destination string }{{"native-smb-inbox-reference_test.go.txt", "reference_windows_test.go"}, {"native-smb-inbox-reference-controls_test.go.txt", "reference_controls_test.go"}} {
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
		if item.destination == "reference_controls_test.go" && bytes.Contains(data, []byte("//go:build")) {
			return errors.New("portable controls cannot be platform excluded")
		}
		formatted, err := format.Source(data)
		if err != nil {
			return err
		}
		outputs[item.destination] = formatted
		receipt.Sources = append(receipt.Sources, sourceReceipt{Origin: "checkout", Path: filepath.ToSlash(path), SHA256: digest(data)})
	}
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
