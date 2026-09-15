package sqlite

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsOverwritePublishesRequestedAttributesWithContent(t *testing.T) {
	for _, disposition := range []storage.WindowsDisposition{storage.WindowsOverwrite, storage.WindowsOverwriteIf, storage.WindowsSupersede} {
		t.Run(fmt.Sprint(disposition), func(t *testing.T) {
			s, ws := windowsAuthority(t)
			f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
			windowsPublish(t, f, 0, 7)
			request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: disposition}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "file"}, DOSAttributes: storage.WindowsDOSReadOnly}
			result, err := ws.Open(t.Context(), request, windowsActionID(t, ws))
			if err != nil || result.Attr.Size != 0 || result.Attr.DOSAttributes != storage.WindowsDOSReadOnly|storage.WindowsDOSArchive {
				t.Fatalf("overwrite=%+v %v", result, err)
			}
			request.Disposition = storage.WindowsOpen
			request.DOSAttributes = 0
			if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("freshwriteopen=%v", err)
			}
		})
	}
}

func TestWindowsOverwriteRequiresExistingHiddenAndSystemFlags(t *testing.T) {
	for _, attributes := range []uint32{storage.WindowsDOSHidden, storage.WindowsDOSSystem} {
		for _, disposition := range []storage.WindowsDisposition{storage.WindowsOverwrite, storage.WindowsOverwriteIf, storage.WindowsSupersede} {
			t.Run(fmt.Sprintf("%d/%d", attributes, disposition), func(t *testing.T) {
				s, ws := windowsAuthority(t)
				f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
				windowsPublish(t, f, 0, 7)
				if _, err := f.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &attributes}, windowsActionID(t, ws)); err != nil {
					t.Fatal(err)
				}
				request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: disposition}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "file"}}
				if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EACCES) {
					t.Fatalf("omittedflags=%v", err)
				}
				if attr, err := f.Stat(t.Context()); err != nil || attr.Size != 7 || attr.DOSAttributes != attributes {
					t.Fatalf("refusedstate=%+v %v", attr, err)
				}
				request.DOSAttributes = attributes
				if result, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); err != nil || result.Attr.DOSAttributes != attributes|storage.WindowsDOSArchive {
					t.Fatalf("matchedflags=%+v %v", result, err)
				}
			})
		}
	}
}
