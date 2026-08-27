package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	sdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
)

// The namespaces these tests build are held in a real blob service and a real database.
// Nothing here stands in for either: the whole claim this package makes is about how those
// two behave together, and a substitute for one of them would be a test of the substitute.
//
// The blob service is Azurite, which speaks the Blob REST API. deployments/localhost brings
// it up; CI brings up the same image. RFS_AZURITE_URL moves it, and the account is the
// well-known development one, whose key is published in Microsoft's own documentation and
// is a credential for nothing.
const (
	defaultURL  = "http://127.0.0.1:10000/devstoreaccount1"
	accountName = "devstoreaccount1"
	accountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func serviceURL() string {
	if url := os.Getenv("RFS_AZURITE_URL"); url != "" {
		return url
	}
	return defaultURL
}

// namespaces counts the namespaces this run has built, so that each gets a key prefix of
// its own. The contract suite asks for a fresh, empty namespace per case, and a prefix is
// what makes one fresh without paying for a container each time.
var namespaces atomic.Int64

// parts is one namespace with its two halves still in reach. Some of what this package
// promises is only observable from underneath — that a swept object is really gone, that a
// read reports an object the store has lost — and checking it from above would be checking
// the claim against itself.
type parts struct {
	objects  *azblob.Objects
	meta     metastore.Store
	database string
	prefix   string
	*objectstore.Storage
}

// newStorage builds one empty namespace: its own database file, and its own prefix in a
// container shared by the whole run.
func newStorage(t *testing.T) storage.Storage {
	t.Helper()
	return newParts(t, 0)
}

func newStorageWithAllowance(t *testing.T, allowance int64) storage.Storage {
	t.Helper()
	return newParts(t, allowance)
}

func newParts(t *testing.T, allowance int64) parts {
	t.Helper()
	name := containerFor(t)
	prefix := fmt.Sprintf("n%d/", namespaces.Add(1))
	database := filepath.Join(t.TempDir(), "meta.db")

	objects, err := azblob.NewWithSharedKey(serviceURL()+"/"+name, accountName, accountKey, prefix)
	if err != nil {
		t.Fatalf("reaching the blob container: %v", err)
	}
	meta, err := sqlite.Open(t.Context(), database, "workspace", allowance, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})
	return parts{objects: objects, meta: meta, database: database, prefix: prefix, Storage: namespace}
}

// contentOf reports which object holds a file's bytes, asked of the tree directly.
func contentOf(t *testing.T, p parts, path string) metastore.Key {
	t.Helper()
	node, err := p.meta.Stat(t.Context(), path)
	if err != nil {
		t.Fatalf("asking the tree about %s: %v", path, err)
	}
	if node.Content == "" {
		t.Fatalf("%s holds no object, and this test needs one", path)
	}
	return node.Content
}

// containerFor makes the container these tests share, once. Creating a container is a
// deployment's act rather than a namespace's, which is why nothing in the package under
// test does it.
const sharedContainer = "remote-fs-test"

var prepared struct {
	once sync.Once
	err  error
}

func containerFor(t *testing.T) string {
	t.Helper()
	prepared.once.Do(func() {
		prepared.err = makeContainer(context.Background(), sharedContainer)
	})
	if prepared.err != nil {
		t.Fatalf("preparing the container these tests share: %v", prepared.err)
	}
	return sharedContainer
}

func makeContainer(ctx context.Context, name string) error {
	credential, err := sdk.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}
	client, err := container.NewClientWithSharedKeyCredential(serviceURL()+"/"+name, credential, nil)
	if err != nil {
		return err
	}
	if _, err := client.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return err
	}
	return nil
}

// TestContract is the whole obligation: a namespace in a blob container behaves the way
// every other namespace does.
func TestContract(t *testing.T) {
	storagetest.Run(t, newStorage)
}

// TestSpaceRefusesWithoutAnAllowance holds the one thing the contract suite checks only for
// consistency: a namespace in a blob container has no capacity of its own to report, so
// without an allowance it has no figures at all rather than invented ones.
func TestSpaceRefusesWithoutAnAllowance(t *testing.T) {
	namespace := newStorage(t)
	if _, err := namespace.Space(t.Context()); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("space answered %v, want ENOSYS: a blob container has no capacity to report", err)
	}
}

// TestSpaceCountsWhatTheNamespaceHolds checks that the count is the namespace's own content
// rather than anything about the machine underneath it, and that it follows every direction
// a size can move.
func TestSpaceCountsWhatTheNamespaceHolds(t *testing.T) {
	const allowance = 1 << 20
	namespace := newStorageWithAllowance(t, allowance)
	ctx := t.Context()

	space, err := namespace.Space(ctx)
	if err != nil {
		t.Fatalf("space: %v", err)
	}
	if space.Total != allowance || space.Used != 0 || space.Avail != allowance {
		t.Fatalf("an empty namespace reports %+v, want the whole allowance free", space)
	}

	if err := namespace.Write(ctx, "f", make([]byte, 1000)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if space, err = namespace.Space(ctx); err != nil || space.Used != 1000 {
		t.Fatalf("after writing 1000 bytes space is %+v (%v), want 1000 taken", space, err)
	}

	// Shrinking a file gives the bytes back, and replacing it does not charge for both
	// versions: the old object is no longer part of the namespace the moment the new one is.
	if err := namespace.Write(ctx, "f", make([]byte, 10)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if space, err = namespace.Space(ctx); err != nil || space.Used != 10 {
		t.Fatalf("after shrinking to 10 bytes space is %+v (%v), want 10 taken", space, err)
	}

	if err := namespace.Remove(ctx, "f"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if space, err = namespace.Space(ctx); err != nil || space.Used != 0 {
		t.Fatalf("after removing the file space is %+v (%v), want nothing taken", space, err)
	}
}

// TestWriteBeyondTheAllowanceIsRefused checks that the refusal lands on the write that
// caused it, and that it says the workspace is full rather than that the machine is.
func TestWriteBeyondTheAllowanceIsRefused(t *testing.T) {
	const allowance = 4096
	namespace := newStorageWithAllowance(t, allowance)
	ctx := t.Context()

	if err := namespace.Write(ctx, "f", make([]byte, allowance+1)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("writing past the allowance failed with %v, want EDQUOT", err)
	}
	// The refused write left nothing behind: neither a file nor a charge.
	if _, err := namespace.Stat(ctx, "f"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("after a refused write the file is %v, want ENOENT", err)
	}
	space, err := namespace.Space(ctx)
	if err != nil {
		t.Fatalf("space: %v", err)
	}
	if space.Used != 0 {
		t.Fatalf("a refused write charged %d bytes", space.Used)
	}
}

// TestSweepClearsABacklog checks the drain a caller runs when a namespace has accumulated
// more unreferenced objects than the mutations passing through it clear on their own.
//
// The backlog is made through the tree directly, because going through the namespace would
// let each removal clear the one before it and there would never be a backlog to find.
func TestSweepClearsABacklog(t *testing.T) {
	p := newParts(t, 0)
	ctx := t.Context()

	const files = 12
	keys := make([]metastore.Key, 0, files)
	for i := range files {
		name := fmt.Sprintf("f%d", i)
		if err := p.Write(ctx, name, []byte(name)); err != nil {
			t.Fatalf("write: %v", err)
		}
		keys = append(keys, contentOf(t, p, name))
	}
	// Every file is written before any is removed. Interleaving the two would let each
	// write clear the garbage the removal before it made, and there would be no backlog.
	for i := range files {
		if err := p.meta.Remove(ctx, fmt.Sprintf("f%d", i)); err != nil {
			t.Fatalf("removing f%d from the tree: %v", i, err)
		}
	}

	removed, err := p.Sweep(ctx, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != files {
		t.Fatalf("the sweep removed %d objects, want %d", removed, files)
	}
	for _, key := range keys {
		if _, err := p.objects.Get(ctx, string(key)); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("object %q survived the sweep: %v", key, err)
		}
	}

	// Nothing is left, and asking again says so rather than failing or looping.
	if removed, err = p.Sweep(ctx, 100); err != nil || removed != 0 {
		t.Fatalf("a second sweep removed %d objects (%v), want none", removed, err)
	}
}

// TestReadReportsAnObjectTheStoreHasLost is the case the whole error vocabulary exists for.
// The tree says the file is there and names the object holding it; the object store does
// not have that object. That is not "the file is not there", and answering ENOENT — or an
// empty file — is what R-ERR-2 forbids, because whatever runs on top acts on it.
func TestReadReportsAnObjectTheStoreHasLost(t *testing.T) {
	p := newParts(t, 0)
	ctx := t.Context()

	if err := p.Write(ctx, "f", []byte("bytes that are about to go missing")); err != nil {
		t.Fatalf("write: %v", err)
	}
	key := contentOf(t, p, "f")
	if err := p.objects.Delete(ctx, string(key)); err != nil {
		t.Fatalf("deleting the object behind the file: %v", err)
	}

	content, err := p.Read(ctx, "f")
	switch {
	case err == nil:
		t.Fatalf("reading a file whose object is gone answered %q", content)
	case errors.Is(err, syscall.ENOENT):
		t.Fatalf("reading a file whose object is gone answered ENOENT, which says the file is not there when the file is there: %v", err)
	case !errors.Is(err, syscall.EIO):
		t.Fatalf("reading a file whose object is gone failed with %v, want EIO", err)
	}

	// The file itself is still there, and still says how long it is. Nothing about the
	// missing object has been written back into the tree.
	if attr, err := p.Stat(ctx, "f"); err != nil || attr.Size == 0 {
		t.Fatalf("after the object went missing the file stats as %+v (%v), want it unchanged", attr, err)
	}
}

// TestReadReportsAnUnreachableObjectStore checks the other half of that distinction: a
// namespace whose tree answers and whose objects cannot be reached reports the failure it
// had rather than the absence it did not observe (R-ERR-6).
func TestReadReportsAnUnreachableObjectStore(t *testing.T) {
	p := newParts(t, 0)
	ctx := t.Context()
	if err := p.Write(ctx, "f", []byte("content")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// A second namespace over the same tree, whose objects are at an address nothing is
	// listening on. The tree answers; the bytes cannot be fetched.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	unreachable, err := azblob.NewWithSharedKey("http://"+address+"/devstoreaccount1/"+sharedContainer, accountName, accountKey, p.prefix)
	if err != nil {
		t.Fatalf("building a client for an address nothing is listening on: %v", err)
	}
	meta, err := sqlite.Open(ctx, p.database, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the tree again: %v", err)
	}
	namespace := objectstore.New(unreachable, meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the second namespace: %v", err)
		}
	})

	// The tree is reachable, so the file is found.
	if _, err := namespace.Stat(ctx, "f"); err != nil {
		t.Fatalf("stat through a namespace whose objects are unreachable: %v", err)
	}
	content, err := namespace.Read(ctx, "f")
	switch {
	case err == nil:
		t.Fatalf("reading through an unreachable object store answered %q", content)
	case errors.Is(err, syscall.ENOENT):
		t.Fatalf("an unreachable object store was reported as a file that is not there: %v", err)
	}
}
