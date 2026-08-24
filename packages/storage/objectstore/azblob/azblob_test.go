package azblob

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/codetreker/remote-fs/packages/storage"
)

// These tests run against Azurite, and they never skip: a run that cannot reach it fails,
// because a green run that proved nothing is the failure mode the checks exist to prevent.
//
// The address comes from RFS_AZURITE_URL, defaulting to the loopback address and the port
// Azurite's blob service listens on, with the account in the path as an emulator requires.
// The account name and key are Microsoft's published development pair, which is the same on
// every Azurite anywhere and is not a secret.
const (
	defaultURL  = "http://127.0.0.1:10000/devstoreaccount1"
	accountName = "devstoreaccount1"
	accountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func accountURL() string {
	if url := os.Getenv("RFS_AZURITE_URL"); url != "" {
		return url
	}
	return defaultURL
}

// Tests share one container and take a prefix of their own inside it, rather than each
// making a container. Two reasons: a prefix costs no request where a container costs one to
// make and one to remove, and separating by prefix is the mechanism production separates
// workspaces with, so every test exercises it rather than only the one test named after it.
var shared struct {
	once   sync.Once
	name   string
	client *container.Client
	err    error
}

func sharedContainer(t *testing.T) string {
	t.Helper()
	shared.once.Do(func() {
		shared.name = fmt.Sprintf("rfs-azblob-%d", time.Now().UnixNano())
		credential, err := container.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			shared.err = err
			return
		}
		client, err := container.NewClientWithSharedKeyCredential(
			accountURL()+"/"+shared.name, credential, nil)
		if err != nil {
			shared.err = err
			return
		}
		if _, err := client.Create(context.Background(), nil); err != nil {
			shared.err = err
			return
		}
		shared.client = client
	})
	if shared.err != nil {
		t.Fatalf("making the container the tests run in, at %s: %v", accountURL(), shared.err)
	}
	return accountURL() + "/" + shared.name
}

// TestMain only tears down. Setting up here instead would turn an unreachable Azurite into a
// run where no test reached a verdict, which reads differently from the failures the tests
// themselves report; leaving setup to the tests makes an absent Azurite fail every one.
func TestMain(m *testing.M) {
	code := m.Run()
	if shared.client != nil {
		if _, err := shared.client.Delete(context.Background(), nil); err != nil {
			fmt.Fprintf(os.Stderr, "removing the container %q the tests ran in: %v\n", shared.name, err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

// objectsUnder opens a store on a prefix no other test uses.
func objectsUnder(t *testing.T) *Objects {
	t.Helper()
	prefix := fmt.Sprintf("%s-%d/", t.Name(), time.Now().UnixNano())
	objects, err := NewWithSharedKey(sharedContainer(t), accountName, accountKey, prefix)
	if err != nil {
		t.Fatalf("opening the store under %q: %v", prefix, err)
	}
	return objects
}

func md5Of(content []byte) []byte {
	sum := md5.Sum(content)
	return sum[:]
}

func TestPutStoresWhatGetReturns(t *testing.T) {
	objects := objectsUnder(t)
	content := []byte("a file's bytes, stored under an opaque key")

	if _, err := objects.Put(context.Background(), "written-once", content); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := objects.Get(context.Background(), "written-once")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("Get returned %q, want %q", got, content)
	}
}

// The digest is the service's Content-MD5 for the bytes it stored. Comparing it against the
// content's own MD5 is what makes the test meaningful: a Put that returned the local sum
// would pass this too, which is why the round trip below matters more than this line — the
// service verifies the Content-MD5 the request carries, so a value that comes back at all
// is one the service checked against what reached it.
func TestPutReportsTheDigestTheServiceComputed(t *testing.T) {
	objects := objectsUnder(t)

	for _, size := range []int{0, 1, 4096} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i)
		}
		key := fmt.Sprintf("sized-%d", size)

		digest, err := objects.Put(context.Background(), key, content)
		if err != nil {
			t.Fatalf("Put of %d bytes: %v", size, err)
		}
		if digest == nil {
			t.Fatalf("Put of %d bytes reported no digest; the service returns Content-MD5 for a block blob", size)
		}
		if want := md5Of(content); string(digest) != string(want) {
			t.Errorf("Put of %d bytes reported digest %x, want %x", size, digest, want)
		}
	}
}

// A key is reserved once and written once, so a second Put is a reservation handed out
// twice. It must be refused, and it must not replace what is already there.
func TestPutRefusesAKeyThatHasBeenWritten(t *testing.T) {
	objects := objectsUnder(t)
	first := []byte("the object the metastore points at")

	if _, err := objects.Put(context.Background(), "taken", first); err != nil {
		t.Fatalf("the first Put: %v", err)
	}

	_, err := objects.Put(context.Background(), "taken", []byte("a second writer's bytes"))
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("the second Put returned %v, want EEXIST", err)
	}

	got, err := objects.Get(context.Background(), "taken")
	if err != nil {
		t.Fatalf("Get after the refused Put: %v", err)
	}
	if string(got) != string(first) {
		t.Errorf("the refused Put replaced the object: Get returned %q, want %q", got, first)
	}
}

func TestGetOfAKeyNobodyWroteIsAbsent(t *testing.T) {
	objects := objectsUnder(t)

	got, err := objects.Get(context.Background(), "never-written")
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Get returned %v, want ENOENT", err)
	}
	if got != nil {
		t.Errorf("Get returned %q alongside its error, want nil", got)
	}
}

// Deletion is driven by a sweeper that may be running again after being interrupted between
// removing the object and recording that it did, so a second Delete must converge.
func TestDeleteRemovesTheObjectAndRunsAgainWithoutFailing(t *testing.T) {
	objects := objectsUnder(t)
	if _, err := objects.Put(context.Background(), "swept", []byte("unreferenced")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := objects.Delete(context.Background(), "swept"); err != nil {
		t.Fatalf("the first Delete: %v", err)
	}
	if _, err := objects.Get(context.Background(), "swept"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Get after Delete returned %v, want ENOENT", err)
	}
	if err := objects.Delete(context.Background(), "swept"); err != nil {
		t.Fatalf("the second Delete: %v", err)
	}
	if err := objects.Delete(context.Background(), "never-written"); err != nil {
		t.Fatalf("Delete of a key nobody wrote: %v", err)
	}
}

func TestPrefixesSeparateTwoWorkspacesSharingAContainer(t *testing.T) {
	url := sharedContainer(t)
	stamp := time.Now().UnixNano()

	one, err := NewWithSharedKey(url, accountName, accountKey, fmt.Sprintf("tenant-a-%d/", stamp))
	if err != nil {
		t.Fatalf("opening the first workspace: %v", err)
	}
	two, err := NewWithSharedKey(url, accountName, accountKey, fmt.Sprintf("tenant-b-%d/", stamp))
	if err != nil {
		t.Fatalf("opening the second workspace: %v", err)
	}

	if _, err := one.Put(context.Background(), "shared-key", []byte("belongs to the first")); err != nil {
		t.Fatalf("the first workspace's Put: %v", err)
	}
	if _, err := two.Get(context.Background(), "shared-key"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the second workspace read the first one's key: %v", err)
	}
	if _, err := two.Put(context.Background(), "shared-key", []byte("belongs to the second")); err != nil {
		t.Fatalf("the second workspace's Put: %v", err)
	}

	got, err := one.Get(context.Background(), "shared-key")
	if err != nil {
		t.Fatalf("the first workspace's Get: %v", err)
	}
	if string(got) != "belongs to the first" {
		t.Errorf("the first workspace read %q, want its own bytes", got)
	}
}

// closedPort is an address nothing listens on: a port is taken and released, so the address
// is known to be free rather than assumed to be.
func closedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("taking a port to release: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing the port: %v", err)
	}
	return address
}

// impatient is a context that turns the SDK's retries off.
//
// The default policy makes four attempts with an exponential backoff, so one call against an
// address that refuses every connection spends seven to eleven seconds asleep. What the
// cases below assert is how a failure is classified, and the fourth attempt's error is
// classified the way the first one's is: the wait costs them the whole of their runtime and
// buys nothing they check. A negative MaxRetries is how the SDK is told "none" — zero means
// "whatever the default is", which is three.
func impatient(ctx context.Context) context.Context {
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
}

// A store nothing answers for must not report an object as absent. This is the mistake
// R-ERR-2 exists to forbid: a Get that answers ENOENT because the network is down tells a
// caller the file is gone, and a Delete that answers success because it could not reach the
// service tells a sweeper the object was removed.
func TestAnUnreachableServiceIsNeverReportedAsAbsent(t *testing.T) {
	address := closedPort(t)
	objects, err := NewWithSharedKey("http://"+address+"/devstoreaccount1/somewhere", accountName, accountKey, "prefix/")
	if err != nil {
		t.Fatalf("opening a store on an address nothing listens on: %v", err)
	}
	ctx := impatient(context.Background())

	t.Run("get", func(t *testing.T) {
		content, err := objects.Get(ctx, "a-key")
		if err == nil {
			t.Fatalf("Get returned %q and no error", content)
		}
		if errors.Is(err, syscall.ENOENT) {
			t.Errorf("Get reported an unreachable service as an absent object: %v", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := objects.Delete(ctx, "a-key")
		if err == nil {
			t.Fatal("Delete reported success against an unreachable service")
		}
		if errors.Is(err, syscall.ENOENT) {
			t.Errorf("Delete reported an unreachable service as an absent object: %v", err)
		}
	})

	t.Run("put", func(t *testing.T) {
		digest, err := objects.Put(ctx, "a-key", []byte("bytes nobody received"))
		if err == nil {
			t.Fatalf("Put reported success against an unreachable service, with digest %x", digest)
		}
		if errors.Is(err, syscall.ENOENT) {
			t.Errorf("Put reported an unreachable service as an absent object: %v", err)
		}
		// The message stands in for every log line this failure will ever produce: it must
		// say what failed without carrying the endpoint the caller never supplied.
		if message := err.Error(); !strings.Contains(message, `put "a-key"`) {
			t.Errorf("Put failed with %q, which does not name the key that failed", message)
		}
	})
}

// A container that is not there answers with the same 404 status as a blob that is not
// there, and means something entirely different: the object may exist, unreachable behind a
// name this client cannot resolve.
func TestAnAbsentContainerIsNeverReportedAsAbsent(t *testing.T) {
	url := fmt.Sprintf("%s/rfs-no-such-container-%d", accountURL(), time.Now().UnixNano())
	objects, err := NewWithSharedKey(url, accountName, accountKey, "prefix/")
	if err != nil {
		t.Fatalf("opening a store on a container that does not exist: %v", err)
	}

	if _, err := objects.Get(context.Background(), "a-key"); errors.Is(err, syscall.ENOENT) {
		t.Errorf("Get reported an absent container as an absent object: %v", err)
	} else if err == nil {
		t.Error("Get succeeded against a container that does not exist")
	} else if message := err.Error(); !strings.Contains(message, string(bloberror.ContainerNotFound)) {
		t.Errorf("Get failed with %q, which does not say what the service answered", message)
	}

	if _, err := objects.Put(context.Background(), "a-key", []byte("bytes")); errors.Is(err, syscall.ENOENT) {
		t.Errorf("Put reported an absent container as an absent object: %v", err)
	} else if err == nil {
		t.Error("Put succeeded against a container that does not exist")
	}

	// Delete converges on an absent object by answering nil, so an absent container reaching
	// that path would report a sweep as done that never happened.
	if err := objects.Delete(context.Background(), "a-key"); err == nil {
		t.Error("Delete reported success against a container that does not exist")
	} else if errors.Is(err, syscall.ENOENT) {
		t.Errorf("Delete reported an absent container as an absent object: %v", err)
	}
}

func TestNewRefusesAPrefixThatWouldNotSeparate(t *testing.T) {
	_, err := NewWithSharedKey(sharedContainer(t), accountName, accountKey, "tenant-a")
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("a prefix not ending in the delimiter was accepted with %v, want EINVAL", err)
	}
}

// An empty prefix is the deployment where one workspace owns the whole container.
func TestAnEmptyPrefixAddressesTheContainerItself(t *testing.T) {
	objects, err := NewWithSharedKey(sharedContainer(t), accountName, accountKey, "")
	if err != nil {
		t.Fatalf("opening a store with no prefix: %v", err)
	}
	key := fmt.Sprintf("unprefixed-%d", time.Now().UnixNano())
	if _, err := objects.Put(context.Background(), key, []byte("at the container's root")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, err := objects.Get(context.Background(), key); err != nil {
		t.Fatalf("Get: %v", err)
	} else if string(got) != "at the container's root" {
		t.Errorf("Get returned %q", got)
	}
}

// The refusal is checked through refuseOversize rather than through Put, because provoking
// it through Put means holding MaxObjectBytes in memory to send bytes that will not be sent.
func TestPutRefusesContentLargerThanOneWriteStores(t *testing.T) {
	if err := refuseOversize("a-key", MaxObjectBytes); err != nil {
		t.Errorf("content of exactly MaxObjectBytes was refused: %v", err)
	}
	err := refuseOversize("a-key", MaxObjectBytes+1)
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("content one byte over the limit was refused with %v, want EFBIG", err)
	}
}

func TestAWithdrawnRequestIsReportedAsInterrupted(t *testing.T) {
	objects := objectsUnder(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := objects.Put(ctx, "withdrawn", []byte("bytes")); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Put on a cancelled context returned %v, want EINTR", err)
	}
	if _, err := objects.Get(ctx, "withdrawn"); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Get on a cancelled context returned %v, want EINTR", err)
	}
	if err := objects.Delete(ctx, "withdrawn"); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Delete on a cancelled context returned %v, want EINTR", err)
	}
}

func TestNewFromConnectionStringReachesTheSameContainer(t *testing.T) {
	name := strings.TrimPrefix(sharedContainer(t), accountURL()+"/")
	connection := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;",
		accountName, accountKey, accountURL())

	prefix := fmt.Sprintf("from-connection-string-%d/", time.Now().UnixNano())
	objects, err := NewFromConnectionString(connection, name, prefix)
	if err != nil {
		t.Fatalf("opening the store from a connection string: %v", err)
	}
	if _, err := objects.Put(context.Background(), "a-key", []byte("reached the same container")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, err := objects.Get(context.Background(), "a-key"); err != nil {
		t.Fatalf("Get: %v", err)
	} else if string(got) != "reached the same container" {
		t.Errorf("Get returned %q", got)
	}
}

// Every code the mapping knows must be an errno the contract allows to travel, and only the
// one code that means the object is not there may be ENOENT.
func TestOnlyBlobNotFoundMeansTheObjectIsAbsent(t *testing.T) {
	for code, errno := range errnoByCode {
		if _, ok := storage.ErrnoName(errno); !ok {
			t.Errorf("%s maps to %v, which is outside the errno vocabulary", code, errno)
		}
		if errno == syscall.ENOENT && code != bloberror.BlobNotFound {
			t.Errorf("%s maps to ENOENT, so a failure that is not an absent object reads as one", code)
		}
	}
	for _, code := range []bloberror.Code{
		bloberror.ContainerNotFound,
		bloberror.AuthenticationFailed,
		bloberror.AuthorizationFailure,
		bloberror.ServerBusy,
		bloberror.OperationTimedOut,
		bloberror.InternalError,
		bloberror.ResourceNotFound,
	} {
		answered := &azcore.ResponseError{ErrorCode: string(code), StatusCode: 404}
		if errno := errnoOf(answered); errno == syscall.ENOENT {
			t.Errorf("%s reads as an absent object", code)
		}
		if err := failure("get", "a-key", answered); errors.Is(err, syscall.ENOENT) {
			t.Errorf("a get failing with %s reads as an absent object: %v", code, err)
		}
	}

	// A failure the service never answered for carries no code, and is EIO rather than
	// anything that claims to know what happened.
	if errno := errnoOf(errors.New("the connection went away")); errno != syscall.EIO {
		t.Errorf("a failure with no response mapped to %v, want EIO", errno)
	}
}
