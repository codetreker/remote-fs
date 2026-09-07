package azblob

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

func TestAvailableProbesTheContainerBeforeRefusingCapacity(t *testing.T) {
	objects := objectsUnder(t)
	available, err := objects.Available(context.Background())
	if !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("Available returned %d bytes and %v, want ENOSYS after a successful probe", available, err)
	}
}

func TestAvailableReportsAnUnreachableService(t *testing.T) {
	address := closedPort(t)
	objects, err := NewWithSharedKey("http://"+address+"/devstoreaccount1/somewhere", accountName, accountKey, "prefix/")
	if err != nil {
		t.Fatalf("opening a store on an address nothing listens on: %v", err)
	}

	available, err := objects.Available(impatient(context.Background()))
	if err == nil || errors.Is(err, syscall.ENOSYS) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("Available returned %d bytes and %v, want the unreachable service as EIO", available, err)
	}
}

func TestAvailableReportsAnAbsentContainer(t *testing.T) {
	url := fmt.Sprintf("%s/rfs-no-such-capacity-container-%d", accountURL(), time.Now().UnixNano())
	objects, err := NewWithSharedKey(url, accountName, accountKey, "prefix/")
	if err != nil {
		t.Fatalf("opening a store on a container that does not exist: %v", err)
	}

	available, err := objects.Available(context.Background())
	if err == nil || errors.Is(err, syscall.ENOSYS) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("Available returned %d bytes and %v, want the missing container as EIO", available, err)
	}
	if !strings.Contains(err.Error(), string(bloberror.ContainerNotFound)) {
		t.Fatalf("Available failed with %q, which does not say what the service answered", err)
	}
}

func TestAvailableReportsRejectedCredentials(t *testing.T) {
	wrongKey := base64.StdEncoding.EncodeToString(make([]byte, 64))
	objects, err := NewWithSharedKey(sharedContainer(t), accountName, wrongKey, "prefix/")
	if err != nil {
		t.Fatalf("opening a store with a syntactically valid wrong key: %v", err)
	}

	available, err := objects.Available(context.Background())
	if err == nil || errors.Is(err, syscall.ENOSYS) || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("Available returned %d bytes and %v, want rejected credentials as EACCES", available, err)
	}
}
