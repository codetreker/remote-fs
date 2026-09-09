package azblob

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFailureClassificationRetainsServiceCause(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		want  syscall.Errno
	}{
		{"absent_blob", &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound), StatusCode: 404}, syscall.ENOENT},
		{"absent_container", &azcore.ResponseError{ErrorCode: string(bloberror.ContainerNotFound), StatusCode: 404}, syscall.EIO},
		{"denied", &azcore.ResponseError{ErrorCode: string(bloberror.AuthorizationFailure), StatusCode: 403}, syscall.EACCES},
		{"cancelled", context.Canceled, syscall.EINTR},
		{"deadline", context.DeadlineExceeded, syscall.EIO},
		{"unknown", errors.New("connection interrupted"), syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := failure("get", "key", test.cause)
			if got := storage.ErrnoOf(fmt.Errorf("reading object: %w", err)); got != test.want {
				t.Fatalf("classification = %v, want %v", got, test.want)
			}
			if !errors.Is(err, test.cause) || !errors.Is(err, test.want) {
				t.Fatalf("error lost its service cause or errno: %v", err)
			}
			if got := storage.ErrnoOf(errors.Join(err, syscall.EIO)); got != syscall.EIO {
				t.Fatalf("independent failure classified as %v, want EIO", got)
			}
		})
	}
}
