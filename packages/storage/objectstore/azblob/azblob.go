// Package azblob implements objectstore.Objects over one Azure Blob Storage container.
//
// Every workspace's objects live in the same container, separated by a key prefix, so the
// blob name for a key is prefix + key and nothing here ever addresses a blob outside its
// own prefix. Sharing one container is what makes a workspace cheap to create: a container
// is an ARM-visible resource with its own lifecycle, and a prefix is a string.
//
// # Which API version we speak, and why it is not the newest
//
// The SDK pins the service API version it sends as a compile-time constant with no public
// override, so the SDK release chooses the wire version. We hold azblob at v1.6.2, which
// pins 2025-11-05: that is the newest version Azurite accepts, and Azurite is what the
// tests run against. Azurite 3.36.0 — the latest release, published 2026-07-17 — answers
// 400 InvalidHeaderValue ("The API version 2026-06-06 is not supported by Azurite") to the
// 2026-04-06 and 2026-06-06 that azblob v1.7.0 and v1.8.0 send, so a newer SDK does not
// merely degrade the tests, it fails every request they make.
//
// Azure keeps every published API version working indefinitely, so speaking an older one
// costs nothing against a real account. Upgrading the SDK past v1.6.2 therefore has to wait
// for an Azurite that accepts the version the newer SDK sends; a rewrite of x-ms-version by
// a pipeline policy is not the way out, because it would make the SDK's generated requests
// claim a version whose semantics they were not generated for.
package azblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"strings"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

// MaxObjectBytes is the largest object one Put can store.
//
// It is the service's own limit on a single Put Blob at API version 2019-12-12 and later,
// and it is enforced here rather than left to the service because the alternative is worse
// in both directions. The SDK's blockblob.MaxUploadBlobBytes is 256 MiB — the limit of an
// API version three generations older — and blockblob.Client.Upload does not check it, so
// the SDK neither refuses an oversized body nor splits it; and the SDK call that does split
// it, UploadBuffer, commits a block list, whose response Content-MD5 is a digest of the
// block list XML rather than of the blob. Above this size there is no single call that both
// stores the object and reports a digest of it, so a Put that large is refused outright.
const MaxObjectBytes = 5000 * 1024 * 1024

// Objects is the set of blobs under one prefix in one container.
type Objects struct {
	container *container.Client
	prefix    string
}

var _ objectstore.Objects = (*Objects)(nil)

// NewWithSharedKey opens the objects under prefix in the container at containerURL, signing
// with the account's shared key.
//
// containerURL carries the whole address, so the same call reaches a real account
// (https://<account>.blob.core.windows.net/<container>) and an emulator, whose URL puts the
// account in the path (http://127.0.0.1:10000/devstoreaccount1/<container>). Nothing here
// has to know which of the two it is talking to.
func NewWithSharedKey(containerURL, accountName, accountKey, prefix string) (*Objects, error) {
	credential, err := container.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("the credential for account %q: %w", accountName, err)
	}
	client, err := container.NewClientWithSharedKeyCredential(containerURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("the container at %s: %w", containerURL, err)
	}
	return newObjects(client, prefix)
}

// NewFromConnectionString opens the objects under prefix in the named container of the
// account the connection string describes.
//
// A connection string names the endpoints as well as the credentials, which is how an
// emulator is configured in practice — UseDevelopmentStorage=true expands to the well-known
// development account and its loopback endpoints.
func NewFromConnectionString(connectionString, containerName, prefix string) (*Objects, error) {
	client, err := container.NewClientFromConnectionString(connectionString, containerName, nil)
	if err != nil {
		return nil, fmt.Errorf("the container %q: %w", containerName, err)
	}
	return newObjects(client, prefix)
}

// newObjects settles the prefix.
//
// A non-empty prefix must end in the delimiter, because prefixes that do not are not
// guaranteed to separate anything: "a" and "ab" both hold the blob named "abc", and the
// workspace that reserved the key would never learn that another one had taken it. An empty
// prefix is allowed and means the workspace owns the whole container, which is a different
// deployment rather than a degenerate case of this one.
func newObjects(client *container.Client, prefix string) (*Objects, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("the key prefix %q does not end in %q, so it does not separate "+
			"this workspace's keys from those of a workspace whose prefix it begins: %w",
			prefix, "/", syscall.EINVAL)
	}
	return &Objects{container: client, prefix: prefix}, nil
}

// Put stores content under key, and returns the digest the service computed for the bytes
// it stored.
//
// The upload is conditional on If-None-Match: *, so a key that has already been written is
// refused with EEXIST rather than overwritten. Keys are reserved by the metastore and used
// once, so a collision means a reservation was handed out twice, and an object silently
// replaced is one that readers holding the old key would then read as the new file.
//
// The content's MD5 travels in the request's Content-MD5 header, which the service verifies
// against the bytes that reach it: a body corrupted in transit fails the Put rather than
// being stored. Against Azurite 3.36.0 a deliberately wrong Content-MD5 answers 400
// InvalidOperation, "Provided contentMD5 doesn't match".
//
// The digest returned is the Content-MD5 the service put in its response, not the local
// sum. Blob Storage computes that header from the bytes it stored — the Put Blob reference
// says so, and it is returned "even when the request doesn't include Content-MD5 or
// x-ms-blob-content-md5 headers", which Azurite confirms: a Put carrying no checksum header
// at all comes back with the correct Content-MD5. Since the service also verified the
// header we sent, a response that arrives at all is the service asserting that what it
// stored hashes to this value.
//
// x-ms-content-crc64 would be the other candidate and is not used: Azurite returns no such
// header on Put Blob, so a digest taken from it would be a value in production and nil
// against the emulator every test runs against.
func (o *Objects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	if err := refuseOversize(key, len(content)); err != nil {
		return nil, err
	}

	sum := md5.Sum(content)
	response, err := o.container.NewBlockBlobClient(o.name(key)).Upload(
		ctx,
		streaming.NopCloser(bytes.NewReader(content)),
		&blockblob.UploadOptions{
			TransactionalValidation: blob.TransferValidationTypeMD5(sum[:]),
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfNoneMatch: to.Ptr(azcore.ETagAny),
				},
			},
		},
	)
	if err != nil {
		return nil, failure("put", key, err)
	}
	return response.ContentMD5, nil
}

// Get returns the whole object under key.
//
// The length the service stated is checked against the length that arrived, so a body that
// ends early cannot come back as a shorter object that reads like a complete one. The HTTP
// client is expected to report a truncated response of its own accord; this makes the
// guarantee hold whether or not it does.
func (o *Objects) Get(ctx context.Context, key string) ([]byte, error) {
	response, err := o.container.NewBlobClient(o.name(key)).DownloadStream(ctx, nil)
	if err != nil {
		return nil, failure("get", key, err)
	}
	defer response.Body.Close()

	content, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, failure("get", key, err)
	}
	if response.ContentLength == nil {
		return nil, fmt.Errorf("get %q: the service did not say how many bytes the object holds, "+
			"so a truncated body cannot be told from a whole one: %w", key, syscall.EIO)
	}
	if int64(len(content)) != *response.ContentLength {
		return nil, fmt.Errorf("get %q: the service said the object holds %d bytes and sent %d: %w",
			key, *response.ContentLength, len(content), syscall.EIO)
	}
	return content, nil
}

// Delete removes the object under key, and treats an object that is not there as done.
//
// Only BlobNotFound is read that way. ContainerNotFound arrives with the same 404 status
// and means the opposite: the object may well exist, behind a container this client cannot
// see, so reporting the sweep as complete would leave it to be paid for forever with
// nothing left recording that it is there (R-ERR-2).
func (o *Objects) Delete(ctx context.Context, key string) error {
	_, err := o.container.NewBlobClient(o.name(key)).Delete(ctx, nil)
	if err != nil {
		if errnoOf(err) == syscall.ENOENT {
			return nil
		}
		return failure("delete", key, err)
	}
	return nil
}

// name is the blob a key stands for. Keys are reserved by the metastore, so they arrive
// already fit to be used and are not inspected here.
func (o *Objects) name(key string) string {
	return o.prefix + key
}

// refuseOversize rejects content no single write can store. It takes the size rather than
// the content so that the refusal can be exercised without holding MaxObjectBytes in memory.
func refuseOversize(key string, size int) error {
	if int64(size) <= MaxObjectBytes {
		return nil
	}
	return fmt.Errorf("put %q: %d bytes is more than the %d a single write stores: %w",
		key, size, int64(MaxObjectBytes), syscall.EFBIG)
}
