package localdisk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

const streamTestChunkLimit = 1024 * 1024

type generatedStreamReader struct {
	size    int64
	offset  int64
	maxRead int
}

func streamTestByte(offset int64) byte {
	return byte(offset*31 + offset/257 + 7)
}

func (r *generatedStreamReader) Read(p []byte) (int, error) {
	if len(p) > streamTestChunkLimit {
		return 0, fmt.Errorf("read requested %d bytes, limit is %d", len(p), streamTestChunkLimit)
	}
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	if r.offset == r.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.size-r.offset))
	for i := range p[:n] {
		p[i] = streamTestByte(r.offset + int64(i))
	}
	r.offset += int64(n)
	return n, nil
}

type generatedStreamWriter struct {
	size     int64
	offset   int64
	maxWrite int
}

func (w *generatedStreamWriter) Write(p []byte) (int, error) {
	if len(p) > streamTestChunkLimit {
		return 0, fmt.Errorf("write supplied %d bytes, limit is %d", len(p), streamTestChunkLimit)
	}
	if w.offset+int64(len(p)) > w.size {
		return 0, fmt.Errorf("write exceeds declared payload size %d", w.size)
	}
	if len(p) > w.maxWrite {
		w.maxWrite = len(p)
	}
	for i, got := range p {
		if want := streamTestByte(w.offset + int64(i)); got != want {
			return i, fmt.Errorf("payload offset %d = %02x, want %02x", w.offset+int64(i), got, want)
		}
	}
	w.offset += int64(len(p))
	return len(p), nil
}

func generatedStreamDigest(size int64) [sha256.Size]byte {
	hash := sha256.New()
	var chunk [32 * 1024]byte
	for offset := int64(0); offset < size; {
		n := int(min(int64(len(chunk)), size-offset))
		for i := range chunk[:n] {
			chunk[i] = streamTestByte(offset + int64(i))
		}
		_, _ = hash.Write(chunk[:n])
		offset += int64(n)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func TestStreamRoundTripUsesBoundedIOAndPersistsAcrossReopen(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := objects.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	var capability objectstore.StreamingObjects = objects
	if err := capability.CheckStreamingObjects(); err != nil {
		t.Fatal(err)
	}
	const key = "stream/key/with\x00bytes"
	const size = int64(3*streamTestChunkLimit + 137)
	digest := generatedStreamDigest(size)
	source := &generatedStreamReader{size: size}
	if err := objects.PutStream(t.Context(), key, size, digest, source); err != nil {
		t.Fatal(err)
	}
	if source.offset != size || source.maxRead == 0 {
		t.Fatalf("source consumed %d of %d bytes; largest read %d", source.offset, size, source.maxRead)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	destination := &generatedStreamWriter{size: size}
	seal, err := reopened.GetStream(t.Context(), key, destination)
	if err != nil {
		t.Fatal(err)
	}
	if seal.Size != size || seal.Digest != digest {
		t.Fatalf("seal = %+v, want size %d and digest %x", seal, size, digest)
	}
	if destination.offset != size || destination.maxWrite == 0 {
		t.Fatalf("destination received %d of %d bytes; largest write %d", destination.offset, size, destination.maxWrite)
	}
}

func TestStreamEnvelopeInteroperatesWithWholeObjectIO(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte("ordinary payload")} {
		t.Run(fmt.Sprintf("size=%d", len(payload)), func(t *testing.T) {
			objects := openForTest(t, Options{})
			digest := sha256.Sum256(payload)
			if err := objects.PutStream(t.Context(), "streamed", int64(len(payload)), digest, bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}
			got, err := objects.Get(t.Context(), "streamed")
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("whole-object read = %q, %v; want %q", got, err, payload)
			}
			if _, err := objects.Put(t.Context(), "whole", payload); err != nil {
				t.Fatal(err)
			}
			var destination bytes.Buffer
			seal, err := objects.GetStream(t.Context(), "whole", &destination)
			if err != nil || !bytes.Equal(destination.Bytes(), payload) || seal.Size != int64(len(payload)) || seal.Digest != digest {
				t.Fatalf("stream read = %q, %+v, %v", destination.Bytes(), seal, err)
			}
		})
	}
}

func TestStreamWorksWithinSmallConfiguredByteBudget(t *testing.T) {
	const maximum = int64(1024)
	objects := openForTest(t, Options{
		MaxObjectBytes:   maximum,
		MaxInFlightBytes: fixedEnvelopeBytes + MaxKeyBytes + maximum,
	})
	for _, size := range []int64{0, 1, maximum} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			key := fmt.Sprintf("small-%d", size)
			digest := generatedStreamDigest(size)
			if err := objects.PutStream(t.Context(), key, size, digest, &generatedStreamReader{size: size}); err != nil {
				t.Fatal(err)
			}
			destination := &generatedStreamWriter{size: size}
			seal, err := objects.GetStream(t.Context(), key, destination)
			if err != nil || seal.Size != size || seal.Digest != digest || destination.offset != size {
				t.Fatalf("small-budget GetStream = %+v, %v; received %d of %d bytes", seal, err, destination.offset, size)
			}
			assertShardAdmission(t, objects, 0, 0)
		})
	}
}

func assertStreamPutAbsent(t *testing.T, objects *Objects, key string) {
	t.Helper()
	for _, path := range []string{objectPath(t, objects.rootPath, key), stagingPath(t, objects.rootPath, key)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("failed PutStream left %s: %v", path, err)
		}
	}
	marker, err := markerName(key, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(objects.rootPath, objectsDirectory, marker)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("failed PutStream left recovery record: %v", err)
	}
	if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("failed PutStream exposed an object or poisoned a clean store: %v", err)
	}
	assertShardAdmission(t, objects, 0, 0)
	assertShardLocksReleased(t, objects, key)
}

func TestStreamPutRejectsLengthAndDigestMismatchWithoutPublishing(t *testing.T) {
	payload := []byte("declared payload")
	digest := sha256.Sum256(payload)
	wrongDigest := digest
	wrongDigest[0] ^= 1
	for _, test := range []struct {
		name    string
		size    int64
		digest  [sha256.Size]byte
		payload []byte
	}{
		{name: "short", size: int64(len(payload) + 1), digest: digest, payload: payload},
		{name: "long", size: int64(len(payload) - 1), digest: digest, payload: payload},
		{name: "wrong-digest", size: int64(len(payload)), digest: wrongDigest, payload: payload},
		{name: "zero-length-with-content", size: 0, digest: sha256.Sum256(nil), payload: payload},
		{name: "negative-length", size: -1, digest: digest, payload: payload},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := openForTest(t, Options{})
			const key = "rejected"
			if err := objects.PutStream(t.Context(), key, test.size, test.digest, bytes.NewReader(test.payload)); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("PutStream = %v, want EINVAL", err)
			}
			assertStreamPutAbsent(t, objects, key)
			if err := objects.PutStream(t.Context(), key, int64(len(payload)), digest, bytes.NewReader(payload)); err != nil {
				t.Fatalf("valid retry after rejected stream: %v", err)
			}
		})
	}
}

func TestStreamPutIsCreateOnly(t *testing.T) {
	objects := openForTest(t, Options{})
	const key = "immutable"
	first := []byte("first")
	if err := objects.PutStream(t.Context(), key, int64(len(first)), sha256.Sum256(first), bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	second := []byte("replacement")
	if err := objects.PutStream(t.Context(), key, int64(len(second)), sha256.Sum256(second), bytes.NewReader(second)); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("duplicate PutStream = %v, want EEXIST", err)
	}
	got, err := objects.Get(t.Context(), key)
	if err != nil || !bytes.Equal(got, first) {
		t.Fatalf("duplicate changed original object: %q, %v", got, err)
	}
}

type failingStreamReader struct {
	err error
}

func (r failingStreamReader) Read([]byte) (int, error) { return 0, r.err }

type failingStreamWriter struct {
	err   error
	short bool
}

func (w failingStreamWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, w.err
}

func TestStreamIOFailuresPreserveErrorsAndReleaseAdmission(t *testing.T) {
	objects := openForTest(t, Options{})
	sourceFailure := errors.New("source failed")
	if err := objects.PutStream(t.Context(), "source-failed", 1, sha256.Sum256([]byte{0}), failingStreamReader{err: sourceFailure}); !errors.Is(err, sourceFailure) {
		t.Fatalf("PutStream = %v, want original source error", err)
	}
	assertStreamPutAbsent(t, objects, "source-failed")
	payload := []byte("destination payload")
	if err := objects.PutStream(t.Context(), "destination-failed", int64(len(payload)), sha256.Sum256(payload), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	destinationFailure := errors.New("destination failed")
	for _, test := range []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{name: "failure", writer: failingStreamWriter{err: destinationFailure}, want: destinationFailure},
		{name: "short-write", writer: failingStreamWriter{short: true}, want: io.ErrShortWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			seal, err := objects.GetStream(t.Context(), "destination-failed", test.writer)
			if !errors.Is(err, test.want) || seal != (objectstore.ObjectSeal{}) {
				t.Fatalf("GetStream = %+v, %v; want zero seal and %v", seal, err, test.want)
			}
			assertShardAdmission(t, objects, 0, 0)
		})
	}
	got, err := objects.Get(t.Context(), "destination-failed")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("destination failure damaged stored object: %q, %v", got, err)
	}
}

func TestStreamGetDistinguishesMissingAndCorruptObjects(t *testing.T) {
	objects := openForTest(t, Options{})
	seal, err := objects.GetStream(t.Context(), "missing", io.Discard)
	if !errors.Is(err, syscall.ENOENT) || seal != (objectstore.ObjectSeal{}) {
		t.Fatalf("missing GetStream = %+v, %v; want ENOENT and zero seal", seal, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "payload", mutate: flipByte(fixedEnvelopeBytes + len("corrupt"))},
		{name: "store-identity", mutate: flipByte(12)},
		{name: "declared-size", mutate: flipByte(39)},
		{name: "symlink", mutate: func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("unrelated", path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "public-permissions", mutate: func(t *testing.T, path string) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "truncated", mutate: func(t *testing.T, path string) {
			if err := os.Truncate(path, fixedEnvelopeBytes-1); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := openForTest(t, Options{})
			const key = "corrupt"
			payload := []byte("content under verification")
			if err := objects.PutStream(t.Context(), key, int64(len(payload)), sha256.Sum256(payload), bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, objectPath(t, objects.rootPath, key))
			seal, err := objects.GetStream(t.Context(), key, io.Discard)
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) || seal != (objectstore.ObjectSeal{}) {
				t.Fatalf("corrupt GetStream = %+v, %v; want only EIO and zero seal", seal, err)
			}
			assertShardAdmission(t, objects, 0, 0)
		})
	}
}

type cancelingStreamReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r *cancelingStreamReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.cancel()
	return n, err
}

type cancelingStreamWriter struct {
	cancel context.CancelFunc
}

func (w cancelingStreamWriter) Write(p []byte) (int, error) {
	w.cancel()
	return len(p), nil
}

func TestStreamCancellationDoesNotPublishOrReturnASeal(t *testing.T) {
	objects := openForTest(t, Options{})
	const size = int64(2*streamTestChunkLimit + 1)
	digest := generatedStreamDigest(size)
	putCtx, cancelPut := context.WithCancel(t.Context())
	defer cancelPut()
	source := &cancelingStreamReader{reader: &generatedStreamReader{size: size}, cancel: cancelPut}
	if err := objects.PutStream(putCtx, "canceled-put", size, digest, source); !errors.Is(err, syscall.EINTR) {
		t.Fatalf("canceled PutStream = %v, want EINTR", err)
	}
	assertStreamPutAbsent(t, objects, "canceled-put")
	if err := objects.PutStream(t.Context(), "canceled-get", size, digest, &generatedStreamReader{size: size}); err != nil {
		t.Fatal(err)
	}
	getCtx, cancelGet := context.WithCancel(t.Context())
	defer cancelGet()
	seal, err := objects.GetStream(getCtx, "canceled-get", cancelingStreamWriter{cancel: cancelGet})
	if !errors.Is(err, syscall.EINTR) || seal != (objectstore.ObjectSeal{}) {
		t.Fatalf("canceled GetStream = %+v, %v; want EINTR and zero seal", seal, err)
	}
	assertShardAdmission(t, objects, 0, 0)
}

func TestStreamStagingSyncFailureDoesNotPublishAndAllowsRetry(t *testing.T) {
	objects := openForTest(t, Options{})
	const key = "stream-sync-failure"
	stagePath := stagingPath(t, objects.rootPath, key)
	originalSync := objects.ops.fsync
	failed := false
	objects.ops.fsync = func(fd int) error {
		var opened, staged unix.Stat_t
		if err := unix.Fstat(fd, &opened); err != nil {
			return err
		}
		if !failed && unix.Stat(stagePath, &staged) == nil && opened.Dev == staged.Dev && opened.Ino == staged.Ino {
			failed = true
			return syscall.EIO
		}
		return originalSync(fd)
	}
	payload := []byte("must cross a durability barrier")
	digest := sha256.Sum256(payload)
	if err := objects.PutStream(t.Context(), key, int64(len(payload)), digest, bytes.NewReader(payload)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("PutStream = %v, want staging fsync failure", err)
	}
	if !failed {
		t.Fatal("fsync fault did not reach the streaming staging inode")
	}
	objects.ops.fsync = originalSync
	assertStreamPutAbsent(t, objects, key)
	if err := objects.PutStream(t.Context(), key, int64(len(payload)), digest, bytes.NewReader(payload)); err != nil {
		t.Fatalf("retry after staging sync failure: %v", err)
	}
	got, err := objects.Get(t.Context(), key)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("retry returned %q, %v", got, err)
	}
}
