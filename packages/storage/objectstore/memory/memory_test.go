package memory_test

import (
	"context"
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/objectstoretest"
)

func TestContract(t *testing.T) {
	objectstoretest.Run(t, func(*testing.T) objectstore.Objects {
		return memory.New()
	})
}

// Memory reports the same MD5 as Blob Storage so that callers which record a returned
// digest exercise that path without requiring a service.
func TestPutReportsTheDigestOfTheBytesItStored(t *testing.T) {
	objects := memory.New()

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
		sum := md5.Sum(content)
		if string(digest) != string(sum[:]) {
			t.Errorf("Put of %d bytes reported digest %x, want %x", size, digest, sum)
		}
	}
}

func TestConcurrentPutsReportTheDigestOfTheirStoredBytes(t *testing.T) {
	const workers = 16
	objects := memory.New()
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	type result struct {
		content []byte
		digest  []byte
		err     error
	}
	results := make(chan result, workers)

	for worker := range workers {
		go func() {
			content := []byte(fmt.Sprintf("worker-%d's object", worker))
			ready <- struct{}{}
			<-start
			digest, err := objects.Put(t.Context(), fmt.Sprintf("worker-%d", worker), content)
			results <- result{content: content, digest: digest, err: err}
		}()
	}
	for range workers {
		<-ready
	}
	close(start)

	for range workers {
		result := <-results
		if result.err != nil {
			t.Errorf("Put: %v", result.err)
			continue
		}
		sum := md5.Sum(result.content)
		if string(result.digest) != string(sum[:]) {
			t.Errorf("Put of %q reported digest %x, want %x", result.content, result.digest, sum)
		}
	}
}
