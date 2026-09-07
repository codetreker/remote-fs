package localdisk

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestWaitingOperationLimitMustBeFinite(t *testing.T) {
	options := Options{MaxWaitingOperations: math.MaxInt}
	if err := options.Validate(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Validate returned %v, want EINVAL", err)
	}
	root := privateRoot(t)
	if _, err := Open(t.Context(), root, options); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Open returned %v, want EINVAL", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid waiting-operation limit modified the root: %v", entries)
	}
}

func keysSharingShard(t *testing.T, count int) ([]string, string) {
	t.Helper()
	byShard := make(map[byte][]string)
	for index := 0; ; index++ {
		key := fmt.Sprintf("admission-shard-key-%d", index)
		location, err := locate(key)
		if err != nil {
			t.Fatal(err)
		}
		byShard[location.shard] = append(byShard[location.shard], key)
		if len(byShard[location.shard]) < count {
			continue
		}
		for shard, keys := range byShard {
			if shard != location.shard && len(keys) != 0 {
				return byShard[location.shard][:count], keys[0]
			}
		}
	}
}

func waitForWaitingOperations(t *testing.T, objects *Objects, want int) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := objects.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if status.WaitingOperations == want {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiting operations are %d, want %d", status.WaitingOperations, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestShardWaitersDoNotConsumeActiveAdmission(t *testing.T) {
	options := Options{
		MaxObjectBytes:        1024,
		MaxInFlightOperations: 1,
		MaxWaitingOperations:  5,
		MaxInFlightBytes:      fixedEnvelopeBytes + MaxKeyBytes + 1024,
		MaxRecoveryEntries:    1,
	}
	objects := openForTest(t, options)
	shared, independent := keysSharingShard(t, 4)
	sharedLocation, _ := locate(shared[0])
	independentLocation, _ := locate(independent)
	if sharedLocation.shard == independentLocation.shard {
		t.Fatal("test keys must use different shards")
	}

	originalLink := objects.ops.linkat
	firstMarkerEntered := make(chan struct{})
	releaseFirstMarker := make(chan struct{})
	independentMarkerEntered := make(chan struct{})
	releaseIndependentMarker := make(chan struct{})
	var firstMarkerBlocked atomic.Bool
	var independentMarkerBlocked atomic.Bool
	var firstMarkerReleased atomic.Bool
	var independentMarkerReleased atomic.Bool
	releaseFirst := func() {
		if firstMarkerReleased.CompareAndSwap(false, true) {
			close(releaseFirstMarker)
		}
	}
	releaseIndependent := func() {
		if independentMarkerReleased.CompareAndSwap(false, true) {
			close(releaseIndependentMarker)
		}
	}
	defer releaseFirst()
	defer releaseIndependent()
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if old == storeIdentityStageName && new == storeIdentityName {
			switch {
			case firstMarkerBlocked.CompareAndSwap(false, true):
				close(firstMarkerEntered)
				<-releaseFirstMarker
			case independentMarkerBlocked.CompareAndSwap(false, true):
				close(independentMarkerEntered)
				<-releaseIndependentMarker
			}
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}

	put := func(ctx context.Context, key string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := objects.Put(ctx, key, []byte(key))
			done <- err
		}()
		return done
	}
	firstDone := put(t.Context(), shared[0])
	<-firstMarkerEntered
	cancelContext, cancel := context.WithCancel(t.Context())
	sharedDone := []<-chan error{
		put(cancelContext, shared[1]),
		put(t.Context(), shared[2]),
		put(t.Context(), shared[3]),
	}
	waitForWaitingOperations(t, objects, 4)

	independentDone := put(t.Context(), independent)
	<-independentMarkerEntered
	status := waitForWaitingOperations(t, objects, 5)
	if status.InFlightOperations != 0 || status.InFlightBytes != 0 {
		t.Fatalf("shard coordination consumed active admission: %+v", status)
	}
	if _, err := objects.Put(t.Context(), "waiting-overflow", nil); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("overflow Put returned %v, want EAGAIN", err)
	}

	cancel()
	if err := <-sharedDone[0]; !errors.Is(err, syscall.EINTR) {
		t.Fatalf("canceled shard waiter returned %v, want EINTR", err)
	}
	waitForWaitingOperations(t, objects, 4)
	releaseIndependent()
	if err := <-independentDone; err != nil {
		t.Fatalf("independent shard Put: %v", err)
	}
	if !firstMarkerBlocked.Load() || !independentMarkerBlocked.Load() {
		t.Fatal("fault seams did not exercise both shard initializations")
	}

	releaseFirst()
	if err := <-firstDone; err != nil {
		t.Fatalf("first shared-shard Put: %v", err)
	}
	for index, done := range sharedDone[1:] {
		if err := <-done; err != nil {
			t.Fatalf("shared-shard Put %d: %v", index+2, err)
		}
	}
	status = waitForWaitingOperations(t, objects, 0)
	if status.InFlightOperations != 0 || status.InFlightBytes != 0 {
		t.Fatalf("completed operations retained admission: %+v", status)
	}
}

func TestCloseDrainsPreActiveShardWaiter(t *testing.T) {
	objects := openForTest(t, Options{MaxWaitingOperations: 1})
	key := "close-drains-shard-waiter"
	originalLink := objects.ops.linkat
	markerEntered := make(chan struct{})
	releaseMarker := make(chan struct{})
	var released atomic.Bool
	release := func() {
		if released.CompareAndSwap(false, true) {
			close(releaseMarker)
		}
	}
	defer release()
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if old == storeIdentityStageName && new == storeIdentityName {
			close(markerEntered)
			<-releaseMarker
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := objects.Put(t.Context(), key, []byte("bytes"))
		putDone <- err
	}()
	<-markerEntered
	status := waitForWaitingOperations(t, objects, 1)
	if status.InFlightOperations != 0 {
		t.Fatalf("pre-active Put consumed operation admission: %+v", status)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- objects.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, _, closing := objects.gate.snapshot()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not close admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the shard waiter drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := objects.Get(t.Context(), "new-after-close"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation admitted during Close returned %v, want EIO", err)
	}
	release()
	if err := <-putDone; err != nil {
		t.Fatalf("pre-active Put did not finish during Close: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}
