package objectstore_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type heldContentIO struct {
	*memory.Objects
	operation        string
	enabled          atomic.Bool
	entered, release chan struct{}
	once             sync.Once
}

func (o *heldContentIO) hold(ctx context.Context, operation string) error {
	if !o.enabled.Load() || o.operation != operation {
		return nil
	}
	o.once.Do(func() { close(o.entered) })
	select {
	case <-o.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (o *heldContentIO) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	if err := o.hold(ctx, "put"); err != nil {
		return nil, err
	}
	return o.Objects.Put(ctx, key, body)
}
func (o *heldContentIO) GetBounded(ctx context.Context, key string, limit int64) ([]byte, error) {
	if err := o.hold(ctx, "get"); err != nil {
		return nil, err
	}
	return o.Objects.GetBounded(ctx, key, limit)
}

func TestRetainedContentIOPinsOutliveRetirement(t *testing.T) {
	for _, operation := range []string{"get", "put"} {
		for _, retirement := range []string{"expiry", "close"} {
			t.Run(operation+"/"+retirement, func(t *testing.T) {
				objects := &heldContentIO{Objects: memory.New(), operation: operation, entered: make(chan struct{}), release: make(chan struct{})}
				release := sync.OnceFunc(func() { close(objects.release) })
				defer release()
				volume, meta := fileVolume(t, objects, 4096, nil)
				options := storage.DefaultFileSessionOptions()
				options.Lease = time.Second
				session := fileSessionFor(t, volume, options)
				file := openFileFor(t, session, "held", retainedOpenOptions{Read: true, Write: true, Create: true})
				if _, err := file.WriteAt(t.Context(), 0, []byte("kept")); err != nil {
					t.Fatal(err)
				}
				if err := volume.Remove(t.Context(), "held"); err != nil {
					t.Fatal(err)
				}
				if _, err := session.Renew(t.Context()); err != nil {
					t.Fatal(err)
				}
				writeID := retainedAction(t, session)
				closeID := retainedAction(t, session)
				objects.enabled.Store(true)
				ioDone := make(chan error, 1)
				go func() {
					if operation == "get" {
						result, err := file.ReadAt(t.Context(), 0, 4)
						if err == nil && string(result.Data) != "kept" {
							err = errors.New("captured object bytes changed")
						}
						ioDone <- err
					} else {
						receipt, err := file.File.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("next")}, writeID)
						if err == nil || receipt.Effects&storage.EffectContentChanged != 0 {
							ioDone <- errors.New("retirement allowed a late publication")
							return
						}
						expected := syscall.ESTALE
						if retirement == "close" {
							expected = syscall.EBADF
						}
						if !errors.Is(err, expected) {
							ioDone <- err
							return
						}
						ioDone <- nil
					}
				}()
				select {
				case <-objects.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("content operation did not enter backend")
				}
				var closeDone chan error
				if retirement == "expiry" {
					await(t, "session expiry while backend I/O is held", func() bool {
						status, err := session.Status(t.Context())
						return err == nil && status.Retired
					})
				} else {
					closeDone = make(chan error, 1)
					go func() { _, err := file.File.Close(t.Context(), closeID); closeDone <- err }()
					await(t, "reference fence while backend I/O is held", func() bool {
						_, err := file.Stat(t.Context())
						return errors.Is(err, syscall.EBADF)
					})
				}
				if used, err := meta.Usage(t.Context()); err != nil || used != 4 {
					t.Fatalf("retirement released admitted I/O's object: usage=%d,%v", used, err)
				}
				select {
				case err := <-ioDone:
					t.Fatalf("backend I/O returned before release: %v", err)
				default:
				}
				if closeDone != nil {
					select {
					case err := <-closeDone:
						t.Fatalf("close returned before I/O drain: %v", err)
					default:
					}
				}
				release()
				if err := <-ioDone; err != nil {
					t.Fatal(err)
				}
				if closeDone != nil {
					if err := <-closeDone; err != nil {
						t.Fatal(err)
					}
				}
				await(t, "physical cleanup after backend I/O drain", func() bool { used, err := meta.Usage(t.Context()); return err == nil && used == 0 })
			})
		}
	}
}
