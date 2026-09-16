package objectstore

import (
	"context"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type nodeReference struct {
	session    *fileSession
	native     metastore.NodeReference
	uses       referenceUses
	active     bool
	operations sync.WaitGroup
	retireMu   sync.Mutex
	retired    bool
	closeMu    sync.Mutex
	closeDone  chan struct{}
	closeErr   error
}

var _ storage.NodeReference = (*nodeReference)(nil)

func (r *nodeReference) begin(ctx context.Context) (context.Context, func(), error) {
	return r.admit(ctx, fileDataOperation)
}

func (r *nodeReference) admit(ctx context.Context, class fileOperationClass) (context.Context, func(), error) {
	done, err := r.session.admit(ctx, false, class)
	if err != nil {
		return nil, nil, err
	}
	r.session.mu.Lock()
	if !r.active {
		r.session.mu.Unlock()
		done()
		return nil, nil, syscall.EBADF
	}
	r.operations.Add(1)
	r.session.mu.Unlock()
	ctx, cancel := r.session.operationContext(ctx)
	ctx = metastore.WithFilePublicationGuard(ctx, r.session.publicationAllowed)
	return ctx, func() { cancel(); r.operations.Done(); done() }, nil
}

func (r *nodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := r.native.Node(ctx)
	return state.Attr(), err
}

func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	if err := change.Check(); err != nil {
		return storage.Attr{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := r.native.SetAttr(ctx, change)
	return state.Attr(), err
}

func (r *nodeReference) retire() error {
	r.session.mu.Lock()
	r.active = false
	r.uses.retiring = true
	r.session.mu.Unlock()
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	if r.retired {
		return r.session.retireReferenceOwners(&r.uses)
	}
	ctx, cancel := r.session.operationContext(r.session.cleanup)
	defer cancel()
	if err := r.native.Retire(ctx); err != nil {
		return err
	}
	r.retired = true
	return r.session.retireReferenceOwners(&r.uses)
}

func (r *nodeReference) startClose() <-chan struct{} {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closeDone != nil {
		select {
		case <-r.closeDone:
			if r.closeErr == nil {
				return r.closeDone
			}
		default:
			return r.closeDone
		}
	}
	r.closeDone = make(chan struct{})
	go r.finishClose()
	return r.closeDone
}

func (r *nodeReference) finishClose() {
	err := r.retire()
	if err == nil {
		r.operations.Wait()
		ctx, cancel := r.session.operationContext(r.session.cleanup)
		err = r.native.Close(ctx)
		cancel()
	}
	if err == nil {
		r.session.mu.Lock()
		delete(r.session.files, r)
		r.session.mu.Unlock()
		r.session.storage.sweepAfterMutation()
	}
	r.closeMu.Lock()
	r.closeErr = err
	close(r.closeDone)
	r.closeMu.Unlock()
}

func (r *nodeReference) Close(ctx context.Context) error {
	done := r.startClose()
	select {
	case <-done:
		r.closeMu.Lock()
		defer r.closeMu.Unlock()
		return r.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
