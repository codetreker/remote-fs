package locking

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/codetreker/remote-fs/packages/storage"
)

func bindingFrom(ctx context.Context) (storage.AccessBinding, error) {
	binding, present := storage.AccessBindingFrom(ctx)
	if !present {
		return storage.AccessBinding{}, nil
	}
	if err := binding.Check(); err != nil {
		return storage.AccessBinding{}, Wrap(Denied, "invalid access binding", err)
	}
	return binding, nil
}

func requireBinding(ctx context.Context, binding storage.AccessBinding) error {
	if err := storage.RequireAccessBinding(ctx, binding); err != nil {
		return Wrap(Denied, "capability belongs to another access context", err)
	}
	return nil
}

func bindingDigest(binding storage.AccessBinding) string {
	if binding == (storage.AccessBinding{}) {
		return ""
	}
	// Length framing keeps distinct pairs independent of identifier contents.
	value := strconv.Itoa(len(binding.SubjectID)) + ":" + binding.SubjectID + binding.AccessID
	digest := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func requireBindingDigest(ctx context.Context, expected string) error {
	binding, err := bindingFrom(ctx)
	if err != nil {
		return err
	}
	if bindingDigest(binding) != expected {
		return fail(Denied, "capability belongs to another access context")
	}
	return nil
}

func (a *Authority) verifySessionBinding(ctx context.Context, id SessionID) (string, error) {
	extra, err := a.verify(string(id), "s", "")
	if err != nil {
		if CodeOf(err) == Retired {
			// Cleanup can acknowledge retired incarnations without their live
			// records. Their claimed binding still cannot authorize another
			// access context to receive that terminal acknowledgment.
			parts := strings.Split(string(id), ".")
			if bindingErr := requireBindingDigest(ctx, parts[3]); bindingErr != nil {
				return "", bindingErr
			}
		}
		return "", err
	}
	return extra, requireBindingDigest(ctx, extra)
}

func ticketBinding(extra string) (expiry, binding string) {
	expiry, binding, _ = strings.Cut(extra, ":")
	return expiry, binding
}
