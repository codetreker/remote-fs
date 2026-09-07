package httprest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	HeaderMutationScope    = "Remote-Fs-Mutation-Scope"
	MaxLockProofs          = 16
	MaxLockCapabilityBytes = 512
	MaxMutationScopeBytes  = 16 << 10
)

func isNamespaceMutation(op Op) bool {
	switch op {
	case OpSetAttr, OpWrite, OpCreate, OpMkdir, OpRemove, OpRemoveDir, OpRename:
		return true
	default:
		return false
	}
}

func validateWireScope(scope locking.MutationScope) error {
	if err := locking.ValidateScope(scope, MaxLockProofs); err != nil {
		return err
	}
	if scope.Owner.Session == "" || scope.Owner.Owner == "" {
		return errors.New("mutation scope requires a complete owner")
	}
	if len(scope.Owner.Session) > MaxLockCapabilityBytes || len(scope.Owner.Owner) > MaxLockCapabilityBytes {
		return errors.New("mutation owner exceeds the capability length bound")
	}
	for _, grant := range scope.Grants {
		if len(grant.ID) > MaxLockCapabilityBytes || len(grant.Resource) > MaxLockCapabilityBytes {
			return errors.New("mutation proof exceeds the capability length bound")
		}
	}
	return nil
}

func encodeMutationScope(scope locking.MutationScope) (string, error) {
	if err := validateWireScope(scope); err != nil {
		return "", locking.Wrap(locking.Invalid, "invalid mutation scope", err)
	}
	if scope.Grants == nil {
		scope.Grants = []locking.GrantRef{}
	}
	body, err := marshalLockJSON(scope)
	if err != nil {
		return "", locking.Wrap(locking.Invalid, "invalid mutation scope encoding", err)
	}
	if len(body) > MaxMutationScopeBytes {
		return "", locking.Wrap(locking.Invalid, "mutation scope exceeds the wire bound", nil)
	}
	return base64.RawURLEncoding.EncodeToString(body), nil
}

func requestMutationScope(r *http.Request, op Op) (locking.MutationScope, bool, error) {
	values := r.Header.Values(HeaderMutationScope)
	if len(values) == 0 {
		return locking.MutationScope{}, false, nil
	}
	if !isNamespaceMutation(op) {
		return locking.MutationScope{}, false, errors.New("mutation scope is only valid on namespace mutations")
	}
	if len(values) != 1 || values[0] == "" || len(values[0]) > base64.RawURLEncoding.EncodedLen(MaxMutationScopeBytes) {
		return locking.MutationScope{}, false, errors.New("mutation scope header violates its single-value size bound")
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(values[0])
	if err != nil {
		return locking.MutationScope{}, false, errors.New("mutation scope is not unpadded base64url")
	}
	var scope locking.MutationScope
	if err := decodeLockJSON(body, &scope); err != nil {
		return locking.MutationScope{}, false, errors.New("mutation scope is not a complete scope object")
	}
	if err := validateWireScope(scope); err != nil {
		return locking.MutationScope{}, false, errors.New("mutation scope violates its owner or proof bounds")
	}
	return scope, true, nil
}

// WithScope retains an immutable proof set for mutations. Reads remain ordinary snapshot
// reads, and every mutation including a no-op carries the proofs to final publication.
func (s *Storage) WithScope(scope locking.MutationScope) (*Storage, error) {
	if scope.Owner != (locking.OwnerRef{}) || len(scope.Grants) != 0 {
		if _, err := encodeMutationScope(scope); err != nil {
			return nil, err
		}
	}
	copied := *s
	frozen := locking.CloneScope(scope)
	copied.scope = &frozen
	return &copied, nil
}

func (s *Storage) Scope(scope locking.MutationScope) (storage.BoundedStorage, error) {
	return s.WithScope(scope)
}

func (s *Storage) LockService() locking.Service { return s }

// Namespace diagnostics use the namespace response reservation, including long pathname
// wrappers. Their typed outcome fields obey the same strict shape as lock controls.
func decodeNamespaceLockFailure(body []byte) (*locking.Error, error) {
	var response struct {
		Errno    string       `json:"errno"`
		Message  string       `json:"message"`
		LockCode locking.Code `json:"lockCode"`
		Recorded bool         `json:"recorded"`
	}
	if !utf8.Valid(body) {
		return nil, errors.New("invalid lock error response encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := checkLockJSON(decoder, reflect.TypeOf(response)); err != nil {
		return nil, errors.New("invalid lock error response")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing lock error response content")
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, errors.New("invalid lock error response")
	}
	errno, ok := storage.ErrnoByName(response.Errno)
	if !ok || !validLockCode(response.LockCode) || errno != locking.Errno(response.LockCode) {
		return nil, errors.New("inconsistent lock error classification")
	}
	return &locking.Error{Code: response.LockCode, Recorded: response.Recorded, Message: response.Message}, nil
}
