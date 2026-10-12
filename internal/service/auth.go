package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
	"golang.org/x/crypto/argon2"
)

const (
	authCookieName     = "__Host-rfs_session"
	credentialLifetime = 90 * 24 * time.Hour
)

type AuthConfig struct {
	Origin              string
	VerificationWait    time.Duration
	LoginRateWindow     time.Duration
	LoginAttempts       int
	LoginBuckets        int
	OutputCheckInterval time.Duration
	OutputWriteTimeout  time.Duration
	MaxOutputContexts   int
	MaxOutputsPerAccess int
}

// DefaultAuthConfig supplies the documented, bounded service authentication policy.
func DefaultAuthConfig(origin string) AuthConfig {
	return AuthConfig{Origin: origin, VerificationWait: 5 * time.Second, LoginRateWindow: 15 * time.Minute, LoginAttempts: 20, LoginBuckets: 4096, OutputCheckInterval: 250 * time.Millisecond, OutputWriteTimeout: 5 * time.Second, MaxOutputContexts: 1024, MaxOutputsPerAccess: 32}
}

type Authenticator struct {
	store       *management.Store
	config      AuthConfig
	now         func() time.Time
	passwords   passwordAdmission
	logins      loginLimits
	dummyHash   string
	outputMu    sync.Mutex
	outputs     map[management.ID]map[uint64]context.CancelFunc
	nextOutput  uint64
	outputCount int
	outputWG    sync.WaitGroup
	closed      bool
}

func NewAuthenticator(store *management.Store, config AuthConfig) (*Authenticator, error) {
	if store == nil {
		return nil, errors.New("authentication requires management store")
	}
	origin, err := url.Parse(config.Origin)
	if err != nil || origin.Scheme != "https" || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || origin.Path != "" || origin.Opaque != "" {
		return nil, errors.New("authentication origin must be an exact HTTPS origin without a path")
	}
	if config.VerificationWait <= 0 || config.LoginRateWindow <= 0 || config.LoginAttempts < 1 || config.LoginBuckets < 2 || config.OutputCheckInterval <= 0 || config.OutputWriteTimeout <= 0 || config.MaxOutputContexts < 1 || config.MaxOutputsPerAccess < 1 || config.MaxOutputsPerAccess > config.MaxOutputContexts {
		return nil, errors.New("authentication limits must be positive")
	}
	dummyPassword, err := randomAuthSecret()
	if err != nil {
		return nil, err
	}
	dummyHash, err := HashAdminPassword(dummyPassword)
	if err != nil {
		return nil, err
	}
	return &Authenticator{store: store, config: config, now: time.Now, passwords: newPasswordAdmission(config.VerificationWait), logins: loginLimits{buckets: make(map[[32]byte]loginBucket), maxBuckets: config.LoginBuckets, maxAttempts: config.LoginAttempts, window: config.LoginRateWindow}, dummyHash: dummyHash, outputs: make(map[management.ID]map[uint64]context.CancelFunc)}, nil
}

// HashAdminPassword produces the fixed Argon2id policy used by offline admin commands.
func HashAdminPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > 1024 {
		return "", errors.New("administrator password must contain 12 to 1024 bytes")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	digest := argon2.IDKey([]byte(password), salt, 3, 64*1024, 1, 32)
	return "$argon2id$v=19$m=65536,t=3,p=1$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(digest), nil
}

func verifyAdminPassword(password, encoded string) (bool, error) {
	if len(password) > 1024 {
		return false, nil
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" || parts[3] != "m=65536,t=3,p=1" {
		return false, errors.New("administrator password hash has unsupported Argon2id parameters")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) != 16 {
		return false, errors.New("administrator password hash has invalid salt")
	}
	expected, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(expected) != 32 {
		return false, errors.New("administrator password hash has invalid digest")
	}
	actual := argon2.IDKey([]byte(password), salt, 3, 64*1024, 1, 32)
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func randomAuthSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func authSecretDigest(secret string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != secret {
		return nil, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(secret))
	return digest[:], nil
}

func validLoginName(name string) bool {
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > 128 {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (a *Authenticator) verifyLogin(ctx context.Context, r *http.Request, login, password string) (management.User, error) {
	if err := a.logins.admit(r, login, a.now()); err != nil {
		return management.User{}, err
	}
	release, err := a.passwords.acquire(ctx)
	if err != nil {
		return management.User{}, err
	}
	defer release()
	credential, err := a.store.AdminCredential(ctx, login)
	if err != nil && !errors.Is(err, management.ErrNotFound) {
		return management.User{}, err
	}
	hash := a.dummyHash
	found := err == nil && validLoginName(login)
	if found {
		hash = credential.PasswordHash
	}
	match, err := verifyAdminPassword(password, hash)
	if err != nil {
		return management.User{}, err
	}
	if !match || !found {
		return management.User{}, ErrUnauthenticated
	}
	current, err := a.store.GetUser(ctx, credential.User.ID)
	if err != nil {
		return management.User{}, err
	}
	if current.AuthEpoch != credential.User.AuthEpoch || current.Role != management.RoleAdmin {
		return management.User{}, ErrUnauthenticated
	}
	return current, nil
}

func authStateError(err error) error {
	if errors.Is(err, management.ErrNotFound) || errors.Is(err, management.ErrDenied) || errors.Is(err, management.ErrExpired) {
		return ErrUnauthenticated
	}
	return err
}

// Authorizer reads persistent ownership on every semantic operation. An admin
// role confers no permission over another user's files.
func (a *Authenticator) Authorizer(volumeID management.ID) authz.Authorizer {
	return authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		p, ok := CurrentPrincipal(ctx)
		if !ok {
			return authz.ErrDenied
		}
		if err := a.CheckFileAccess(ctx, p, volumeID); err != nil {
			if errors.Is(err, ErrUnauthenticated) || errors.Is(err, ErrForbidden) {
				return fmt.Errorf("%w: %w", authz.ErrDenied, err)
			}
			return err
		}
		return nil
	})
}

// CheckVolumeOwner checks current authentication and immutable ownership.
// Callers additionally enforce the operation scope and selected volume IDs.
func (a *Authenticator) CheckVolumeOwner(ctx context.Context, p Principal, volumeID management.ID) error {
	if err := a.CheckCurrent(ctx, p); err != nil {
		return err
	}
	volume, err := a.store.GetVolume(ctx, p.UserID, volumeID)
	if errors.Is(err, management.ErrDenied) {
		return ErrForbidden
	}
	if err != nil {
		return err
	}
	if volume.OwnerUserID != p.UserID {
		return ErrForbidden
	}
	return nil
}

// CheckFileAccess must run before opening or allocating a volume runtime.
func (a *Authenticator) CheckFileAccess(ctx context.Context, p Principal, volumeID management.ID) error {
	if !p.HasScope(ScopeFiles) || !p.AllowsVolume(volumeID) {
		return ErrForbidden
	}
	return a.CheckVolumeOwner(ctx, p, volumeID)
}

func (a *Authenticator) RequireScope(ctx context.Context, p Principal, scope string) error {
	if err := a.CheckCurrent(ctx, p); err != nil {
		return err
	}
	if !p.HasScope(scope) {
		return ErrForbidden
	}
	return nil
}

// AccessBinding adapts the authenticated service identity to core capability ownership.
func (a *Authenticator) AccessBinding(ctx context.Context) (storage.AccessBinding, error) {
	p, ok := CurrentPrincipal(ctx)
	if !ok {
		return storage.AccessBinding{}, fmt.Errorf("%w: %w", authz.ErrDenied, ErrUnauthenticated)
	}
	if err := a.CheckCurrent(ctx, p); err != nil {
		if errors.Is(err, ErrUnauthenticated) || errors.Is(err, ErrForbidden) {
			return storage.AccessBinding{}, fmt.Errorf("%w: %w", authz.ErrDenied, err)
		}
		return storage.AccessBinding{}, err
	}
	binding := storage.AccessBinding{SubjectID: string(p.UserID), AccessID: string(p.AuthenticationID)}
	return binding, binding.Check()
}

func parseBearer(header string) (management.ID, string, error) {
	fields := strings.Split(header, " ")
	if len(fields) != 2 || fields[0] != "Bearer" {
		return "", "", ErrUnauthenticated
	}
	parts := strings.Split(fields[1], ".")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return "", "", ErrUnauthenticated
	}
	if _, err := strconv.ParseUint(parts[0][:16], 16, 64); err != nil {
		return "", "", ErrUnauthenticated
	}
	if _, err := strconv.ParseUint(parts[0][16:], 16, 64); err != nil {
		return "", "", ErrUnauthenticated
	}
	if _, err := authSecretDigest(parts[1]); err != nil {
		return "", "", err
	}
	return management.ID(parts[0]), parts[1], nil
}
