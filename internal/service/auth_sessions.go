package service

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
)

func principalFromUser(user management.User, id management.ID, kind string) Principal {
	return Principal{UserID: user.ID, AuthenticationID: id, Role: user.Role, DisplayName: user.DisplayName, AuthEpoch: user.AuthEpoch, Kind: kind}
}

func principalFromCredential(user management.User, credential management.Credential) Principal {
	p := principalFromUser(user, credential.ID, "cli")
	p.Scopes = append([]string(nil), credential.Scopes...)
	p.AllOwnedVolumes = credential.AllOwnedVolumes
	p.VolumeIDs = append([]management.ID(nil), credential.VolumeIDs...)
	return p
}

// Authenticate checks current persistent authentication and browser mutation
// protections. It rejects duplicate cookies, duplicate Authorization fields and
// requests carrying both a session cookie and a bearer credential.
func (a *Authenticator) Authenticate(ctx context.Context, r *http.Request) (Principal, error) {
	headers := r.Header.Values("Authorization")
	cookies := r.CookiesNamed(authCookieName)
	if len(headers) > 1 || len(cookies) > 1 || (len(headers) > 0 && len(cookies) > 0) {
		return Principal{}, ErrUnauthenticated
	}
	if len(headers) == 1 {
		id, secret, err := parseBearer(headers[0])
		if err != nil {
			return Principal{}, err
		}
		digest, err := authSecretDigest(secret)
		if err != nil {
			return Principal{}, err
		}
		credential, err := a.store.ValidateCredential(ctx, id, digest)
		if err != nil {
			return Principal{}, authStateError(err)
		}
		user, err := a.store.GetUser(ctx, credential.UserID)
		if err != nil {
			return Principal{}, authStateError(err)
		}
		if user.AuthEpoch != credential.AuthEpoch {
			return Principal{}, ErrUnauthenticated
		}
		return principalFromCredential(user, credential), nil
	}
	if len(cookies) == 0 {
		return Principal{}, ErrUnauthenticated
	}
	id, secret, err := parseBearer("Bearer " + cookies[0].Value)
	if err != nil {
		return Principal{}, err
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		return Principal{}, err
	}
	session, err := a.store.ValidateSession(ctx, id, digest, true)
	if err != nil {
		return Principal{}, authStateError(err)
	}
	if r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
		if err := a.checkOrigin(r); err != nil {
			return Principal{}, err
		}
		tokens := r.Header.Values("X-CSRF-Token")
		if len(tokens) != 1 || !equalAuthValue(tokens[0], session.CSRFToken) {
			return Principal{}, ErrForbidden
		}
	}
	user, err := a.store.GetUser(ctx, session.UserID)
	if err != nil {
		return Principal{}, authStateError(err)
	}
	if user.AuthEpoch != session.AuthEpoch {
		return Principal{}, ErrUnauthenticated
	}
	return principalFromUser(user, session.ID, "browser"), nil
}

func (a *Authenticator) CheckCurrent(ctx context.Context, p Principal) error {
	if p.UserID == "" || p.AuthenticationID == "" || p.AuthEpoch < 1 {
		return ErrUnauthenticated
	}
	now := a.now()
	switch p.Kind {
	case "browser":
		session, err := a.store.GetSession(ctx, p.AuthenticationID)
		if err != nil {
			return authStateError(err)
		}
		if session.UserID != p.UserID || session.AuthEpoch != p.AuthEpoch || session.RevokedAt != nil || !now.Before(session.IdleExpiresAt) || !now.Before(session.AbsoluteExpiresAt) {
			return ErrUnauthenticated
		}
	case "cli":
		credential, err := a.store.GetCredential(ctx, p.AuthenticationID)
		if err != nil {
			return authStateError(err)
		}
		if credential.UserID != p.UserID || credential.AuthEpoch != p.AuthEpoch || credential.RevokedAt != nil || !now.Before(credential.ExpiresAt) {
			return ErrUnauthenticated
		}
	default:
		return ErrUnauthenticated
	}
	user, err := a.store.GetUser(ctx, p.UserID)
	if err != nil {
		return authStateError(err)
	}
	if user.AuthEpoch != p.AuthEpoch || user.Role != p.Role {
		return ErrUnauthenticated
	}
	return nil
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := a.Authenticate(r.Context(), r)
		if err != nil {
			writeAuthError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
	})
}

func (a *Authenticator) checkOrigin(r *http.Request) error {
	origins := r.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != a.config.Origin {
		return ErrForbidden
	}
	return nil
}

// OutputContext bounds active output scopes and cancels them after logout,
// revocation, expiry, password recovery, or an authentication-store failure.
// Each output unit must additionally call CheckCurrent before writing.
func (a *Authenticator) OutputContext(ctx context.Context, p Principal) (context.Context, context.CancelFunc, error) {
	if err := a.CheckCurrent(ctx, p); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	a.outputMu.Lock()
	owned := a.outputs[p.AuthenticationID]
	if a.closed {
		a.outputMu.Unlock()
		cancel()
		return nil, nil, ErrAuthClosed
	}
	if a.outputCount >= a.config.MaxOutputContexts || len(owned) >= a.config.MaxOutputsPerAccess || a.nextOutput == ^uint64(0) {
		a.outputMu.Unlock()
		cancel()
		return nil, nil, ErrAuthCapacity
	}
	if owned == nil {
		owned = make(map[uint64]context.CancelFunc)
		a.outputs[p.AuthenticationID] = owned
	}
	a.nextOutput++
	id := a.nextOutput
	owned[id] = cancel
	a.outputCount++
	a.outputWG.Add(1)
	a.outputMu.Unlock()
	done := func() {
		cancel()
		a.outputMu.Lock()
		if owned := a.outputs[p.AuthenticationID]; owned != nil {
			if _, exists := owned[id]; exists {
				delete(owned, id)
				a.outputCount--
			}
			if len(owned) == 0 {
				delete(a.outputs, p.AuthenticationID)
			}
		}
		a.outputMu.Unlock()
	}
	go func() {
		defer a.outputWG.Done()
		defer done()
		ticker := time.NewTicker(a.config.OutputCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.CheckCurrent(ctx, p); err != nil {
					return
				}
			}
		}
	}()
	return WithPrincipal(ctx, p), done, nil
}

func (a *Authenticator) cancelAccess(id management.ID) {
	a.outputMu.Lock()
	defer a.outputMu.Unlock()
	for _, cancel := range a.outputs[id] {
		cancel()
	}
}

// Close fences new output scopes, cancels existing scopes and waits for their
// pollers to finish. It does not own or close the management store.
func (a *Authenticator) Close() error {
	a.outputMu.Lock()
	a.closed = true
	for _, outputs := range a.outputs {
		for _, cancel := range outputs {
			cancel()
		}
	}
	a.outputMu.Unlock()
	a.outputWG.Wait()
	return nil
}

// CheckOutput rechecks identity and applies a finite write deadline. A writer
// that cannot set deadlines fails explicitly before publishing another unit.
func (a *Authenticator) CheckOutput(ctx context.Context, w http.ResponseWriter) error {
	p, ok := CurrentPrincipal(ctx)
	if !ok {
		return ErrUnauthenticated
	}
	if err := a.CheckCurrent(ctx, p); err != nil {
		return err
	}
	if err := http.NewResponseController(w).SetWriteDeadline(a.now().Add(a.config.OutputWriteTimeout)); err != nil {
		return err
	}
	return nil
}

func (a *Authenticator) browserSession(ctx context.Context, r *http.Request) (Principal, management.Session, error) {
	p, err := a.Authenticate(ctx, r)
	if err != nil {
		return Principal{}, management.Session{}, err
	}
	if p.Kind != "browser" {
		return Principal{}, management.Session{}, ErrForbidden
	}
	session, err := a.store.GetSession(ctx, p.AuthenticationID)
	if err != nil {
		return Principal{}, management.Session{}, authStateError(err)
	}
	return p, session, nil
}

func authCookie(value string, expires time.Time) *http.Cookie {
	return &http.Cookie{Name: authCookieName, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, Expires: expires}
}

func clearAuthCookie(w http.ResponseWriter) {
	cookie := authCookie("", time.Unix(1, 0))
	cookie.MaxAge = -1
	http.SetCookie(w, cookie)
}

func constantHeader(r *http.Request, name, value string) bool {
	values := r.Header.Values(name)
	return len(values) == 1 && strings.EqualFold(values[0], value)
}
