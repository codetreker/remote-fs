package service

import (
	"context"
	"errors"

	"github.com/codetreker/remote-fs/internal/management"
)

var (
	ErrUnauthenticated = errors.New("authentication required")
	ErrForbidden       = errors.New("access forbidden")
	ErrAuthCapacity    = errors.New("authentication capacity exhausted")
	ErrAuthClosed      = errors.New("authenticator is closed")
)

const (
	ScopeVolumesRead    = "volumes:read"
	ScopeVolumesWrite   = "volumes:write"
	ScopeFiles          = "files"
	ScopeAdminSummaries = "admin:summaries"
)

// Principal is supplied by the service after checking current persistent identity.
// AuthenticationID remains unchanged when a CLI credential secret is rotated.
type Principal struct {
	UserID           management.ID
	AuthenticationID management.ID
	Role             management.UserRole
	DisplayName      string
	AuthEpoch        int64
	Kind             string
	Scopes           []string
	AllOwnedVolumes  bool
	VolumeIDs        []management.ID
}

type principalContextKey struct{}

// CurrentPrincipal returns an identity copy; current revocation and expiration
// must be checked by the authentication or authorization operation using it.
func CurrentPrincipal(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey{}).(Principal)
	p.Scopes = append([]string(nil), p.Scopes...)
	p.VolumeIDs = append([]management.ID(nil), p.VolumeIDs...)
	return p, ok
}

// WithPrincipal is reserved for authenticated service orchestration; request
// parameters and headers must never be used to construct its identity.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	p.Scopes = append([]string(nil), p.Scopes...)
	p.VolumeIDs = append([]management.ID(nil), p.VolumeIDs...)
	return context.WithValue(ctx, principalContextKey{}, p)
}

func (p Principal) HasScope(scope string) bool {
	if p.Kind == "browser" {
		return true
	}
	for _, candidate := range p.Scopes {
		if candidate == scope {
			return true
		}
	}
	return false
}

func (p Principal) AllowsVolume(id management.ID) bool {
	if p.Kind == "browser" || p.AllOwnedVolumes {
		return true
	}
	for _, candidate := range p.VolumeIDs {
		if candidate == id {
			return true
		}
	}
	return false
}
