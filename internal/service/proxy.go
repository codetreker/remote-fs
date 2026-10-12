package service

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Only configured immediate peers can supply an original client address.
// The chain is read from the trusted end; an untrusted hop cannot add authority.
func proxyClientRequest(r *http.Request, configured []string) (*http.Request, error) {
	if len(r.Header.Values("X-Forwarded-For")) == 0 {
		return r, nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return nil, err
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return nil, err
	}
	trusted := func(address netip.Addr) bool {
		for _, value := range configured {
			prefix, parseErr := netip.ParsePrefix(value)
			if parseErr == nil && prefix.Contains(address.Unmap()) {
				return true
			}
		}
		return false
	}
	if !trusted(peer) {
		return r, nil
	}
	values := r.Header.Values("X-Forwarded-For")
	if len(values) != 1 || len(values[0]) > 4096 {
		return nil, errors.New("invalid forwarded client address")
	}
	chain := strings.Split(values[0], ",")
	if len(chain) > 32 {
		return nil, errors.New("forwarded client chain exceeds its bound")
	}
	for i := len(chain) - 1; i >= 0 && trusted(peer); i-- {
		address, parseErr := netip.ParseAddr(strings.TrimSpace(chain[i]))
		if parseErr != nil {
			return nil, errors.New("invalid forwarded client address")
		}
		peer = address
	}
	copy := r.Clone(r.Context())
	copy.RemoteAddr = net.JoinHostPort(peer.String(), "0")
	return copy, nil
}
