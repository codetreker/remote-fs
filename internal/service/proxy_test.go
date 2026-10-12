package service

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTrustedProxyClientChainPreservesUntrustedIdentity(t *testing.T) {
	for _, test := range []struct {
		name, remote, chain, want string
		bad                       bool
	}{
		{"untrusted forged", "192.0.2.2:345", "198.51.100.1", "192.0.2.2:345", false},
		{"trusted client", "127.0.0.1:345", "198.51.100.1", "198.51.100.1:0", false},
		{"untrusted chain stops", "127.0.0.1:345", "203.0.113.1, 198.51.100.1", "198.51.100.1:0", false},
		{"trusted chain", "127.0.0.1:345", "203.0.113.1, 127.0.0.2", "203.0.113.1:0", false},
		{"malformed trusted", "127.0.0.1:345", "garbage", "", true},
		{"oversized chain", "127.0.0.1:345", strings.Repeat("127.0.0.2,", 33) + "203.0.113.1", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "https://files.example.test/", nil)
			request.RemoteAddr = test.remote
			request.Header.Set("X-Forwarded-For", test.chain)
			forwarded, err := proxyClientRequest(request, []string{"127.0.0.0/8"})
			if test.bad {
				if err == nil {
					t.Fatal("invalid trusted chain accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if forwarded.RemoteAddr != test.want {
				t.Fatalf("client=%s want=%s", forwarded.RemoteAddr, test.want)
			}
			if request.RemoteAddr != test.remote {
				t.Fatal("original request identity mutated")
			}
		})
	}
}
