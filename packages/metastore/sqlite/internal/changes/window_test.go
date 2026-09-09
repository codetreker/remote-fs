package changes

import (
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestWindowDefaultsAndValidation(t *testing.T) {
	if got := DefaultWindow(); got != (Window{Floor: 100, Cap: 10000, Age: 10 * time.Minute}) {
		t.Fatalf("default retention = %+v", got)
	}
	for _, window := range []Window{DefaultWindow(), {Floor: 1, Cap: 1, Age: time.Nanosecond}} {
		if err := CheckWindow(window); err != nil {
			t.Fatalf("valid window %+v: %v", window, err)
		}
	}
	for _, window := range []Window{
		{Floor: 0, Cap: 1, Age: time.Second},
		{Floor: -1, Cap: 1, Age: time.Second},
		{Floor: 2, Cap: 1, Age: time.Second},
		{Floor: 1, Cap: 1},
		{Floor: 1, Cap: 1, Age: -time.Second},
	} {
		if err := CheckWindow(window); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("window %+v returned %v, want EINVAL", window, err)
		}
	}
}
