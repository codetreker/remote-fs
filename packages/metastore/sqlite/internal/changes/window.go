package changes

import (
	"fmt"
	"syscall"
	"time"
)

// Window bounds retained history. Floor takes precedence over Age; Cap is absolute.
type Window struct {
	// Floor is the fewest entries a namespace's log keeps, however old they are.
	Floor int

	// Cap is the most entries a namespace's log keeps.
	Cap int

	// Age is how long an entry is kept, subject to Floor.
	Age time.Duration
}

// DefaultWindow supplies the retention policy for callers without an explicit window.
func DefaultWindow() Window {
	return Window{Floor: 100, Cap: 10000, Age: 10 * time.Minute}
}

// CheckWindow rejects inconsistent or empty retention windows without changing them.
func CheckWindow(w Window) error {
	switch {
	case w.Floor < 1:
		return fmt.Errorf("a log keeping at least %d entries would have nothing to resume from: %w",
			w.Floor, syscall.EINVAL)
	case w.Cap < w.Floor:
		return fmt.Errorf("a log capped at %d entries cannot keep the %d it is required to: %w",
			w.Cap, w.Floor, syscall.EINVAL)
	case w.Age <= 0:
		return fmt.Errorf("entries older than %v is every entry there will ever be: %w",
			w.Age, syscall.EINVAL)
	}
	return nil
}
