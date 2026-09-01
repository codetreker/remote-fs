package replicated

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// What a mount remembers on behalf of the callers waiting for their own changes is the one
// thing here that grows with traffic rather than with the size of the tree, so it is the one
// R-INT-3 is about: anything that accumulates needs a ceiling, and a mount that is busy never
// has a moment with nothing in flight.
//
// This reaches inside the type rather than driving it through a mountpoint, because what is
// asserted is the size of something a caller cannot see. Everything about what it is for is
// tested from the outside; what is tested here is that it does not grow without bound.
func TestWhatIsRememberedForAWaiterIsDroppedWhenNoWaiterCouldUseIt(t *testing.T) {
	s := &Storage{notify: make(chan struct{}), touched: map[location]touch{}}

	// One caller starts, fifty names change, a second caller starts, fifty more change. The
	// second caller waits for something later than where it began, so nothing from the first
	// fifty can release it.
	first := s.expect()
	for i := range 50 {
		s.applied(changeAt(int64(i), metastore.Position(i+1)))
	}
	second := s.expect()
	for i := 50; i < 100; i++ {
		s.applied(changeAt(int64(i), metastore.Position(i+1)))
	}
	if held := len(s.touched); held != 100 {
		t.Fatalf("a hundred names changed while two callers were waiting and %d are remembered", held)
	}

	// The first caller leaves. Everything that only it could have been released by is of no
	// use to anybody now.
	s.forget(first)
	if held := len(s.touched); held != 50 {
		t.Fatalf("%d names are remembered after the caller that could have been released by half of them left, want 50", held)
	}
	for where, landed := range s.touched {
		if landed.filled <= second && landed.emptied <= second {
			t.Fatalf("%v is remembered at %+v, which is at or before where the one remaining caller began (%d)", where, landed, second)
		}
	}

	// The last one leaves and nothing is remembered at all.
	s.forget(second)
	if held := len(s.touched); held != 0 {
		t.Fatalf("%d names are still remembered with nobody waiting", held)
	}
}

// changeAt is one change to a name of its own.
func changeAt(name int64, at metastore.Position) metastore.Change {
	node := metastore.Node{ID: name + 1}
	return metastore.Change{
		Position: at,
		Kind:     metastore.Created,
		Parent:   1,
		Name:     []byte{byte(name), byte(name >> 8)},
		Node:     &node,
	}
}
