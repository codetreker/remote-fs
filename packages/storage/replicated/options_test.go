package replicated_test

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage/replicated"
)

func TestConfirmationOptionsRejectUnboundedOrImpossibleValues(t *testing.T) {
	valid := replicated.DefaultOptions()
	tests := []struct {
		name   string
		change func(*replicated.Options)
	}{
		{"zero replay timeout", func(options *replicated.Options) { options.ReplayTimeout = 0 }},
		{"negative replay timeout", func(options *replicated.Options) { options.ReplayTimeout = -time.Second }},
		{"zero grace", func(options *replicated.Options) { options.ConfirmationGrace = 0 }},
		{"negative grace", func(options *replicated.Options) { options.ConfirmationGrace = -time.Second }},
		{"zero active confirmations", func(options *replicated.Options) { options.MaxActiveConfirmations = 0 }},
		{"unbounded active confirmations", func(options *replicated.Options) { options.MaxActiveConfirmations = math.MaxInt }},
		{"negative waiters", func(options *replicated.Options) { options.MaxWaitingConfirmations = -1 }},
		{"unbounded waiters", func(options *replicated.Options) { options.MaxWaitingConfirmations = math.MaxInt }},
		{"negative file sessions", func(options *replicated.Options) { options.MaxFileSessions = -1 }},
		{"unbounded file sessions", func(options *replicated.Options) { options.MaxFileSessions = math.MaxInt }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.change(&options)
			if err := options.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Check returned %v, want EINVAL", err)
			}
			if _, err := replicated.NewWithOptions(context.Background(), nil, nil, options); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("NewWithOptions returned %v, want EINVAL before using its dependencies", err)
			}
		})
	}
}

func TestDefaultConfirmationOptionsAreFinite(t *testing.T) {
	options := replicated.DefaultOptions()
	if err := options.Check(); err != nil {
		t.Fatalf("default options are invalid: %v", err)
	}
	if options != (replicated.Options{
		ConfirmationGrace:       replicated.DefaultConfirmationGrace,
		ReplayTimeout:           replicated.DefaultReplayTimeout,
		MaxActiveConfirmations:  replicated.DefaultMaxActiveConfirmations,
		MaxWaitingConfirmations: replicated.DefaultMaxWaitingConfirmations,
		MaxFileSessions:         replicated.DefaultMaxFileSessions,
	}) {
		t.Fatalf("default options are %+v and do not match their exported defaults", options)
	}
}

func TestConfirmationGraceCompatibilityConstructorValidatesBeforeDependencies(t *testing.T) {
	if _, err := replicated.NewWithConfirmationGrace(context.Background(), nil, nil, 0); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("NewWithConfirmationGrace returned %v, want EINVAL", err)
	}
}
