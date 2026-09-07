package main

import (
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

type capacityFlag struct {
	name  string
	value *int
	help  string
}

func lockCapacityFlags(options *locking.Options) []capacityFlag {
	return []capacityFlag{
		{"lock-max-sessions", &options.MaxSessions, "maximum live lock sessions"},
		{"lock-max-tickets", &options.MaxTickets, "maximum retained enrollment consumptions"},
		{"lock-max-owners", &options.MaxOwners, "maximum live lock owners"},
		{"lock-max-resources", &options.MaxResources, "maximum retained file-lock resources"},
		{"lock-max-actions", &options.MaxActions, "maximum retained lock actions"},
		{"lock-max-grants", &options.MaxGrants, "maximum active file-lock grants"},
		{"lock-max-queued", &options.MaxQueued, "maximum queued lock acquisitions"},
		{"lock-owners-per-session", &options.OwnersPerSession, "maximum live owners in one lock session"},
		{"lock-owner-actions-per-session", &options.OwnerActionsPerSession, "maximum retained owner-creation actions in one session"},
		{"lock-actions-per-owner", &options.ActionsPerOwner, "maximum retained actions for one lock owner"},
		{"lock-grants-per-owner", &options.GrantsPerOwner, "maximum active grants for one lock owner"},
		{"lock-queued-per-owner", &options.QueuedPerOwner, "maximum queued acquisitions for one lock owner"},
		{"lock-queued-per-resource", &options.QueuedPerResource, "maximum queued acquisitions for one file"},
		{"lock-max-proofs", &options.MaxProofs, "maximum grant proofs in one explicit mutation scope"},
		{"lock-max-request-bytes", &options.MaxRequestBytes, "maximum bytes in one lock-management request identifier"},
	}
}

type lockDurationFlag struct {
	name  string
	value *time.Duration
	help  string
}

func lockDurationFlags(options *locking.Options) []lockDurationFlag {
	return []lockDurationFlag{
		{"lock-max-lease", &options.MaxLease, "maximum lease duration; reducing it does not shorten restart recovery"},
		{"lock-max-wait", &options.MaxWait, "maximum wait for one lock acquisition"},
		{"lock-ticket-ttl", &options.TicketTTL, "validity interval of an enrollment ticket"},
		{"lock-session-idle", &options.SessionIdle, "idle lifetime of a lock session; must cover maximum lease and wait"},
		{"lock-resource-ttl", &options.ResourceTTL, "validity interval of a resolved file resource"},
	}
}

func bindLockOptions(flags *flag.FlagSet, options *locking.Options) {
	for _, option := range lockCapacityFlags(options) {
		flags.IntVar(option.value, option.name, *option.value, option.help)
	}
	for _, option := range lockDurationFlags(options) {
		flags.DurationVar(option.value, option.name, *option.value, option.help)
	}
}

func validateLockOptions(options locking.Options) error {
	for _, option := range lockCapacityFlags(&options) {
		if *option.value <= 0 || *option.value == math.MaxInt {
			return fmt.Errorf("-%s must be positive and below the largest integer", option.name)
		}
	}
	for _, option := range lockDurationFlags(&options) {
		if *option.value < time.Millisecond {
			return fmt.Errorf("-%s must be at least 1ms", option.name)
		}
	}
	if options.SessionIdle < options.MaxLease || options.SessionIdle < options.MaxWait {
		return fmt.Errorf("-lock-session-idle must cover both -lock-max-lease and -lock-max-wait")
	}
	return options.Validate()
}
