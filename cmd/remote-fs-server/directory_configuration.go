package main

import (
	"flag"
	"fmt"
	"math"
	"strconv"

	"github.com/codetreker/remote-fs/packages/storage/localdir"
)

func directoryCapacityFlags(limits *localdir.Limits) []capacityFlag {
	return []capacityFlag{
		{"dir-max-operations", &limits.MaxOperations, "maximum native directory operations admitted at once"},
		{"dir-max-waiters", &limits.MaxWaiters, "maximum native directory operations waiting for admission"},
		{"dir-max-pinned-targets", &limits.MaxPinnedTargets, "maximum file objects retained for logical directory resources"},
		{"dir-max-snapshot-entries", &limits.MaxSnapshotEntries, "maximum entries in one captured directory listing"},
		{"dir-max-recovery-entries", &limits.MaxRecoveryEntries, "maximum staging records examined while opening directory state"},
		{"dir-max-path-bytes", &limits.MaxPathBytes, "maximum bytes in a directory namespace path"},
	}
}

func bindDirectoryLimits(flags *flag.FlagSet, limits *localdir.Limits) {
	for _, option := range directoryCapacityFlags(limits) {
		flags.IntVar(option.value, option.name, *option.value, option.help)
	}
	flags.Var((*directorySizeFlag)(&limits.MaxStagingBytes), "dir-max-staging-bytes", "maximum aggregate bytes in staged directory writes, as SIZE")
	flags.Var((*directorySizeFlag)(&limits.MaxSnapshotBytes), "dir-max-snapshot-bytes", "maximum bytes in one captured directory listing, as SIZE")
}

type directorySizeFlag int64

func (f *directorySizeFlag) String() string {
	return strconv.FormatInt(int64(*f), 10)
}

func (f *directorySizeFlag) Set(value string) error {
	parsed := positiveSizeFlag{}
	if err := parsed.Set(value); err != nil {
		return err
	}
	*f = directorySizeFlag(parsed.bytes)
	return nil
}

func validateDirectoryLimits(config commandConfig, given map[string]bool) error {
	limits := config.directoryLimits
	for _, option := range directoryCapacityFlags(&limits) {
		if given[option.name] && config.directory == "" {
			return fmt.Errorf("-%s requires -dir", option.name)
		}
		if *option.value <= 0 || *option.value == math.MaxInt {
			return fmt.Errorf("-%s must be positive and below the largest integer", option.name)
		}
	}
	for _, name := range []string{"dir-max-staging-bytes", "dir-max-snapshot-bytes"} {
		if given[name] && config.directory == "" {
			return fmt.Errorf("-%s requires -dir", name)
		}
	}
	if _, err := limits.Effective(); err != nil {
		return err
	}
	if config.directory != "" {
		maxWrite := config.http.MaxWriteBytes
		if maxWrite == 0 {
			maxWrite = config.http.MaxBodyBytes
		}
		if maxWrite > limits.MaxStagingBytes {
			return fmt.Errorf("-http-max-write-bytes is %d, above -dir-max-staging-bytes at %d", maxWrite, limits.MaxStagingBytes)
		}
	}
	return nil
}
