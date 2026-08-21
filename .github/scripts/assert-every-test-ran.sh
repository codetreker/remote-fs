#!/usr/bin/env bash
#
# Run one layer of the suite and fail unless every test in it actually ran.
#
# `go test` exits 0 when a test calls t.Skip, and still prints `ok` for the package.
# The layers that mount a filesystem skip themselves when /dev/fuse or fusermount is
# missing, so on such a machine a run that proved nothing is indistinguishable from a
# run that proved everything. Those layers are the only evidence the system works at
# all, which makes that particular green the most expensive lie available here.
#
# Three checks, because they catch different shapes of nothing-happened:
#
#   - a `skip` event for any test, which is a prerequisite the machine did not meet;
#   - a package pattern that matches no test at all, which is what a rename leaves
#     behind and which otherwise turns this whole check into a formality;
#   - a test the binaries report they contain that reached no verdict at all, which is
#     what a TestMain bailing out early, or a stray -run filter, looks like.
#
# Callers must pass -count=1. The test cache keys on environment variables but not on
# files outside the module, so a result recorded where /dev/fuse existed replays
# verbatim where it does not:
# https://github.com/golang/go/blob/go1.25.0/src/cmd/go/internal/test/test.go#L1962-L1970
#
# Usage: assert-every-test-ran.sh <go test flags and packages>

set -uo pipefail

run=$(mktemp) list=$(mktemp) expected=$(mktemp) report=$(mktemp)
trap 'rm -f "$run" "$list" "$expected" "$report"' EXIT

module=github.com/codetreker/remote-fs/

# jq reconstructs `go test -v` from the event stream, so the log reads normally and
# stays live: buffering it would hide which test wedged a mount.
go test -json "$@" | tee "$run" | jq -j --unbuffered 'select(.Action == "output") | .Output'
status=${PIPESTATUS[0]}

# -list builds each test binary and runs it with -test.list, executing no test bodies.
# The set it prints is exactly the set the run above had the opportunity to run, so the
# expectation is derived rather than maintained by hand.
go test -json -list '.*' "$@" >"$list" || status=1

jq -rs --arg module "$module" '
  [ .[] | select(has("Test")) ]
  | group_by(.Package + " " + .Test)
  | map(select(any(.[]; .Action == "skip")))
  | map({
      name: ((.[0].Package | ltrimstr($module)) + "  " + .[0].Test),
      why: ([ .[] | select(.Action == "output") | .Output
              | select(test("^\\s*(=== |--- )") | not)
              | sub("^\\s+"; "") | sub("\\s+$"; "") ] | join(" "))
    })
  | .[] | "  " + .name + "\n      " + .why
' "$run" >"$report"

if [ -s "$report" ]; then
  printf '\nFAIL: these tests did not run:\n\n'
  cat "$report"
  printf '\nA skipped test proves nothing. Every test named above must run here.\n'
  status=1
fi

jq -r --arg module "$module" \
  'select(.Action == "output" and (.Output | test("^Test\\S*\n$")))
   | (.Package | ltrimstr($module)) + "  " + (.Output | rtrimstr("\n"))' \
  "$list" | sort -u >"$expected"

if [ ! -s "$expected" ]; then
  printf '\nFAIL: no test exists in: %s\n' "$*"
  exit 1
fi

jq -r --arg module "$module" \
  'select(has("Test") and (.Action | test("^(pass|fail|skip)$")))
   | (.Package | ltrimstr($module)) + "  " + .Test' \
  "$run" | sort -u | comm -23 "$expected" - >"$report"

if [ -s "$report" ]; then
  printf '\nFAIL: these tests exist in the built binaries but reached no verdict:\n\n'
  cat "$report"
  status=1
fi

exit "$status"
