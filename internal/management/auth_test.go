package management

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func authTestStore(t *testing.T, adjust func(*Options)) (*Store, string) {
	t.Helper()
	root, err := filepath.Abs("../../.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	options := DefaultOptions()
	options.Create = true
	if adjust != nil {
		adjust(&options)
	}
	path := filepath.Join(dir, "catalog.sqlite")
	store, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	store.now = func() time.Time { return time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC) }
	return store, path
}

func authTestAdmin(t *testing.T, s *Store, name string) User {
	t.Helper()
	user, err := s.AddAdmin(context.Background(), name, name, "encoded-hash")
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func authTestSession(t *testing.T, s *Store, user User, value byte) Session {
	t.Helper()
	session, err := s.CreateSession(context.Background(), SessionInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, SecretDigest: bytes.Repeat([]byte{value}, 32), CSRFToken: strings.Repeat("c", 43)})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func authTestCredential(t *testing.T, s *Store, user User, value byte) Credential {
	t.Helper()
	credential, err := s.CreateCredential(context.Background(), CredentialInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, Label: "mount", SecretDigest: bytes.Repeat([]byte{value}, 32), Scopes: []string{"files", "volumes:read"}, AllOwnedVolumes: true, ExpiresAt: s.now().Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return credential
}

func TestAuthBootstrapNamedAdminsAndReset(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	first, err := s.BootstrapAdmin(ctx, "first", "First administrator", "hash-one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BootstrapAdmin(ctx, "another", "Another", "hash"); !errors.Is(err, ErrConflict) {
		t.Fatalf("repeat bootstrap: %v", err)
	}
	second := authTestAdmin(t, s, "second")
	if first.ID == second.ID {
		t.Fatal("administrator identities reused")
	}
	if _, err := s.AddAdmin(ctx, "first", "Duplicate", "hash"); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate login: %v", err)
	}
	browser := authTestSession(t, s, first, 1)
	cli := authTestCredential(t, s, first, 2)
	otherBrowser := authTestSession(t, s, second, 3)
	user, err := s.ResetAdminPassword(ctx, first.ID, "hash-two")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != first.ID || user.AuthEpoch != first.AuthEpoch+1 {
		t.Fatalf("reset changed owner or failed to advance epoch: %+v", user)
	}
	record, err := s.AdminCredential(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	if record.PasswordHash != "hash-two" || record.PasswordVersion != 2 {
		t.Fatalf("reset record: %+v", record)
	}
	if _, err := s.ValidateSession(ctx, browser.ID, bytes.Repeat([]byte{1}, 32), false); !errors.Is(err, ErrDenied) {
		t.Fatalf("old browser survives reset: %v", err)
	}
	if _, err := s.ValidateCredential(ctx, cli.ID, bytes.Repeat([]byte{2}, 32)); !errors.Is(err, ErrDenied) {
		t.Fatalf("old CLI survives reset: %v", err)
	}
	if _, err := s.ValidateSession(ctx, otherBrowser.ID, bytes.Repeat([]byte{3}, 32), false); err != nil {
		t.Fatalf("unrelated administrator revoked: %v", err)
	}
	if _, err := s.CreateSession(ctx, SessionInput{UserID: first.ID, AuthEpoch: first.AuthEpoch, SecretDigest: bytes.Repeat([]byte{4}, 32), CSRFToken: strings.Repeat("x", 43)}); !errors.Is(err, ErrDenied) {
		t.Fatalf("old password verification minted new session: %v", err)
	}
	if _, err := s.CreateCredential(ctx, CredentialInput{UserID: first.ID, AuthEpoch: first.AuthEpoch, Label: "old password", SecretDigest: bytes.Repeat([]byte{5}, 32), Scopes: []string{"files"}, AllOwnedVolumes: true, ExpiresAt: s.now().Add(time.Hour)}); !errors.Is(err, ErrDenied) {
		t.Fatalf("old verification minted CLI credential: %v", err)
	}
	if _, err := s.AdminCredential(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown login: %v", err)
	}
}

func TestAuthSessionIdleAbsoluteTouchAndPersistence(t *testing.T) {
	ctx := context.Background()
	s, path := authTestStore(t, nil)
	user := authTestAdmin(t, s, "admin")
	session := authTestSession(t, s, user, 1)
	start := s.now()
	s.now = func() time.Time { return start.Add(30 * time.Second) }
	untouched, err := s.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), true)
	if err != nil {
		t.Fatal(err)
	}
	if !untouched.LastUsedAt.Equal(start) {
		t.Fatal("touch persisted more than once per minute")
	}
	s.now = func() time.Time { return start.Add(10 * time.Minute) }
	touched, err := s.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), true)
	if err != nil {
		t.Fatal(err)
	}
	if !touched.IdleExpiresAt.Equal(start.Add(40 * time.Minute)) {
		t.Fatalf("idle extension: %v", touched.IdleExpiresAt)
	}
	if _, err := s.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{2}, 32), false); !errors.Is(err, ErrDenied) {
		t.Fatalf("wrong digest accepted: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	reopened, err := Open(ctx, path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.now = func() time.Time { return start.Add(39 * time.Minute) }
	restored, err := reopened.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), false)
	if err != nil {
		t.Fatal(err)
	}
	if restored.CSRFToken != session.CSRFToken {
		t.Fatal("restart changed CSRF token")
	}
	reopened.now = func() time.Time { return start.Add(40 * time.Minute) }
	if _, err := reopened.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), false); !errors.Is(err, ErrExpired) {
		t.Fatalf("idle deadline accepted: %v", err)
	}
	// Last-minute interactive activity cannot move the absolute deadline.
	if _, err := reopened.write.ExecContext(ctx, "UPDATE sessions SET last_used_at=?,idle_expires_at=? WHERE id=?", unixMillis(start.Add(11*time.Hour+50*time.Minute)), unixMillis(start.Add(12*time.Hour)), session.ID); err != nil {
		t.Fatal(err)
	}
	reopened.now = func() time.Time { return start.Add(11*time.Hour + 59*time.Minute) }
	capped, err := reopened.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), true)
	if err != nil {
		t.Fatal(err)
	}
	if !capped.IdleExpiresAt.Equal(session.AbsoluteExpiresAt) {
		t.Fatal("touch exceeded absolute expiry")
	}
	reopened.now = func() time.Time { return session.AbsoluteExpiresAt }
	if _, err := reopened.ValidateSession(ctx, session.ID, bytes.Repeat([]byte{1}, 32), true); !errors.Is(err, ErrExpired) {
		t.Fatalf("absolute deadline accepted: %v", err)
	}
}

func TestAuthSessionCapacityAndOwnedRevocation(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, func(o *Options) { o.MaxSessions = 2; o.MaxSessionsPerUser = 1 })
	one := authTestAdmin(t, s, "one")
	two := authTestAdmin(t, s, "two")
	session := authTestSession(t, s, one, 1)
	if _, err := s.CreateSession(ctx, SessionInput{UserID: one.ID, AuthEpoch: 1, SecretDigest: bytes.Repeat([]byte{2}, 32), CSRFToken: strings.Repeat("x", 43)}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("per-user cap: %v", err)
	}
	if err := s.RevokeSession(ctx, two.ID, session.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("cross-user revoke: %v", err)
	}
	if err := s.RevokeSession(ctx, one.ID, session.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeSession(ctx, one.ID, session.ID); err != nil {
		t.Fatalf("revoke retry: %v", err)
	}
	replacement := authTestSession(t, s, one, 2)
	if replacement.ID == session.ID {
		t.Fatal("revoked access ID reused")
	}
	var count int
	if err := s.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM sessions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("terminal sessions accumulated: %d", count)
	}
}

func TestAuthCredentialRotationRetriesGraceAndRestart(t *testing.T) {
	ctx := context.Background()
	s, path := authTestStore(t, nil)
	user := authTestAdmin(t, s, "admin")
	credential := authTestCredential(t, s, user, 1)
	original := s.now()
	input := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "rotate-one", ExpectedGeneration: 1, PendingDigest: bytes.Repeat([]byte{2}, 32), Fingerprint: bytes.Repeat([]byte{3}, 32)}
	prepared, err := s.PrepareCredentialRotation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ID != credential.ID || prepared.Generation != 1 || prepared.PendingActionID != input.ActionID {
		t.Fatalf("prepare identity: %+v", prepared)
	}
	if _, err := s.ValidateCredential(ctx, credential.ID, input.PendingDigest); !errors.Is(err, ErrDenied) {
		t.Fatalf("pending authenticated ordinary request: %v", err)
	}
	repeated, err := s.PrepareCredentialRotation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !repeated.PendingExpiresAt.Equal(*prepared.PendingExpiresAt) {
		t.Fatal("retry extended pending lifetime")
	}
	changed := input
	changed.Fingerprint = bytes.Repeat([]byte{4}, 32)
	if _, err := s.PrepareCredentialRotation(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("prepare changed fingerprint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	reopened, err := Open(ctx, path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.now = func() time.Time { return original.Add(time.Minute) }
	activated, err := reopened.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, input.ExpectedGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if activated.ID != credential.ID || activated.Generation != 2 || activated.PendingActionID != "" || activated.RotationActionID != input.ActionID {
		t.Fatalf("activation record: %+v", activated)
	}
	if _, err := reopened.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, input.ExpectedGeneration); err != nil {
		t.Fatalf("lost activation response retry: %v", err)
	}
	if _, err := reopened.PrepareCredentialRotation(ctx, input); err != nil {
		t.Fatalf("lost prepare response after activation: %v", err)
	}
	if _, err := reopened.ValidateCredential(ctx, credential.ID, bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatalf("previous secret grace: %v", err)
	}
	reopened.now = func() time.Time { return original.Add(2 * time.Minute) }
	if _, err := reopened.ValidateCredential(ctx, credential.ID, bytes.Repeat([]byte{1}, 32)); !errors.Is(err, ErrDenied) {
		t.Fatalf("previous accepted at grace deadline: %v", err)
	}
	if _, err := reopened.ValidateCredential(ctx, credential.ID, input.PendingDigest); err != nil {
		t.Fatal(err)
	}
	second := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "rotate-two", ExpectedGeneration: 2, PendingDigest: bytes.Repeat([]byte{5}, 32), Fingerprint: bytes.Repeat([]byte{6}, 32)}
	if _, err := reopened.PrepareCredentialRotation(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ActivateCredentialRotation(ctx, credential.ID, second.ActionID, second.PendingDigest, second.ExpectedGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.PrepareCredentialRotation(ctx, input); !errors.Is(err, ErrConflict) {
		t.Fatalf("ancient prepare replay: %v", err)
	}
	if _, err := reopened.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, input.ExpectedGeneration); !errors.Is(err, ErrDenied) {
		t.Fatalf("ancient activation replay: %v", err)
	}
}

func TestAuthCredentialExpirationCapacityAndOwnedRevocation(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, func(o *Options) { o.MaxCredentials = 2; o.MaxCredentialsPerUser = 1 })
	one := authTestAdmin(t, s, "one")
	two := authTestAdmin(t, s, "two")
	credential := authTestCredential(t, s, one, 1)
	if _, err := s.CreateCredential(ctx, CredentialInput{UserID: one.ID, AuthEpoch: 1, Label: "full", SecretDigest: bytes.Repeat([]byte{2}, 32), Scopes: []string{"files"}, AllOwnedVolumes: true, ExpiresAt: s.now().Add(time.Hour)}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("credential per-user cap: %v", err)
	}
	if err := s.RevokeCredential(ctx, two.ID, credential.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("other-user credential revoke: %v", err)
	}
	if err := s.RevokeCredential(ctx, one.ID, credential.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeCredential(ctx, one.ID, credential.ID); err != nil {
		t.Fatalf("revoke retry: %v", err)
	}
	if _, err := s.ValidateCredential(ctx, credential.ID, bytes.Repeat([]byte{1}, 32)); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked credential accepted: %v", err)
	}
	replacement := authTestCredential(t, s, one, 2)
	now := replacement.ExpiresAt
	s.now = func() time.Time { return now }
	if _, err := s.ValidateCredential(ctx, replacement.ID, bytes.Repeat([]byte{2}, 32)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expiry boundary: %v", err)
	}
	authTestCredential(t, s, one, 3)
	var count int
	if err := s.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM cli_credentials").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("terminal credentials accumulated: %d", count)
	}
}

func authTestOrdinaryUser(t *testing.T, s *Store, name string) User {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	now := s.now()
	err = s.writeTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO users(id,role,display_name,auth_epoch,created_at) VALUES(?,'user',?,1,?)", id, name, unixMillis(now))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return User{ID: id, Role: RoleUser, DisplayName: name, AuthEpoch: 1, CreatedAt: now}
}

func TestAuthOrdinaryUserProjectionAndCredentialRestrictions(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	admin := authTestAdmin(t, s, "admin")
	ordinary := authTestOrdinaryUser(t, s, "ordinary")
	another := authTestOrdinaryUser(t, s, "another")
	users, err := s.ListOrdinaryUsers(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || users[0].Role != RoleUser {
		t.Fatalf("ordinary user projection: %+v", users)
	}
	next, err := s.ListOrdinaryUsers(ctx, users[0].ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || next[0].ID <= users[0].ID {
		t.Fatalf("user pagination: %+v", next)
	}
	if _, err := s.ListOrdinaryUsers(ctx, "", s.options.MaxPageSize+1); !errors.Is(err, ErrCapacity) {
		t.Fatalf("unbounded user listing: %v", err)
	}
	if _, err := s.ResetAdminPassword(ctx, ordinary.ID, "hash"); !errors.Is(err, ErrDenied) {
		t.Fatalf("reset ordinary user: %v", err)
	}
	base := CredentialInput{UserID: ordinary.ID, AuthEpoch: 1, Label: "ordinary", SecretDigest: bytes.Repeat([]byte{1}, 32), Scopes: []string{"admin:summaries"}, AllOwnedVolumes: true, ExpiresAt: s.now().Add(time.Hour)}
	if _, err := s.CreateCredential(ctx, base); !errors.Is(err, ErrDenied) {
		t.Fatalf("ordinary admin scope: %v", err)
	}
	base.Scopes = []string{"unknown"}
	if _, err := s.CreateCredential(ctx, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("invented scope: %v", err)
	}
	base.Scopes = []string{"files", "files"}
	if _, err := s.CreateCredential(ctx, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate scope: %v", err)
	}
	volume, err := s.CreateVolume(ctx, ordinary.ID, "workspace", 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err = s.SetVolumeState(ctx, ordinary.ID, volume.ID, volume.ManagementRevision, VolumeActive, "")
	if err != nil {
		t.Fatal(err)
	}
	base.Scopes = []string{"files"}
	base.AllOwnedVolumes = false
	base.VolumeIDs = []ID{volume.ID}
	owned, err := s.CreateCredential(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(owned.VolumeIDs) != 1 || owned.VolumeIDs[0] != volume.ID {
		t.Fatalf("owned restriction missing: %+v", owned)
	}
	base.UserID = admin.ID
	base.SecretDigest = bytes.Repeat([]byte{2}, 32)
	if _, err := s.CreateCredential(ctx, base); !errors.Is(err, ErrDenied) {
		t.Fatalf("administrator granted other's volume: %v", err)
	}
	base.UserID = another.ID
	if _, err := s.CreateCredential(ctx, base); !errors.Is(err, ErrDenied) {
		t.Fatalf("other owner resource: %v", err)
	}
}

func TestAuthRotationPendingDeadlineAndOwnership(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	owner := authTestAdmin(t, s, "owner")
	other := authTestAdmin(t, s, "other")
	credential := authTestCredential(t, s, owner, 1)
	input := RotationInput{UserID: other.ID, CredentialID: credential.ID, ActionID: "rotate", ExpectedGeneration: 1, PendingDigest: bytes.Repeat([]byte{2}, 32), Fingerprint: bytes.Repeat([]byte{3}, 32)}
	if _, err := s.PrepareCredentialRotation(ctx, input); !errors.Is(err, ErrDenied) {
		t.Fatalf("cross-owner prepare: %v", err)
	}
	input.UserID = owner.ID
	prepared, err := s.PrepareCredentialRotation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	now := *prepared.PendingExpiresAt
	s.now = func() time.Time { return now }
	if _, err := s.PrepareCredentialRotation(ctx, input); !errors.Is(err, ErrExpired) {
		t.Fatalf("prepare resurrected expired action: %v", err)
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, input.ExpectedGeneration); !errors.Is(err, ErrDenied) {
		t.Fatalf("retired activate: %v", err)
	}
	current, err := s.GetCredential(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Generation != 2 {
		t.Fatalf("expired pending generation not fenced: %d", current.Generation)
	}
	fresh := RotationInput{UserID: owner.ID, CredentialID: credential.ID, ActionID: "fresh", ExpectedGeneration: 2, PendingDigest: bytes.Repeat([]byte{4}, 32), Fingerprint: bytes.Repeat([]byte{5}, 32)}
	if _, err := s.PrepareCredentialRotation(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareCredentialRotation(ctx, input); !errors.Is(err, ErrConflict) {
		t.Fatalf("expired candidate reentered after another prepare: %v", err)
	}
	if _, err := s.ValidateCredential(ctx, credential.ID, bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatalf("pending expiry destroyed active secret: %v", err)
	}
}

func TestAuthConcurrentSessionAdmissionAndActivation(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, func(o *Options) { o.MaxSessions = 2; o.MaxSessionsPerUser = 2 })
	user := authTestAdmin(t, s, "admin")
	start := make(chan struct{})
	results := make(chan error, 20)
	for i := byte(1); i <= 20; i++ {
		go func(value byte) {
			<-start
			_, err := s.CreateSession(ctx, SessionInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, SecretDigest: bytes.Repeat([]byte{value}, 32), CSRFToken: strings.Repeat("c", 43)})
			results <- err
		}(i)
	}
	close(start)
	admitted := 0
	for range 20 {
		err := <-results
		if err == nil {
			admitted++
		} else if !errors.Is(err, ErrCapacity) {
			t.Fatalf("session admission: %v", err)
		}
	}
	if admitted != 2 {
		t.Fatalf("concurrent cap admitted %d sessions", admitted)
	}
	credential := authTestCredential(t, s, user, 25)
	rotation := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "concurrent", ExpectedGeneration: 1, PendingDigest: bytes.Repeat([]byte{26}, 32), Fingerprint: bytes.Repeat([]byte{27}, 32)}
	if _, err := s.PrepareCredentialRotation(ctx, rotation); err != nil {
		t.Fatal(err)
	}
	type activation struct {
		generation int64
		err        error
	}
	activated := make(chan activation, 2)
	start = make(chan struct{})
	for range 2 {
		go func() {
			<-start
			record, err := s.ActivateCredentialRotation(ctx, credential.ID, rotation.ActionID, rotation.PendingDigest, rotation.ExpectedGeneration)
			activated <- activation{record.Generation, err}
		}()
	}
	close(start)
	for range 2 {
		result := <-activated
		if result.err != nil || result.generation != 2 {
			t.Fatalf("concurrent activation: %+v", result)
		}
	}
	current, err := s.GetCredential(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Generation != 2 {
		t.Fatal("duplicate activation advanced generation twice")
	}
}

func TestAuthRejectsMalformedRecordsAndInputs(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	for _, name := range []string{"", "\x00", string([]byte{0xff}), strings.Repeat("x", 129)} {
		if _, err := s.AddAdmin(ctx, name, "Admin", "hash"); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid login %q: %v", name, err)
		}
	}
	user := authTestAdmin(t, s, "admin")
	for _, csrf := range []string{"", strings.Repeat("c", 31), strings.Repeat("c", 129), strings.Repeat("c", 32) + "\n", strings.Repeat("c", 32) + "é"} {
		if _, err := s.CreateSession(ctx, SessionInput{UserID: user.ID, AuthEpoch: 1, SecretDigest: bytes.Repeat([]byte{1}, 32), CSRFToken: csrf}); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid CSRF accepted: %v", err)
		}
	}
	if _, err := s.CreateSession(ctx, SessionInput{UserID: user.ID, AuthEpoch: 1, SecretDigest: []byte{1}, CSRFToken: strings.Repeat("c", 43)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("short digest accepted: %v", err)
	}
	session := authTestSession(t, s, user, 1)
	if _, err := s.write.ExecContext(ctx, "UPDATE sessions SET csrf_token=? WHERE id=?", strings.Repeat("!", 43), session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSession(ctx, session.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed session returned: %v", err)
	}
	credential := authTestCredential(t, s, user, 2)
	if _, err := s.write.ExecContext(ctx, "UPDATE cli_credentials SET scopes='[\"unknown\"]' WHERE id=?", credential.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredential(ctx, credential.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt scope returned: %v", err)
	}
}

func TestAuthResourceScopeRequiresExplicitResources(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	user := authTestAdmin(t, s, "admin")
	input := CredentialInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, Label: "restricted", SecretDigest: bytes.Repeat([]byte{1}, 32), Scopes: []string{"files"}, ExpiresAt: s.now().Add(time.Hour)}
	for _, scope := range []string{"files", "volumes:read", "volumes:write"} {
		input.Scopes = []string{scope}
		if _, err := s.CreateCredential(ctx, input); !errors.Is(err, ErrConflict) {
			t.Fatalf("resource scope %s without selector: %v", scope, err)
		}
	}
	input.Scopes = []string{"admin:summaries"}
	credential, err := s.CreateCredential(ctx, input)
	if err != nil {
		t.Fatalf("summary-only credential requires no owned volume: %v", err)
	}
	if _, err := s.write.ExecContext(ctx, "UPDATE cli_credentials SET scopes='[\"files\"]' WHERE id=?", credential.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredential(ctx, credential.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stored resource scope lacks selector: %v", err)
	}
}

func authTestTamperIdentity(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	err := s.writeTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
			return err
		}
		_, mutationErr := tx.Exec(query, args...)
		_, restoreErr := tx.Exec("PRAGMA ignore_check_constraints=OFF")
		return errors.Join(mutationErr, restoreErr)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAuthRejectsNoncanonicalStoredIdentitiesAndScopes(t *testing.T) {
	ctx := context.Background()
	t.Run("user", func(t *testing.T) {
		s, _ := authTestStore(t, nil)
		user := authTestAdmin(t, s, "admin")
		upper := ID(strings.Repeat("A", 32))
		authTestTamperIdentity(t, s, "UPDATE users SET id=? WHERE id=?", upper, user.ID)
		if _, err := s.GetUser(ctx, upper); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("uppercase user identity: %v", err)
		}
	})
	t.Run("session", func(t *testing.T) {
		s, _ := authTestStore(t, nil)
		user := authTestAdmin(t, s, "admin")
		session := authTestSession(t, s, user, 1)
		upper := ID(strings.Repeat("A", 32))
		authTestTamperIdentity(t, s, "UPDATE sessions SET id=? WHERE id=?", upper, session.ID)
		if _, err := s.GetSession(ctx, upper); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("uppercase session identity: %v", err)
		}
	})
	t.Run("credential", func(t *testing.T) {
		s, _ := authTestStore(t, nil)
		user := authTestAdmin(t, s, "admin")
		credential := authTestCredential(t, s, user, 1)
		upper := ID(strings.Repeat("A", 32))
		authTestTamperIdentity(t, s, "UPDATE cli_credentials SET id=? WHERE id=?", upper, credential.ID)
		if _, err := s.GetCredential(ctx, upper); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("uppercase credential identity: %v", err)
		}
	})
	t.Run("scopes", func(t *testing.T) {
		s, _ := authTestStore(t, nil)
		user := authTestAdmin(t, s, "admin")
		credential := authTestCredential(t, s, user, 1)
		if _, err := s.write.ExecContext(ctx, "UPDATE cli_credentials SET scopes='[\"volumes:read\",\"files\"]' WHERE id=?", credential.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetCredential(ctx, credential.ID); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("noncanonical scopes: %v", err)
		}
	})
}

func TestAuthActivatedReceiptSurvivesLaterPendingRetirement(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	user := authTestAdmin(t, s, "admin")
	credential := authTestCredential(t, s, user, 1)
	first := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "activated", ExpectedGeneration: 1, PendingDigest: bytes.Repeat([]byte{2}, 32), Fingerprint: bytes.Repeat([]byte{3}, 32)}
	if _, err := s.PrepareCredentialRotation(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, first.ActionID, first.PendingDigest, first.ExpectedGeneration); err != nil {
		t.Fatal(err)
	}
	second := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "expired", ExpectedGeneration: 2, PendingDigest: bytes.Repeat([]byte{4}, 32), Fingerprint: bytes.Repeat([]byte{5}, 32)}
	prepared, err := s.PrepareCredentialRotation(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	now := *prepared.PendingExpiresAt
	s.now = func() time.Time { return now }
	if _, err := s.PrepareCredentialRotation(ctx, second); !errors.Is(err, ErrExpired) {
		t.Fatalf("retire later pending: %v", err)
	}
	for _, retry := range []func() (Credential, error){
		func() (Credential, error) {
			return s.ActivateCredentialRotation(ctx, credential.ID, first.ActionID, first.PendingDigest, first.ExpectedGeneration)
		},
		func() (Credential, error) { return s.PrepareCredentialRotation(ctx, first) },
	} {
		receipt, err := retry()
		if err != nil {
			t.Fatalf("activated receipt retry after retirement: %v", err)
		}
		if receipt.Generation != 3 || receipt.RotationGeneration != 2 || receipt.RotationActionID != first.ActionID {
			t.Fatalf("activation receipt lost after retirement: %+v", receipt)
		}
	}
	if _, err := s.ValidateCredential(ctx, credential.ID, first.PendingDigest); err != nil {
		t.Fatalf("retirement changed active secret: %v", err)
	}
}

func TestAuthRotationActivationAuthenticatesBeforeGeneration(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	user := authTestAdmin(t, s, "admin")
	credential := authTestCredential(t, s, user, 1)
	input := RotationInput{UserID: user.ID, CredentialID: credential.ID, ActionID: "rotation", ExpectedGeneration: 1, PendingDigest: bytes.Repeat([]byte{2}, 32), Fingerprint: bytes.Repeat([]byte{3}, 32)}
	if _, err := s.PrepareCredentialRotation(ctx, input); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []int64{-1, 0, 1, 2, 100} {
		if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, bytes.Repeat([]byte{4}, 32), generation); !errors.Is(err, ErrDenied) {
			t.Fatalf("wrong pending secret generation %d: %v", generation, err)
		}
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, "wrong-action", input.PendingDigest, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("wrong action disclosed generation: %v", err)
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, 2); !errors.Is(err, ErrConflict) {
		t.Fatalf("authenticated generation mismatch: %v", err)
	}
	prepared, err := s.GetCredential(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Generation != 1 || prepared.PendingActionID != input.ActionID {
		t.Fatal("generation refusal mutated rotation")
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, 1); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []int64{-1, 0, 1, 2, 100} {
		if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, bytes.Repeat([]byte{4}, 32), generation); !errors.Is(err, ErrDenied) {
			t.Fatalf("wrong activated secret generation %d: %v", generation, err)
		}
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, 2); !errors.Is(err, ErrConflict) {
		t.Fatalf("activated wrong original generation: %v", err)
	}
	if _, err := s.ActivateCredentialRotation(ctx, credential.ID, input.ActionID, input.PendingDigest, 1); err != nil {
		t.Fatalf("activated exact retry: %v", err)
	}
}

func TestAuthHasLocalAdminRequiresCurrentCatalog(t *testing.T) {
	ctx := context.Background()
	s, _ := authTestStore(t, nil)
	exists, err := s.HasLocalAdmin(ctx)
	if err != nil || exists {
		t.Fatalf("fresh catalog administrator: %t, %v", exists, err)
	}
	authTestAdmin(t, s, "one")
	authTestAdmin(t, s, "two")
	exists, err = s.HasLocalAdmin(ctx)
	if err != nil || !exists {
		t.Fatalf("named administrator catalog: %t, %v", exists, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HasLocalAdmin(ctx); err == nil {
		t.Fatal("unavailable catalog reported successful absence")
	}
}
