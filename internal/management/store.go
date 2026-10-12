// Package management owns the standalone service's durable account and lifecycle records.
// Filesystem metadata and native publication receipts remain in their volume authorities.
package management

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
	_ "modernc.org/sqlite"
)

type ID string

func (id ID) Valid() bool { return validID(id) }

func validID(id ID) bool {
	if len(id) != 32 {
		return false
	}
	for _, character := range id {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func NewID() (ID, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return ID(hex.EncodeToString(bytes[:])), nil
}

var (
	ErrNotFound = errors.New("management record not found")
	ErrDenied   = errors.New("management access denied")
	ErrConflict = errors.New("management condition conflict")
	ErrExpired  = errors.New("management acceptance window expired")
	ErrCapacity = errors.New("management resource capacity exhausted")
	ErrCorrupt  = errors.New("management state is inconsistent")
	ErrClosed   = errors.New("management store is closed")
)

// CommitUnknownError preserves the accepted responsibility when SQLite cannot
// confirm the transaction result. It must not be interpreted as a rollback.
type CommitUnknownError struct{ Cause error }

func (e *CommitUnknownError) Error() string { return "management transaction outcome is unknown" }
func (e *CommitUnknownError) Unwrap() error { return e.Cause }

type Options struct {
	Create                bool
	ExpectedDatabaseID    ID
	MaxReaders            int
	MaxUsers              int
	MaxSessions           int
	MaxSessionsPerUser    int
	MaxCredentials        int
	MaxCredentialsPerUser int
	MaxCredentialVolumes  int
	MaxVolumes            int
	MaxPageSize           int
	BusyTimeout           time.Duration
	SessionIdleTTL        time.Duration
	SessionAbsoluteTTL    time.Duration
	CredentialMaxTTL      time.Duration
	RotationPendingTTL    time.Duration
	RotationGrace         time.Duration
}

func DefaultOptions() Options {
	return Options{
		MaxReaders: 4, MaxUsers: 10000,
		MaxSessions: 1024, MaxSessionsPerUser: 8,
		MaxCredentials: 1024, MaxCredentialsPerUser: 32, MaxCredentialVolumes: 128,
		MaxVolumes:     10000,
		MaxPageSize:    50,
		BusyTimeout:    2 * time.Second,
		SessionIdleTTL: 30 * time.Minute, SessionAbsoluteTTL: 12 * time.Hour,
		CredentialMaxTTL:   90 * 24 * time.Hour,
		RotationPendingTTL: 10 * time.Minute, RotationGrace: time.Minute,
	}
}

func (o Options) check() error {
	for _, n := range []int{o.MaxReaders, o.MaxUsers, o.MaxSessions, o.MaxSessionsPerUser,
		o.MaxCredentials, o.MaxCredentialsPerUser, o.MaxCredentialVolumes, o.MaxVolumes,
		o.MaxPageSize} {
		if n <= 0 || n == math.MaxInt {
			return fmt.Errorf("management count limits must be positive and bounded")
		}
	}
	for _, d := range []time.Duration{o.BusyTimeout, o.SessionIdleTTL, o.SessionAbsoluteTTL,
		o.CredentialMaxTTL, o.RotationPendingTTL, o.RotationGrace} {
		if d < time.Millisecond {
			return errors.New("management durations must be at least one millisecond")
		}
	}
	if o.SessionIdleTTL > o.SessionAbsoluteTTL || o.MaxSessionsPerUser > o.MaxSessions ||
		o.MaxCredentialsPerUser > o.MaxCredentials {
		return errors.New("management per-user and idle limits exceed their enclosing limits")
	}
	return nil
}

func (o Options) Validate() error { return o.check() }

type Store struct {
	write       *sql.DB
	read        *sql.DB
	options     Options
	now         func() time.Time
	id          ID
	closeMu     sync.Mutex
	closed      bool
	closeErr    error
	writerMu    sync.Mutex
	readClosed  bool
	writeClosed bool
}

//go:embed migrations/*.sql
var migrationFiles embed.FS

var migrations = sqliteschema.MustLoad(migrationFiles, "migrations")

// Open requires the caller to retain exclusive service-root ownership until Close
// succeeds. Create is an explicit offline initialization choice, never a recovery
// fallback for a missing database. The returned ID binds catalog evidence to its root.
// A failed cleanup returns the retained Store; Close must
// succeed before the caller releases its surrounding ownership.
func Open(ctx context.Context, database string, options Options) (opened *Store, returned error) {
	if err := options.check(); err != nil {
		return nil, err
	}
	if database == "" || !filepath.IsAbs(database) {
		return nil, errors.New("management database requires an absolute filesystem path")
	}
	if err := validateFile(database, options.Create); err != nil {
		return nil, err
	}
	s := &Store{options: options, now: time.Now}
	defer func() {
		if returned != nil {
			if closeErr := s.Close(); closeErr != nil {
				opened = s
				returned = errors.Join(returned, closeErr)
			}
		}
	}()
	if err := prepareFile(database, options.Create); err != nil {
		return nil, err
	}
	writer, err := openPool(ctx, database, true, 1, options.BusyTimeout)
	if err != nil {
		return nil, err
	}
	s.write = writer
	if err := s.writeTx(ctx, func(tx *sql.Tx) error {
		if err := migrations.Reach(ctx, tx); err != nil {
			return err
		}
		if err := initializeIdentity(ctx, tx, options.Create); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT id FROM database_identity WHERE singleton=1").Scan(&s.id); err != nil {
			return fmt.Errorf("read management identity: %w", err)
		}
		if options.ExpectedDatabaseID != "" && options.ExpectedDatabaseID != s.id {
			return fmt.Errorf("management database identity differs from the service root: %w", ErrCorrupt)
		}
		return validateIntegrity(ctx, tx)
	}); err != nil {
		return nil, err
	}
	s.read, err = openPool(ctx, database, false, options.MaxReaders, options.BusyTimeout)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) DatabaseID() ID { return s.id }

func unixMillis(t time.Time) int64 { return t.UTC().UnixMilli() }

func prepareFile(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return fmt.Errorf("create management database: %w", err)
		}
		return errors.Join(file.Sync(), file.Close())
	}
	if err != nil {
		return fmt.Errorf("open management database: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("management database must be an owner-only regular file")
	}
	return nil
}

func validateFile(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open management database: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("management database must be an owner-only regular file")
	}
	return nil
}

func openPool(ctx context.Context, path string, writer bool, limit int, busy time.Duration) (*sql.DB, error) {
	parameters := url.Values{}
	parameters.Set("mode", "rw")
	parameters.Add("_pragma", "journal_mode(WAL)")
	parameters.Add("_pragma", "busy_timeout("+strconv.FormatInt(busy.Milliseconds(), 10)+")")
	parameters.Add("_pragma", "synchronous(FULL)")
	if writer {
		parameters.Set("_txlock", "immediate")
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: parameters.Encode()}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(limit)
	db.SetMaxIdleConns(limit)
	if err := db.PingContext(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	var synchronous int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if synchronous != 2 {
		return nil, errors.Join(errors.New("management SQLite must use FULL synchronous mode"), db.Close())
	}
	return db, nil
}

func initializeIdentity(ctx context.Context, tx *sql.Tx, create bool) error {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM database_identity").Scan(&count); err != nil {
		return err
	}
	if count == 1 {
		return nil
	}
	if count != 0 || !create {
		return fmt.Errorf("management database has no unique durable identity: %w", ErrCorrupt)
	}
	id, err := NewID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO database_identity(singleton,id) VALUES(1,?)", id)
	return err
}

func (s *Store) writeTx(ctx context.Context, action func(*sql.Tx) error) (returned error) {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	s.closeMu.Lock()
	closed := s.closed
	s.closeMu.Unlock()
	if closed {
		return ErrClosed
	}
	connection, err := s.write.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire management writer: %w", err)
	}
	defer func() { returned = errors.Join(returned, connection.Close()) }()
	tx, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin management transaction: %w", err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			returned = errors.Join(returned, fmt.Errorf("rollback management transaction: %w", err))
		}
	}()
	if err := action(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return &CommitUnknownError{Cause: err}
	}
	return nil
}

// Close drains the SQLite pools before relinquishing their ownership. Its result
// must be checked before the surrounding service-root lock is released.
func (s *Store) Close() error {
	s.writerMu.Lock()
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.closed = true
	var readErr error
	if s.read != nil && !s.readClosed {
		readErr = s.read.Close()
		s.readClosed = readErr == nil
	}
	var writeErr error
	if s.write != nil && !s.writeClosed {
		writeErr = s.write.Close()
		s.writeClosed = writeErr == nil
	}
	s.writerMu.Unlock()
	s.closeErr = errors.Join(readErr, writeErr)
	return s.closeErr
}

func validateIntegrity(ctx context.Context, tx *sql.Tx) error {
	var check string
	if err := tx.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil {
		return err
	}
	if check != "ok" {
		return fmt.Errorf("management SQLite integrity check failed: %w", ErrCorrupt)
	}
	for _, query := range []string{
		"SELECT EXISTS(SELECT 1 FROM local_admin_credentials a LEFT JOIN users u ON u.id=a.user_id WHERE u.id IS NULL OR u.role<>'admin')",
		"SELECT EXISTS(SELECT 1 FROM users u WHERE u.role='admin' AND NOT EXISTS(SELECT 1 FROM local_admin_credentials a WHERE a.user_id=u.id))",
		"SELECT EXISTS(SELECT 1 FROM identity_bindings b LEFT JOIN users u ON u.id=b.user_id WHERE u.id IS NULL OR u.role<>'user')",
		"SELECT EXISTS(SELECT 1 FROM sessions s LEFT JOIN users u ON u.id=s.user_id WHERE u.id IS NULL OR s.auth_epoch>u.auth_epoch)",
		"SELECT EXISTS(SELECT 1 FROM cli_credentials c LEFT JOIN users u ON u.id=c.user_id WHERE u.id IS NULL OR c.auth_epoch>u.auth_epoch)",
		"SELECT EXISTS(SELECT 1 FROM volumes v LEFT JOIN users u ON u.id=v.owner_user_id WHERE u.id IS NULL)",
		"SELECT EXISTS(SELECT 1 FROM credential_volumes b LEFT JOIN cli_credentials c ON c.id=b.credential_id LEFT JOIN volumes v ON v.id=b.volume_id WHERE c.id IS NULL OR v.id IS NULL OR v.owner_user_id<>c.user_id)",
	} {
		var invalid bool
		if err := tx.QueryRowContext(ctx, query).Scan(&invalid); err != nil {
			return fmt.Errorf("validate management relationships: %w", err)
		}
		if invalid {
			return fmt.Errorf("management record relationships are inconsistent: %w", ErrCorrupt)
		}
	}
	return nil
}
