package management

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode"
	"unicode/utf8"
)

type UserRole string

const (
	RoleAdmin UserRole = "admin"
	RoleUser  UserRole = "user"
)

type User struct {
	ID          ID
	Role        UserRole
	DisplayName string
	AuthEpoch   int64
	CreatedAt   time.Time
}

type AdminCredential struct {
	User            User
	LoginName       string
	PasswordHash    string
	PasswordVersion int64
	ChangedAt       time.Time
}

const userColumns = "id, role, display_name, auth_epoch, created_at"

type rowScanner interface{ Scan(...any) error }

func scanUser(row rowScanner) (User, error) {
	var u User
	var created int64
	if err := row.Scan(&u.ID, &u.Role, &u.DisplayName, &u.AuthEpoch, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	if !validID(u.ID) || (u.Role != RoleAdmin && u.Role != RoleUser) || u.AuthEpoch < 1 {
		return User{}, fmt.Errorf("%w: invalid user record", ErrCorrupt)
	}
	u.CreatedAt = time.UnixMilli(created).UTC()
	return u, nil
}

func requireUserTx(ctx context.Context, tx *sql.Tx, id ID) (User, error) {
	return scanUser(tx.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE id = ?", id))
}

func (s *Store) GetUser(ctx context.Context, id ID) (User, error) {
	return scanUser(s.read.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE id = ?", id))
}

func (s *Store) ListOrdinaryUsers(ctx context.Context, after ID, limit int) ([]User, error) {
	if limit < 1 || limit > s.options.MaxPageSize {
		return nil, fmt.Errorf("%w: invalid page size", ErrCapacity)
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+userColumns+" FROM users WHERE role = 'user' AND id > ? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]User, 0, limit)
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func validAuthName(value string) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) < 1 || utf8.RuneCountInString(value) > 128 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (s *Store) BootstrapAdmin(ctx context.Context, loginName, displayName, passwordHash string) (User, error) {
	return s.addAdmin(ctx, loginName, displayName, passwordHash, true)
}

func (s *Store) AddAdmin(ctx context.Context, loginName, displayName, passwordHash string) (User, error) {
	return s.addAdmin(ctx, loginName, displayName, passwordHash, false)
}

func (s *Store) addAdmin(ctx context.Context, loginName, displayName, passwordHash string, bootstrap bool) (User, error) {
	if !validAuthName(loginName) || !validAuthName(displayName) || passwordHash == "" || len(passwordHash) > 4096 {
		return User{}, fmt.Errorf("%w: invalid administrator credential", ErrConflict)
	}
	id, err := NewID()
	if err != nil {
		return User{}, err
	}
	var user User
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		if bootstrap {
			var exists bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')").Scan(&exists); err != nil {
				return err
			}
			if exists {
				return ErrConflict
			}
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			return err
		}
		if count >= s.options.MaxUsers {
			return ErrCapacity
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM local_admin_credentials WHERE login_name = ?)", loginName).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrConflict
		}
		now := s.now().UTC()
		if _, err := tx.ExecContext(ctx, "INSERT INTO users(id,role,display_name,auth_epoch,next_action_namespace,retired_before_namespace,logical_time_high_water,created_at) VALUES(?,'admin',?,1,1,0,0,?)", id, displayName, unixMillis(now)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO local_admin_credentials(user_id,login_name,password_hash,password_version,changed_at) VALUES(?,?,?,1,?)", id, loginName, passwordHash, unixMillis(now)); err != nil {
			return err
		}
		user = User{ID: id, Role: RoleAdmin, DisplayName: displayName, AuthEpoch: 1, CreatedAt: now}
		return nil
	})
	return user, err
}

func (s *Store) HasLocalAdmin(ctx context.Context) (bool, error) {
	var exists bool
	err := s.read.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM local_admin_credentials c JOIN users u ON u.id=c.user_id WHERE u.role='admin')").Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) AdminCredential(ctx context.Context, loginName string) (AdminCredential, error) {
	var a AdminCredential
	var created, changed int64
	err := s.read.QueryRowContext(ctx, "SELECT u.id,u.role,u.display_name,u.auth_epoch,u.created_at,c.login_name,c.password_hash,c.password_version,c.changed_at FROM users u JOIN local_admin_credentials c ON c.user_id=u.id WHERE c.login_name=?", loginName).Scan(&a.User.ID, &a.User.Role, &a.User.DisplayName, &a.User.AuthEpoch, &created, &a.LoginName, &a.PasswordHash, &a.PasswordVersion, &changed)
	if errors.Is(err, sql.ErrNoRows) {
		return AdminCredential{}, ErrNotFound
	}
	if err != nil {
		return AdminCredential{}, err
	}
	if !validID(a.User.ID) || a.User.Role != RoleAdmin || a.User.AuthEpoch < 1 || a.PasswordVersion < 1 || a.PasswordHash == "" {
		return AdminCredential{}, fmt.Errorf("%w: invalid administrator record", ErrCorrupt)
	}
	a.User.CreatedAt = time.UnixMilli(created).UTC()
	a.ChangedAt = time.UnixMilli(changed).UTC()
	return a, nil
}

func (s *Store) ResetAdminPassword(ctx context.Context, userID ID, passwordHash string) (User, error) {
	if passwordHash == "" || len(passwordHash) > 4096 {
		return User{}, fmt.Errorf("%w: invalid password hash", ErrConflict)
	}
	var user User
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		u, err := requireUserTx(ctx, tx, userID)
		if err != nil {
			return err
		}
		if u.Role != RoleAdmin {
			return ErrDenied
		}
		if u.AuthEpoch == int64(^uint64(0)>>1) {
			return ErrCapacity
		}
		now := unixMillis(s.now())
		result, err := tx.ExecContext(ctx, "UPDATE local_admin_credentials SET password_hash=?,password_version=password_version+1,changed_at=? WHERE user_id=? AND password_version<9223372036854775807", passwordHash, now, userID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("%w: missing or exhausted administrator credential", ErrCorrupt)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE users SET auth_epoch=auth_epoch+1 WHERE id=?", userID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL", now, userID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE cli_credentials SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL", now, userID); err != nil {
			return err
		}
		u.AuthEpoch++
		user = u
		return nil
	})
	return user, err
}

func validDigest(digest []byte) bool { return len(digest) == 32 }

func validCSRFToken(token string) bool {
	if len(token) < 32 || len(token) > 128 {
		return false
	}
	for _, c := range []byte(token) {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func equalDigest(a, b []byte) bool {
	return validDigest(a) && validDigest(b) && subtle.ConstantTimeCompare(a, b) == 1
}

type SessionInput struct {
	UserID       ID
	AuthEpoch    int64
	SecretDigest []byte
	CSRFToken    string
}

type Session struct {
	ID                ID
	UserID            ID
	AuthEpoch         int64
	CSRFToken         string
	CreatedAt         time.Time
	LastUsedAt        time.Time
	IdleExpiresAt     time.Time
	AbsoluteExpiresAt time.Time
	RevokedAt         *time.Time
	secretDigest      []byte
	currentAuthEpoch  int64
}

const sessionColumns = "s.id,s.user_id,s.auth_epoch,s.csrf_token,s.created_at,s.last_used_at,s.idle_expires_at,s.absolute_expires_at,s.revoked_at,s.secret_digest,u.auth_epoch"

func scanSession(row rowScanner) (Session, error) {
	var session Session
	var created, last, idle, absolute int64
	var revoked sql.NullInt64
	err := row.Scan(&session.ID, &session.UserID, &session.AuthEpoch, &session.CSRFToken, &created, &last, &idle, &absolute, &revoked, &session.secretDigest, &session.currentAuthEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	if !validID(session.ID) || !validID(session.UserID) || !validDigest(session.secretDigest) || session.AuthEpoch < 1 || session.currentAuthEpoch < 1 || !validCSRFToken(session.CSRFToken) || last < created || idle < last || absolute < created {
		return Session{}, fmt.Errorf("%w: invalid session record", ErrCorrupt)
	}
	session.CreatedAt = time.UnixMilli(created).UTC()
	session.LastUsedAt = time.UnixMilli(last).UTC()
	session.IdleExpiresAt = time.UnixMilli(idle).UTC()
	session.AbsoluteExpiresAt = time.UnixMilli(absolute).UTC()
	if revoked.Valid {
		t := time.UnixMilli(revoked.Int64).UTC()
		session.RevokedAt = &t
	}
	return session, nil
}

func (s *Store) CreateSession(ctx context.Context, input SessionInput) (Session, error) {
	if !validDigest(input.SecretDigest) || !validCSRFToken(input.CSRFToken) {
		return Session{}, fmt.Errorf("%w: invalid session input", ErrConflict)
	}
	id, err := NewID()
	if err != nil {
		return Session{}, err
	}
	var session Session
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		u, err := requireUserTx(ctx, tx, input.UserID)
		if err != nil {
			return err
		}
		if input.AuthEpoch != u.AuthEpoch {
			return ErrDenied
		}
		now := s.now().UTC()
		if _, err := tx.ExecContext(ctx, "DELETE FROM sessions WHERE revoked_at IS NOT NULL OR idle_expires_at<=? OR absolute_expires_at<=? OR auth_epoch<>(SELECT auth_epoch FROM users WHERE users.id=sessions.user_id)", unixMillis(now), unixMillis(now)); err != nil {
			return err
		}
		var total, owned int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(user_id=?),0) FROM sessions", input.UserID).Scan(&total, &owned); err != nil {
			return err
		}
		if total >= s.options.MaxSessions || owned >= s.options.MaxSessionsPerUser {
			return ErrCapacity
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE secret_digest=?)", input.SecretDigest).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrConflict
		}
		absolute := now.Add(s.options.SessionAbsoluteTTL)
		idle := now.Add(s.options.SessionIdleTTL)
		if idle.After(absolute) {
			idle = absolute
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO sessions(id,user_id,kind,secret_digest,csrf_token,auth_epoch,created_at,last_used_at,idle_expires_at,absolute_expires_at) VALUES(?,?,'browser',?,?,?,?,?,?,?)", id, input.UserID, input.SecretDigest, input.CSRFToken, u.AuthEpoch, unixMillis(now), unixMillis(now), unixMillis(idle), unixMillis(absolute))
		if err != nil {
			return err
		}
		session = Session{ID: id, UserID: input.UserID, AuthEpoch: u.AuthEpoch, CSRFToken: input.CSRFToken, CreatedAt: now, LastUsedAt: now, IdleExpiresAt: idle, AbsoluteExpiresAt: absolute, currentAuthEpoch: u.AuthEpoch}
		return nil
	})
	return session, err
}

func (s *Store) GetSession(ctx context.Context, id ID) (Session, error) {
	return scanSession(s.read.QueryRowContext(ctx, "SELECT "+sessionColumns+" FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id=?", id))
}

func (session Session) validAt(now time.Time) error {
	if session.RevokedAt != nil || session.AuthEpoch != session.currentAuthEpoch {
		return ErrDenied
	}
	if !now.Before(session.IdleExpiresAt) || !now.Before(session.AbsoluteExpiresAt) {
		return ErrExpired
	}
	return nil
}

func (s *Store) ValidateSession(ctx context.Context, id ID, digest []byte, touch bool) (Session, error) {
	if !validDigest(digest) {
		return Session{}, ErrDenied
	}
	if !touch {
		session, err := s.GetSession(ctx, id)
		if err != nil {
			return Session{}, err
		}
		if !equalDigest(session.secretDigest, digest) {
			return Session{}, ErrDenied
		}
		if err := session.validAt(s.now()); err != nil {
			return Session{}, err
		}
		return session, nil
	}
	var session Session
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		value, err := scanSession(tx.QueryRowContext(ctx, "SELECT "+sessionColumns+" FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id=?", id))
		if err != nil {
			return err
		}
		if !equalDigest(value.secretDigest, digest) {
			return ErrDenied
		}
		now := s.now().UTC()
		if err := value.validAt(now); err != nil {
			return err
		}
		// At most one persisted idle extension per minute; stream keepalives never request a touch.
		if now.Sub(value.LastUsedAt) >= time.Minute {
			idle := now.Add(s.options.SessionIdleTTL)
			if idle.After(value.AbsoluteExpiresAt) {
				idle = value.AbsoluteExpiresAt
			}
			if _, err := tx.ExecContext(ctx, "UPDATE sessions SET last_used_at=?,idle_expires_at=? WHERE id=?", unixMillis(now), unixMillis(idle), id); err != nil {
				return err
			}
			value.LastUsedAt = now
			value.IdleExpiresAt = idle
		}
		session = value
		return nil
	})
	return session, err
}

func (s *Store) RevokeSession(ctx context.Context, userID, id ID) error {
	return s.writeTx(ctx, func(tx *sql.Tx) error {
		var owner ID
		if err := tx.QueryRowContext(ctx, "SELECT user_id FROM sessions WHERE id=?", id).Scan(&owner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if owner != userID {
			return ErrDenied
		}
		_, err := tx.ExecContext(ctx, "UPDATE sessions SET revoked_at=COALESCE(revoked_at,?) WHERE id=?", unixMillis(s.now()), id)
		return err
	})
}

type CredentialInput struct {
	UserID          ID
	AuthEpoch       int64
	Label           string
	SecretDigest    []byte
	Scopes          []string
	AllOwnedVolumes bool
	VolumeIDs       []ID
	ExpiresAt       time.Time
}

type Credential struct {
	ID                  ID
	UserID              ID
	Label               string
	Scopes              []string
	AllOwnedVolumes     bool
	VolumeIDs           []ID
	AuthEpoch           int64
	Generation          int64
	CreatedAt           time.Time
	ExpiresAt           time.Time
	RevokedAt           *time.Time
	PendingActionID     ID
	PendingExpiresAt    *time.Time
	RotationActionID    ID
	RotationGeneration  int64
	currentDigest       []byte
	pendingDigest       []byte
	pendingFingerprint  []byte
	previousDigest      []byte
	previousValidUntil  *time.Time
	rotationFingerprint []byte
	currentAuthEpoch    int64
	currentRole         UserRole
}

const credentialColumns = "c.id,c.user_id,c.label,c.scopes,c.all_owned_volumes,c.auth_epoch,c.generation,c.created_at,c.expires_at,c.revoked_at,c.current_digest,c.pending_digest,c.pending_action_id,c.pending_fingerprint,c.pending_expires_at,c.previous_digest,c.previous_valid_until,c.rotation_action_id,c.rotation_fingerprint,c.rotation_generation,u.auth_epoch,u.role"

func canonicalScopes(scopes []string, role UserRole) ([]string, error) {
	if len(scopes) < 1 || len(scopes) > 4 {
		return nil, ErrConflict
	}
	result := append([]string(nil), scopes...)
	sort.Strings(result)
	for i, value := range result {
		if i > 0 && result[i-1] == value {
			return nil, ErrConflict
		}
		switch value {
		case "files", "volumes:read", "volumes:write":
		case "admin:summaries":
			if role != RoleAdmin {
				return nil, ErrDenied
			}
		default:
			return nil, ErrConflict
		}
	}
	return result, nil
}

func validCredentialResources(scopes []string, allOwned bool, volumeIDs []ID) bool {
	if allOwned {
		return len(volumeIDs) == 0
	}
	if len(volumeIDs) > 0 {
		return true
	}
	for _, scope := range scopes {
		if scope == "files" || scope == "volumes:read" || scope == "volumes:write" {
			return false
		}
	}
	return true
}

func scanCredential(row rowScanner) (Credential, error) {
	var c Credential
	var scopes string
	var allOwned int
	var created, expires int64
	var revoked, pendingUntil, previousUntil, rotationGeneration sql.NullInt64
	var pendingAction, rotationAction sql.NullString
	err := row.Scan(&c.ID, &c.UserID, &c.Label, &scopes, &allOwned, &c.AuthEpoch, &c.Generation, &created, &expires, &revoked, &c.currentDigest, &c.pendingDigest, &pendingAction, &c.pendingFingerprint, &pendingUntil, &c.previousDigest, &previousUntil, &rotationAction, &c.rotationFingerprint, &rotationGeneration, &c.currentAuthEpoch, &c.currentRole)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, err
	}
	if !validID(c.ID) || !validID(c.UserID) || !validDigest(c.currentDigest) || c.AuthEpoch < 1 || c.currentAuthEpoch < 1 || c.Generation < 1 || (allOwned != 0 && allOwned != 1) || expires <= created || (c.currentRole != RoleAdmin && c.currentRole != RoleUser) {
		return Credential{}, fmt.Errorf("%w: invalid credential record", ErrCorrupt)
	}
	if err := json.Unmarshal([]byte(scopes), &c.Scopes); err != nil {
		return Credential{}, fmt.Errorf("%w: credential scopes: %v", ErrCorrupt, err)
	}
	canonical, err := canonicalScopes(c.Scopes, RoleAdmin)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: invalid stored credential scopes", ErrCorrupt)
	}
	for i, scope := range canonical {
		if scope != c.Scopes[i] {
			return Credential{}, fmt.Errorf("%w: noncanonical credential scopes", ErrCorrupt)
		}
	}
	if (len(c.pendingDigest) > 0) != pendingAction.Valid || pendingAction.Valid != pendingUntil.Valid || pendingAction.Valid != (len(c.pendingFingerprint) > 0) || (pendingAction.Valid && (!validDigest(c.pendingDigest) || !validDigest(c.pendingFingerprint))) {
		return Credential{}, fmt.Errorf("%w: invalid pending credential rotation", ErrCorrupt)
	}
	if (len(c.previousDigest) > 0) != previousUntil.Valid || (previousUntil.Valid && !validDigest(c.previousDigest)) {
		return Credential{}, fmt.Errorf("%w: invalid previous credential generation", ErrCorrupt)
	}
	if rotationAction.Valid != rotationGeneration.Valid || rotationAction.Valid != (len(c.rotationFingerprint) > 0) || (rotationAction.Valid && (!validDigest(c.rotationFingerprint) || rotationGeneration.Int64 < 2 || rotationGeneration.Int64 > c.Generation)) {
		return Credential{}, fmt.Errorf("%w: invalid rotation receipt", ErrCorrupt)
	}
	c.AllOwnedVolumes = allOwned == 1
	c.CreatedAt = time.UnixMilli(created).UTC()
	c.ExpiresAt = time.UnixMilli(expires).UTC()
	if revoked.Valid {
		t := time.UnixMilli(revoked.Int64).UTC()
		c.RevokedAt = &t
	}
	if pendingUntil.Valid {
		t := time.UnixMilli(pendingUntil.Int64).UTC()
		c.PendingExpiresAt = &t
		c.PendingActionID = ID(pendingAction.String)
	}
	if previousUntil.Valid {
		t := time.UnixMilli(previousUntil.Int64).UTC()
		c.previousValidUntil = &t
	}
	if rotationAction.Valid {
		c.RotationActionID = ID(rotationAction.String)
		c.RotationGeneration = rotationGeneration.Int64
	}
	return c, nil
}

func credentialTx(ctx context.Context, tx *sql.Tx, id ID, maxVolumes int) (Credential, error) {
	c, err := scanCredential(tx.QueryRowContext(ctx, "SELECT "+credentialColumns+" FROM cli_credentials c JOIN users u ON u.id=c.user_id WHERE c.id=?", id))
	if err != nil {
		return Credential{}, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT volume_id FROM credential_volumes WHERE credential_id=? ORDER BY volume_id LIMIT ?", id, maxVolumes+1)
	if err != nil {
		return Credential{}, err
	}
	defer rows.Close()
	c.VolumeIDs = make([]ID, 0)
	for rows.Next() {
		var volumeID ID
		if err := rows.Scan(&volumeID); err != nil {
			return Credential{}, err
		}
		if !validID(volumeID) {
			return Credential{}, fmt.Errorf("%w: invalid credential volume identity", ErrCorrupt)
		}
		c.VolumeIDs = append(c.VolumeIDs, volumeID)
		if len(c.VolumeIDs) > maxVolumes {
			return Credential{}, fmt.Errorf("%w: credential resource list exceeds its bound", ErrCorrupt)
		}
	}
	if err := rows.Err(); err != nil {
		return Credential{}, err
	}
	if !validCredentialResources(c.Scopes, c.AllOwnedVolumes, c.VolumeIDs) {
		return Credential{}, fmt.Errorf("%w: ambiguous credential resource restrictions", ErrCorrupt)
	}
	return c, nil
}

func (s *Store) CreateCredential(ctx context.Context, input CredentialInput) (Credential, error) {
	if !validDigest(input.SecretDigest) || !validAuthName(input.Label) || len(input.VolumeIDs) > s.options.MaxCredentialVolumes || (input.AllOwnedVolumes && len(input.VolumeIDs) > 0) {
		return Credential{}, fmt.Errorf("%w: invalid credential input", ErrConflict)
	}
	volumes := append([]ID(nil), input.VolumeIDs...)
	sort.Slice(volumes, func(i, j int) bool { return volumes[i] < volumes[j] })
	for i, id := range volumes {
		if id == "" || (i > 0 && volumes[i-1] == id) {
			return Credential{}, ErrConflict
		}
	}
	id, err := NewID()
	if err != nil {
		return Credential{}, err
	}
	var result Credential
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		user, err := requireUserTx(ctx, tx, input.UserID)
		if err != nil {
			return err
		}
		if input.AuthEpoch != user.AuthEpoch {
			return ErrDenied
		}
		scopes, err := canonicalScopes(input.Scopes, user.Role)
		if err != nil {
			return err
		}
		if !validCredentialResources(scopes, input.AllOwnedVolumes, volumes) {
			return ErrConflict
		}
		now := s.now().UTC()
		expires := time.UnixMilli(unixMillis(input.ExpiresAt)).UTC()
		if !expires.After(now) || expires.After(now.Add(s.options.CredentialMaxTTL)) {
			return ErrExpired
		}
		for _, volumeID := range volumes {
			var owner ID
			var state string
			err := tx.QueryRowContext(ctx, "SELECT owner_user_id,state FROM volumes WHERE id=?", volumeID).Scan(&owner, &state)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if owner != input.UserID {
				return ErrDenied
			}
			if state == "deleted" {
				return ErrNotFound
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM credential_volumes WHERE credential_id IN (SELECT id FROM cli_credentials WHERE revoked_at IS NOT NULL OR expires_at<=? OR auth_epoch<>(SELECT auth_epoch FROM users WHERE users.id=cli_credentials.user_id))", unixMillis(now)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM cli_credentials WHERE revoked_at IS NOT NULL OR expires_at<=? OR auth_epoch<>(SELECT auth_epoch FROM users WHERE users.id=cli_credentials.user_id)", unixMillis(now)); err != nil {
			return err
		}
		var total, owned int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(user_id=?),0) FROM cli_credentials", input.UserID).Scan(&total, &owned); err != nil {
			return err
		}
		if total >= s.options.MaxCredentials || owned >= s.options.MaxCredentialsPerUser {
			return ErrCapacity
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM cli_credentials WHERE current_digest=?)", input.SecretDigest).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrConflict
		}
		encoded, err := json.Marshal(scopes)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO cli_credentials(id,user_id,label,scopes,all_owned_volumes,current_digest,generation,auth_epoch,created_at,expires_at) VALUES(?,?,?,?,?,?,1,?,?,?)", id, input.UserID, input.Label, string(encoded), input.AllOwnedVolumes, input.SecretDigest, user.AuthEpoch, unixMillis(now), unixMillis(expires)); err != nil {
			return err
		}
		for _, volumeID := range volumes {
			if _, err := tx.ExecContext(ctx, "INSERT INTO credential_volumes(credential_id,volume_id) VALUES(?,?)", id, volumeID); err != nil {
				return err
			}
		}
		result = Credential{ID: id, UserID: input.UserID, Label: input.Label, Scopes: scopes, AllOwnedVolumes: input.AllOwnedVolumes, VolumeIDs: volumes, AuthEpoch: user.AuthEpoch, Generation: 1, CreatedAt: now, ExpiresAt: expires, currentAuthEpoch: user.AuthEpoch, currentRole: user.Role}
		return nil
	})
	return result, err
}

func (s *Store) GetCredential(ctx context.Context, id ID) (Credential, error) {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Credential{}, err
	}
	defer tx.Rollback()
	credential, err := credentialTx(ctx, tx, id, s.options.MaxCredentialVolumes)
	if err != nil {
		return Credential{}, err
	}
	if err := tx.Commit(); err != nil {
		return Credential{}, err
	}
	return credential, nil
}

func (c Credential) validAt(now time.Time) error {
	if c.RevokedAt != nil || c.AuthEpoch != c.currentAuthEpoch {
		return ErrDenied
	}
	if !now.Before(c.ExpiresAt) {
		return ErrExpired
	}
	for _, scope := range c.Scopes {
		if scope == "admin:summaries" && c.currentRole != RoleAdmin {
			return ErrDenied
		}
	}
	return nil
}

func (s *Store) ValidateCredential(ctx context.Context, id ID, digest []byte) (Credential, error) {
	if !validDigest(digest) {
		return Credential{}, ErrDenied
	}
	c, err := s.GetCredential(ctx, id)
	if err != nil {
		return Credential{}, err
	}
	now := s.now()
	if err := c.validAt(now); err != nil {
		return Credential{}, err
	}
	current := equalDigest(c.currentDigest, digest)
	previous := c.previousValidUntil != nil && now.Before(*c.previousValidUntil) && equalDigest(c.previousDigest, digest)
	if !current && !previous {
		return Credential{}, ErrDenied
	}
	return c, nil
}

func (s *Store) RevokeCredential(ctx context.Context, userID, id ID) error {
	return s.writeTx(ctx, func(tx *sql.Tx) error {
		c, err := credentialTx(ctx, tx, id, s.options.MaxCredentialVolumes)
		if err != nil {
			return err
		}
		if c.UserID != userID {
			return ErrDenied
		}
		_, err = tx.ExecContext(ctx, "UPDATE cli_credentials SET revoked_at=COALESCE(revoked_at,?) WHERE id=?", unixMillis(s.now()), id)
		return err
	})
}

type RotationInput struct {
	UserID             ID
	CredentialID       ID
	ActionID           ID
	ExpectedGeneration int64
	PendingDigest      []byte
	Fingerprint        []byte
}

func expirePendingRotationTx(ctx context.Context, tx *sql.Tx, c Credential, now time.Time) (Credential, bool, error) {
	if c.PendingExpiresAt == nil || now.Before(*c.PendingExpiresAt) {
		return c, false, nil
	}
	if c.Generation == int64(^uint64(0)>>1) {
		return Credential{}, false, ErrCapacity
	}
	// Retiring a prepared generation fences delayed prepare/activate requests even
	// when the active secret is retained. The latest activated receipt stays queryable.
	if _, err := tx.ExecContext(ctx, "UPDATE cli_credentials SET generation=generation+1,pending_digest=NULL,pending_action_id=NULL,pending_fingerprint=NULL,pending_expires_at=NULL WHERE id=?", c.ID); err != nil {
		return Credential{}, false, err
	}
	c.Generation++
	c.PendingActionID = ""
	c.PendingExpiresAt = nil
	c.pendingDigest = nil
	c.pendingFingerprint = nil
	return c, true, nil
}

func (s *Store) PrepareCredentialRotation(ctx context.Context, input RotationInput) (Credential, error) {
	if (!validAuthName(string(input.ActionID)) || len(input.ActionID) > 128) || input.ExpectedGeneration < 1 || !validDigest(input.PendingDigest) || !validDigest(input.Fingerprint) {
		return Credential{}, ErrConflict
	}
	var result Credential
	var admissionErr error
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		c, err := credentialTx(ctx, tx, input.CredentialID, s.options.MaxCredentialVolumes)
		if err != nil {
			return err
		}
		if c.UserID != input.UserID {
			return ErrDenied
		}
		now := s.now().UTC()
		if err := c.validAt(now); err != nil {
			return err
		}
		retiredAction := c.PendingActionID
		c, retired, err := expirePendingRotationTx(ctx, tx, c, now)
		if err != nil {
			return err
		}
		refuse := func(err error) error {
			if retired {
				admissionErr = err
				return nil
			}
			return err
		}
		if retired && retiredAction == input.ActionID {
			return refuse(ErrExpired)
		}
		if c.RotationActionID == input.ActionID {
			if !equalDigest(c.rotationFingerprint, input.Fingerprint) || c.RotationGeneration-1 != input.ExpectedGeneration || !equalDigest(c.currentDigest, input.PendingDigest) {
				return refuse(ErrConflict)
			}
			result = c
			return nil
		}
		if c.Generation != input.ExpectedGeneration {
			return refuse(ErrConflict)
		}
		if c.Generation == int64(^uint64(0)>>1) {
			return refuse(ErrCapacity)
		}
		if c.PendingActionID == input.ActionID {
			if !equalDigest(c.pendingFingerprint, input.Fingerprint) || !equalDigest(c.pendingDigest, input.PendingDigest) {
				return refuse(ErrConflict)
			}
			result = c
			return nil
		}
		if c.PendingExpiresAt != nil {
			return refuse(ErrConflict)
		}
		if equalDigest(c.currentDigest, input.PendingDigest) || equalDigest(c.previousDigest, input.PendingDigest) {
			return refuse(ErrConflict)
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM cli_credentials WHERE id<>? AND (current_digest=? OR pending_digest=? OR previous_digest=?))", input.CredentialID, input.PendingDigest, input.PendingDigest, input.PendingDigest).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return refuse(ErrConflict)
		}
		until := now.Add(s.options.RotationPendingTTL)
		if until.After(c.ExpiresAt) {
			until = c.ExpiresAt
		}
		if _, err := tx.ExecContext(ctx, "UPDATE cli_credentials SET pending_digest=?,pending_action_id=?,pending_fingerprint=?,pending_expires_at=? WHERE id=?", input.PendingDigest, input.ActionID, input.Fingerprint, unixMillis(until), input.CredentialID); err != nil {
			return err
		}
		c.PendingActionID = input.ActionID
		c.PendingExpiresAt = &until
		c.pendingDigest = append([]byte(nil), input.PendingDigest...)
		c.pendingFingerprint = append([]byte(nil), input.Fingerprint...)
		result = c
		return nil
	})
	if err != nil {
		return Credential{}, err
	}
	return result, admissionErr
}

func (s *Store) ActivateCredentialRotation(ctx context.Context, credentialID, actionID ID, pendingDigest []byte, expectedGeneration int64) (Credential, error) {
	if (!validAuthName(string(actionID)) || len(actionID) > 128) || !validDigest(pendingDigest) {
		return Credential{}, ErrDenied
	}
	var result Credential
	var admissionErr error
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		c, err := credentialTx(ctx, tx, credentialID, s.options.MaxCredentialVolumes)
		if err != nil {
			return err
		}
		activated := c.RotationActionID == actionID && equalDigest(c.currentDigest, pendingDigest)
		prepared := c.PendingActionID == actionID && equalDigest(c.pendingDigest, pendingDigest)
		if !activated && !prepared {
			return ErrDenied
		}
		now := s.now().UTC()
		if err := c.validAt(now); err != nil {
			return err
		}
		if activated {
			if expectedGeneration < 1 || expectedGeneration != c.RotationGeneration-1 {
				return ErrConflict
			}
			result = c
			return nil
		}
		if expectedGeneration < 1 || expectedGeneration != c.Generation {
			return ErrConflict
		}
		c, retired, err := expirePendingRotationTx(ctx, tx, c, now)
		if err != nil {
			return err
		}
		if retired {
			admissionErr = ErrExpired
			return nil
		}
		if c.Generation == int64(^uint64(0)>>1) {
			return ErrCapacity
		}
		grace := now.Add(s.options.RotationGrace)
		if grace.After(c.ExpiresAt) {
			grace = c.ExpiresAt
		}
		generation := c.Generation + 1
		if _, err := tx.ExecContext(ctx, "UPDATE cli_credentials SET previous_digest=current_digest,previous_valid_until=?,current_digest=pending_digest,generation=?,rotation_action_id=pending_action_id,rotation_fingerprint=pending_fingerprint,rotation_generation=?,pending_digest=NULL,pending_action_id=NULL,pending_fingerprint=NULL,pending_expires_at=NULL WHERE id=?", unixMillis(grace), generation, generation, credentialID); err != nil {
			return err
		}
		c.previousDigest = c.currentDigest
		c.previousValidUntil = &grace
		c.currentDigest = c.pendingDigest
		c.Generation = generation
		c.RotationActionID = actionID
		c.rotationFingerprint = c.pendingFingerprint
		c.RotationGeneration = generation
		c.PendingActionID = ""
		c.PendingExpiresAt = nil
		c.pendingDigest = nil
		c.pendingFingerprint = nil
		result = c
		return nil
	})
	if err != nil {
		return Credential{}, err
	}
	return result, admissionErr
}
