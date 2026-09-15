package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Policy 1 binds persisted names to Unicode 15.0 simple uppercase. Changing the
// table requires a new policy and explicit validation of existing volumes.
const windowsNamePolicyVersion uint32 = 1
const windowsNameUnicodeVersion = "15.0.0"

func windowsNameKey(name []byte) (string, error) {
	if unicode.Version != windowsNameUnicodeVersion {
		return "", syscall.ENOSYS
	}
	if len(name) == 0 || !utf8.Valid(name) {
		return "", syscall.EINVAL
	}
	text := string(name)
	if strings.HasSuffix(text, ".") || strings.HasSuffix(text, " ") {
		return "", syscall.EINVAL
	}
	units := 0
	for _, r := range text {
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return "", syscall.EINVAL
		}
		units++
		if r > 0xffff {
			units++
		}
		if units > 255 {
			return "", syscall.ENAMETOOLONG
		}
	}
	key := strings.ToUpper(text)
	base, _, _ := strings.Cut(key, ".")
	// Win32 treats spaces before the extension as part of the reserved-device
	// spelling: CON .txt must not become an apparently ordinary file.
	base = strings.TrimRight(base, " ")
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return "", syscall.EINVAL
	}
	if len(base) > 3 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		suffix := base[3:]
		if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' || suffix == "¹" || suffix == "²" || suffix == "³" {
			return "", syscall.EINVAL
		}
	}
	return key, nil
}

func (s *Store) windowsNameVersion(ctx context.Context, tx *sql.Tx) (uint32, error) {
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT windows_name_version FROM volumes WHERE id = ?`, s.volume).Scan(&version); err != nil {
		return 0, err
	}
	if version != 0 && version != int64(windowsNamePolicyVersion) {
		return 0, fmt.Errorf("unknown Windows name policy %d: %w", version, syscall.EIO)
	}
	if version != 0 && unicode.Version != windowsNameUnicodeVersion {
		return 0, syscall.ENOSYS
	}
	return uint32(version), nil
}

// scanWindowsNames bounds retained keys and driver allocations before exposing a
// name. The caller's transaction pins one graph throughout validation.
func (s *Store) scanWindowsNames(ctx context.Context, tx *sql.Tx, parent *int64, visit func(int64, int64, []byte, string) error) error {
	query := `SELECT parent, node, length(name), CASE WHEN length(name) <= 1020 THEN name ELSE NULL END FROM entries WHERE volume = ?`
	args := []any{s.volume}
	if parent != nil {
		query += ` AND parent = ?`
		args = append(args, *parent)
	}
	query += ` ORDER BY parent, name`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	remaining := s.maxIntegrityBytes
	count := int64(0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		count++
		if count > s.maxIntegrityRecords {
			return syscall.EFBIG
		}
		var owner, node, size int64
		var name []byte
		if err := rows.Scan(&owner, &node, &size, &name); err != nil {
			return err
		}
		// Account for the row, original bytes and the uppercase comparison key.
		if size < 0 || size > 1020 {
			return syscall.ENAMETOOLONG
		}
		cost := int64(256) + size*4
		if cost > remaining {
			return syscall.EFBIG
		}
		remaining -= cost
		key, err := windowsNameKey(name)
		if err != nil {
			return err
		}
		if err := visit(owner, node, name, key); err != nil {
			return err
		}
	}
	return rows.Err()
}

// activateWindowsLocked runs under coordinator.commit, inside the same write
// transaction that publishes the durable policy. A failed scan changes nothing.
func (s *Store) activateWindowsLocked(ctx context.Context, tx *sql.Tx) error {
	if unicode.Version != windowsNameUnicodeVersion {
		return syscall.ENOSYS
	}
	version, err := s.windowsNameVersion(ctx, tx)
	if err != nil {
		return err
	}
	if version == windowsNamePolicyVersion {
		return nil
	}
	var previous int64
	keys := make(map[string]struct{})
	graph := make(map[int64]*windowsPathNode)
	if err := s.scanWindowsNames(ctx, tx, nil, func(parent, node int64, name []byte, key string) error {
		if parent != previous {
			clear(keys)
			previous = parent
		}
		if _, exists := keys[key]; exists {
			return syscall.EEXIST
		}
		keys[key] = struct{}{}
		if node <= 0 || node == s.root || parent <= 0 {
			return syscall.EIO
		}
		if _, exists := graph[node]; exists {
			return syscall.EIO
		}
		graph[node] = &windowsPathNode{parent: parent, part: windowsPathPart(name)}
		return nil
	}); err != nil {
		return err
	}
	if err := validateWindowsPaths(ctx, s.root, graph); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE volumes SET windows_name_version = ? WHERE id = ?`, windowsNamePolicyVersion, s.volume)
	return err
}

func (s *Store) windowsNameAllowed(ctx context.Context, tx *sql.Tx, parent int64, name []byte, exceptID int64) error {
	version, err := s.windowsNameVersion(ctx, tx)
	if err != nil || version == 0 {
		return err
	}
	key, err := windowsNameKey(name)
	if err != nil {
		return err
	}
	if err := s.scanWindowsNames(ctx, tx, &parent, func(_ int64, node int64, _ []byte, existing string) error {
		if node != exceptID && key == existing {
			return syscall.EEXIST
		}
		return nil
	}); err != nil {
		return err
	}
	return s.windowsPathAllowed(ctx, tx, parent, name, exceptID)
}

// windowsLookup resolves the same folded key used by admission. A second match
// is corruption, never a reason to pick whichever row the driver returned first.
func (s *Store) windowsLookup(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (metastore.Node, bool, error) {
	version, err := s.windowsNameVersion(ctx, tx)
	if err != nil {
		return metastore.Node{}, false, err
	}
	if version == 0 {
		return metastore.Node{}, false, syscall.ENOSYS
	}
	key, err := windowsNameKey(name)
	if err != nil {
		return metastore.Node{}, false, err
	}
	var id int64
	found := false
	err = s.scanWindowsNames(ctx, tx, &parent, func(_ int64, node int64, _ []byte, existing string) error {
		if existing == key {
			if found {
				return syscall.EIO
			}
			found = true
			id = node
		}
		return nil
	})
	if err != nil || !found {
		return metastore.Node{}, false, err
	}
	node, err := nodeByID(ctx, tx, id)
	return node, err == nil, err
}
