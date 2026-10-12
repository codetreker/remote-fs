package schema

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateServiceAuthority(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = " AND v.id=?"
		args = []any{*volume}
	}
	var invalid int64
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM volumes v WHERE (
 typeof(v.named_files)!='integer' OR typeof(v.named_directories)!='integer' OR
 v.named_files!=(SELECT count(*) FROM entries e JOIN nodes n ON n.id=e.node WHERE e.volume=v.id AND n.kind IN(1,3)) OR
 v.named_directories!=(SELECT count(*) FROM entries e JOIN nodes n ON n.id=e.node WHERE e.volume=v.id AND n.kind=2) OR
 typeof(v.quota_revision)!='integer' OR
 (v.quota_bytes IS NULL AND v.quota_revision!=0) OR
 (v.quota_bytes IS NOT NULL AND (typeof(v.quota_bytes)!='integer' OR v.quota_bytes<0 OR v.quota_revision<1)))`+where, args...).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("invalid summary or quota authority: %w", syscall.EIO)
	}
	return validateActionRelationships(ctx, db, volume)
}

func validateActionRelationships(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	filter := ""
	var args []any
	if volume != nil {
		filter = " AND x.volume=?"
		args = []any{*volume}
	}
	queries := []string{
		`SELECT count(*) FROM action_subjects x LEFT JOIN volumes v ON v.id=x.volume WHERE (
 v.id IS NULL OR typeof(x.subject)!='text' OR length(CAST(x.subject AS BLOB)) NOT BETWEEN 1 AND 128 OR
 typeof(x.floor)!='integer' OR x.floor<0 OR typeof(x.highwater)!='integer' OR x.highwater<0 OR
 typeof(x.logical_sec)!='integer' OR typeof(x.logical_nsec)!='integer' OR x.logical_nsec NOT BETWEEN 0 AND 999999999)`,
		`SELECT count(*) FROM action_namespaces x LEFT JOIN action_subjects s ON s.volume=x.volume AND s.subject=x.subject WHERE (
 s.volume IS NULL OR typeof(x.sequence)!='integer' OR x.sequence<1 OR x.sequence<s.floor OR x.sequence>s.highwater OR
 typeof(x.accept_sec)!='integer' OR typeof(x.accept_nsec)!='integer' OR x.accept_nsec NOT BETWEEN 0 AND 999999999 OR typeof(x.admission)!='integer' OR x.admission NOT IN(0,1))`,
		`SELECT count(*) FROM authority_actions x LEFT JOIN action_namespaces n ON n.volume=x.volume AND n.subject=x.subject AND n.sequence=x.namespace WHERE (
 n.volume IS NULL OR typeof(x.nonce)!='text' OR length(CAST(x.nonce AS BLOB)) NOT BETWEEN 1 AND 128 OR
 typeof(x.kind)!='integer' OR x.kind NOT IN(1,2) OR typeof(x.fingerprint)!='blob' OR length(x.fingerprint)!=32 OR
 typeof(x.outcome)!='integer' OR x.outcome NOT BETWEEN 1 AND 5 OR typeof(x.failure)!='integer' OR x.failure NOT BETWEEN 0 AND 2147483647 OR
 (x.failure!=0 AND x.outcome!=3) OR typeof(x.created_sec)!='integer' OR CASE WHEN typeof(x.result)='blob' AND length(x.result)<=16384 THEN json_valid(x.result) ELSE 0 END=0 OR (x.acceptance IS NOT NULL AND CASE WHEN typeof(x.acceptance)='blob' AND length(x.acceptance)<=16384 THEN json_valid(x.acceptance) ELSE 0 END=0) OR (x.kind=1 AND x.outcome=1 AND x.acceptance IS NULL))`,
		`SELECT count(*) FROM imports x
 LEFT JOIN authority_actions a ON a.volume=x.volume AND a.subject=x.subject AND a.namespace=x.namespace AND a.nonce=x.nonce
 LEFT JOIN objects o ON o.key=x.object_key
 WHERE (a.volume IS NULL OR a.kind!=2 OR a.outcome!=1 OR o.key IS NULL OR o.volume!=x.volume OR
 typeof(x.sealed)!='integer' OR x.sealed NOT IN(0,1) OR o.state NOT IN(0,3) OR (x.sealed=1 AND (o.state!=0 OR typeof(o.digest)!='blob' OR length(o.digest)!=32)) OR
 CASE WHEN typeof(x.command)='blob' AND length(x.command)<=262144 THEN json_valid(x.command) ELSE 0 END=0 OR CASE WHEN typeof(x.access_binding)='blob' AND length(x.access_binding)<=16384 THEN json_valid(x.access_binding) ELSE 0 END=0)`,
	}
	for _, query := range queries {
		var invalid int64
		if err := db.QueryRowContext(ctx, query+filter, args...).Scan(&invalid); err != nil {
			return err
		}
		if invalid != 0 {
			return fmt.Errorf("invalid durable action relationship: %w", syscall.EIO)
		}
	}
	var missing int64
	where := ""
	if volume != nil {
		where = " AND a.volume=?"
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM authority_actions a WHERE a.kind=2 AND a.outcome=1 AND NOT EXISTS(SELECT 1 FROM imports i WHERE i.volume=a.volume AND i.subject=a.subject AND i.namespace=a.namespace AND i.nonce=a.nonce)`+where, args...).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return fmt.Errorf("pending import lost its reservation: %w", syscall.EIO)
	}
	return nil
}

func serviceIntegrityWork(ctx context.Context, db sqlvalue.Queryer, volume *int64, limit int64, count *int64) error {
	var tables int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN('action_subjects','action_namespaces','authority_actions','imports')`).Scan(&tables); err != nil {
		return err
	}
	if tables == 0 {
		return nil
	}
	if tables != 4 {
		return fmt.Errorf("incomplete durable action schema: %w", syscall.EIO)
	}
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume=?"
		args = []any{*volume}
	}
	var total int64
	for _, table := range []string{"action_subjects", "action_namespaces", "authority_actions", "imports"} {
		var records int64
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+table+where, args...).Scan(&records); err != nil {
			return err
		}
		if records > limit-total {
			return fmt.Errorf("durable actions exceed integrity record limit: %w", syscall.EFBIG)
		}
		total += records
	}
	*count = total
	return nil
}
