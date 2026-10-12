CREATE TABLE database_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    id TEXT NOT NULL UNIQUE CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE TABLE users (
    id TEXT PRIMARY KEY CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*'),
    role TEXT NOT NULL CHECK(role IN ('admin','user')),
    display_name TEXT NOT NULL CHECK(length(display_name) BETWEEN 1 AND 128),
    auth_epoch INTEGER NOT NULL CHECK(auth_epoch>=1),
    next_action_namespace INTEGER NOT NULL CHECK(next_action_namespace>=1),
    retired_before_namespace INTEGER NOT NULL CHECK(retired_before_namespace>=0 AND retired_before_namespace<next_action_namespace),
    logical_time_high_water INTEGER NOT NULL CHECK(logical_time_high_water>=0),
    retired_native_revision INTEGER NOT NULL DEFAULT 0 CHECK(retired_native_revision>=0),
    retired_native_proof BLOB NOT NULL DEFAULT X'',
    created_at INTEGER NOT NULL CHECK(created_at>=0)
) STRICT;

CREATE TABLE local_admin_credentials (
    user_id TEXT PRIMARY KEY,
    login_name TEXT NOT NULL UNIQUE CHECK(length(login_name) BETWEEN 1 AND 128),
    password_hash TEXT NOT NULL CHECK(length(password_hash) BETWEEN 1 AND 4096),
    password_version INTEGER NOT NULL CHECK(password_version>=1),
    changed_at INTEGER NOT NULL CHECK(changed_at>=0)
) STRICT;

CREATE TABLE identity_bindings (
    issuer TEXT NOT NULL CHECK(length(issuer) BETWEEN 1 AND 2048),
    subject TEXT NOT NULL CHECK(length(subject) BETWEEN 1 AND 512),
    user_id TEXT NOT NULL,
    PRIMARY KEY(issuer,subject)
) STRICT;

CREATE TABLE sessions (
    id TEXT PRIMARY KEY CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*'),
    user_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind='browser'),
    secret_digest BLOB NOT NULL UNIQUE CHECK(length(secret_digest)=32),
    csrf_token TEXT NOT NULL CHECK(length(csrf_token) BETWEEN 32 AND 256),
    auth_epoch INTEGER NOT NULL CHECK(auth_epoch>=1),
    created_at INTEGER NOT NULL CHECK(created_at>=0),
    last_used_at INTEGER NOT NULL CHECK(last_used_at>=created_at),
    idle_expires_at INTEGER NOT NULL CHECK(idle_expires_at>=last_used_at),
    absolute_expires_at INTEGER NOT NULL CHECK(absolute_expires_at>created_at),
    revoked_at INTEGER CHECK(revoked_at>=0),
    CHECK(idle_expires_at<=absolute_expires_at)
) STRICT;
CREATE INDEX session_owner ON sessions(user_id);

CREATE TABLE cli_credentials (
    id TEXT PRIMARY KEY CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*'),
    user_id TEXT NOT NULL,
    label TEXT NOT NULL CHECK(length(label) BETWEEN 1 AND 128),
    scopes TEXT NOT NULL CHECK(json_valid(scopes) AND json_type(scopes)='array'),
    all_owned_volumes INTEGER NOT NULL CHECK(all_owned_volumes IN (0,1)),
    current_digest BLOB NOT NULL UNIQUE CHECK(length(current_digest)=32),
    generation INTEGER NOT NULL CHECK(generation>=1),
    pending_digest BLOB CHECK(length(pending_digest)=32),
    pending_action_id TEXT,
    pending_fingerprint BLOB CHECK(length(pending_fingerprint)=32),
    pending_expires_at INTEGER CHECK(pending_expires_at>=0),
    previous_digest BLOB CHECK(length(previous_digest)=32),
    previous_valid_until INTEGER CHECK(previous_valid_until>=0),
    rotation_action_id TEXT,
    rotation_fingerprint BLOB CHECK(length(rotation_fingerprint)=32),
    rotation_generation INTEGER CHECK(rotation_generation>=2),
    auth_epoch INTEGER NOT NULL CHECK(auth_epoch>=1),
    created_at INTEGER NOT NULL CHECK(created_at>=0),
    expires_at INTEGER NOT NULL CHECK(expires_at>created_at),
    revoked_at INTEGER CHECK(revoked_at>=0),
    CHECK((pending_digest IS NULL AND pending_action_id IS NULL AND pending_fingerprint IS NULL AND pending_expires_at IS NULL)
       OR (pending_digest IS NOT NULL AND pending_action_id IS NOT NULL AND pending_fingerprint IS NOT NULL AND pending_expires_at IS NOT NULL)),
    CHECK((previous_digest IS NULL)=(previous_valid_until IS NULL)),
    CHECK((rotation_action_id IS NULL AND rotation_fingerprint IS NULL AND rotation_generation IS NULL)
       OR (rotation_action_id IS NOT NULL AND rotation_fingerprint IS NOT NULL AND rotation_generation IS NOT NULL))
) STRICT;
CREATE INDEX credential_owner ON cli_credentials(user_id);

CREATE TABLE credential_volumes (
    credential_id TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    PRIMARY KEY(credential_id,volume_id)
) STRICT;

CREATE TABLE action_namespaces (
    actor_user_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    accept_until INTEGER NOT NULL CHECK(accept_until>=0),
    retired_at INTEGER CHECK(retired_at>=0),
    PRIMARY KEY(actor_user_id,sequence)
) STRICT;

CREATE TABLE namespace_volume_participants (
    actor_user_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    volume_id TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK(created_at>=0),
    PRIMARY KEY(actor_user_id,sequence,volume_id)
) STRICT;
CREATE INDEX participant_volume ON namespace_volume_participants(volume_id);

CREATE TABLE volumes (
    id TEXT PRIMARY KEY CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*'),
    owner_user_id TEXT NOT NULL,
    display_name TEXT NOT NULL CHECK(length(display_name) BETWEEN 1 AND 128),
    storage_name TEXT NOT NULL UNIQUE CHECK(storage_name=id),
    state TEXT NOT NULL CHECK(state IN ('creating','active','deleting','deleted','failed')),
    management_revision INTEGER NOT NULL CHECK(management_revision>=1),
    last_operation_id TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK(created_at>=0),
    updated_at INTEGER NOT NULL CHECK(updated_at>=created_at)
) STRICT;
CREATE UNIQUE INDEX volume_owner_name ON volumes(owner_user_id,display_name) WHERE state<>'deleted';
CREATE INDEX volume_owner ON volumes(owner_user_id,id);

CREATE TRIGGER immutable_volume_identity BEFORE UPDATE OF id,owner_user_id,storage_name ON volumes
WHEN NEW.id<>OLD.id OR NEW.owner_user_id<>OLD.owner_user_id OR NEW.storage_name<>OLD.storage_name
BEGIN SELECT RAISE(ABORT,'volume identity and ownership are immutable'); END;

CREATE TABLE operations (
    id TEXT PRIMARY KEY CHECK(length(id)=32 AND id NOT GLOB '*[^0-9a-f]*'),
    actor_user_id TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('create','rename','quota','delete')),
    action_namespace INTEGER NOT NULL CHECK(action_namespace>=1),
    action_nonce TEXT NOT NULL,
    fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),
    expected_revision INTEGER NOT NULL CHECK(expected_revision>=0),
    payload BLOB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed')),
    phase TEXT NOT NULL CHECK(phase IN ('accepted','initializing','bound','applying','frozen','verified-close','quarantined','reclaiming','completed')),
    revision INTEGER NOT NULL CHECK(revision>=1),
    verified_root_proof BLOB,
    planned_reclaim_proof BLOB,
    quarantine_leaf TEXT NOT NULL,
    reclaiming_proof BLOB,
    result BLOB,
    error_code TEXT NOT NULL,
    settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0,1)),
    settlement_proof BLOB,
    created_at INTEGER NOT NULL CHECK(created_at>=0),
    updated_at INTEGER NOT NULL CHECK(updated_at>=created_at),
    UNIQUE(actor_user_id,action_namespace,action_nonce),
    CHECK(settled=0 OR state IN ('succeeded','failed'))
) STRICT;
CREATE INDEX operation_pending ON operations(state,id);
