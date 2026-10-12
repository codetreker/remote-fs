ALTER TABLE volumes ADD COLUMN named_files INTEGER NOT NULL DEFAULT 0 CHECK(typeof(named_files)='integer' AND named_files>=0);
ALTER TABLE volumes ADD COLUMN named_directories INTEGER NOT NULL DEFAULT 0 CHECK(typeof(named_directories)='integer' AND named_directories>=0);
ALTER TABLE volumes ADD COLUMN quota_bytes INTEGER CHECK(quota_bytes IS NULL OR (typeof(quota_bytes)='integer' AND quota_bytes>=0));
ALTER TABLE volumes ADD COLUMN quota_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(quota_revision)='integer' AND quota_revision>=0);
UPDATE volumes SET named_files=(SELECT count(*) FROM entries e JOIN nodes n ON n.id=e.node WHERE e.volume=volumes.id AND n.kind IN(1,3)),
 named_directories=(SELECT count(*) FROM entries e JOIN nodes n ON n.id=e.node WHERE e.volume=volumes.id AND n.kind=2);
CREATE TRIGGER entries_summary_insert AFTER INSERT ON entries BEGIN
 UPDATE volumes SET named_files=named_files+(SELECT kind IN(1,3) FROM nodes WHERE id=NEW.node),
 named_directories=named_directories+(SELECT kind=2 FROM nodes WHERE id=NEW.node) WHERE id=NEW.volume;
END;
CREATE TRIGGER entries_summary_delete BEFORE DELETE ON entries BEGIN
 UPDATE volumes SET named_files=named_files-(SELECT kind IN(1,3) FROM nodes WHERE id=OLD.node),
 named_directories=named_directories-(SELECT kind=2 FROM nodes WHERE id=OLD.node) WHERE id=OLD.volume;
END;
CREATE TRIGGER entries_summary_update BEFORE UPDATE OF node,volume ON entries BEGIN
 UPDATE volumes SET named_files=named_files-(SELECT kind IN(1,3) FROM nodes WHERE id=OLD.node),
 named_directories=named_directories-(SELECT kind=2 FROM nodes WHERE id=OLD.node) WHERE id=OLD.volume;
 UPDATE volumes SET named_files=named_files+(SELECT kind IN(1,3) FROM nodes WHERE id=NEW.node),
 named_directories=named_directories+(SELECT kind=2 FROM nodes WHERE id=NEW.node) WHERE id=NEW.volume;
END;
CREATE TABLE action_subjects(volume INTEGER NOT NULL,subject TEXT NOT NULL,
 floor INTEGER NOT NULL DEFAULT 0, highwater INTEGER NOT NULL DEFAULT 0, logical_sec INTEGER NOT NULL DEFAULT 0,logical_nsec INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(volume,subject)) WITHOUT ROWID;
CREATE TABLE action_namespaces(volume INTEGER NOT NULL,subject TEXT NOT NULL,sequence INTEGER NOT NULL,
 accept_sec INTEGER NOT NULL,accept_nsec INTEGER NOT NULL,admission INTEGER NOT NULL DEFAULT 1 CHECK(admission IN(0,1)), PRIMARY KEY(volume,subject,sequence)) WITHOUT ROWID;
CREATE TABLE authority_actions(volume INTEGER NOT NULL,subject TEXT NOT NULL,namespace INTEGER NOT NULL,nonce TEXT NOT NULL,
 kind INTEGER NOT NULL,fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),outcome INTEGER NOT NULL,
 failure INTEGER NOT NULL DEFAULT 0,result BLOB,acceptance BLOB,created_sec INTEGER NOT NULL,
 PRIMARY KEY(volume,subject,namespace,nonce)) WITHOUT ROWID;
CREATE TABLE imports(volume INTEGER NOT NULL,subject TEXT NOT NULL,namespace INTEGER NOT NULL,nonce TEXT NOT NULL,
 object_key TEXT NOT NULL,command BLOB NOT NULL,sealed INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(volume,subject,namespace,nonce)) WITHOUT ROWID;
CREATE TRIGGER nodes_summary_kind BEFORE UPDATE OF kind ON nodes BEGIN
 UPDATE volumes SET named_files=named_files+
  ((NEW.kind IN(1,3))-(OLD.kind IN(1,3)))*(SELECT count(*) FROM entries WHERE node=OLD.id),
 named_directories=named_directories+((NEW.kind=2)-(OLD.kind=2))*(SELECT count(*) FROM entries WHERE node=OLD.id)
 WHERE id=OLD.volume;
END;
ALTER TABLE imports ADD COLUMN access_binding BLOB NOT NULL DEFAULT X'7B7D';

CREATE UNIQUE INDEX imports_by_object_key ON imports(object_key);
CREATE INDEX authority_pending_by_volume ON authority_actions(volume) WHERE outcome IN(1,4);
