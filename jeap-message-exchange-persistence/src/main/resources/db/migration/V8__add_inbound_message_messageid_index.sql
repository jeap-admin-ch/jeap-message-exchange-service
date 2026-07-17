-- The malware scan result flow only knows the messageId (S3 object key), not the bpId.
-- The index is created concurrently to avoid blocking writes to inbound_message during the rolling deployment.
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction, so this migration is configured with
-- executeInTransaction=false (see V8__add_inbound_message_messageid_index.sql.conf): each statement executes
-- separately in autocommit mode. The DROP cleans up a possibly invalid index left behind by a failed earlier
-- attempt.
DROP INDEX IF EXISTS inbound_message_messageid_idx;
CREATE INDEX CONCURRENTLY inbound_message_messageid_idx ON inbound_message ("messageId");
