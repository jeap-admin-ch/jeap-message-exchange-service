-- inbound_message was created without a primary key: the scan status updates join on "sequenceId" and the
-- housekeeping delete filters on "sequenceId", both causing a sequential scan per statement without an index.
-- The unique index is created concurrently to avoid blocking writes to inbound_message; CREATE INDEX
-- CONCURRENTLY cannot run inside a transaction, so this migration is configured with
-- executeInTransaction=false (see V9__add_inbound_message_sequenceid_primary_key.sql.conf): each statement
-- executes separately in autocommit mode. The DROP cleans up a possibly invalid index left behind by a failed
-- earlier attempt. Promoting the index to the primary key only takes a brief ACCESS EXCLUSIVE lock since the
-- index is already built ("sequenceId" is an identity column and therefore NOT NULL already).
DROP INDEX IF EXISTS inbound_message_pkey;
CREATE UNIQUE INDEX CONCURRENTLY inbound_message_pkey ON inbound_message ("sequenceId");
ALTER TABLE inbound_message ADD CONSTRAINT inbound_message_pkey PRIMARY KEY USING INDEX inbound_message_pkey;
