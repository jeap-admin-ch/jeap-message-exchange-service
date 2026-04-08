CREATE TABLE inbound_message
(
    "sequenceId"    bigint            NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1),
    "messageId"     character varying NOT NULL,
    "bpId"          character varying NOT NULL,
    "contentLength" numeric           NOT NULL,
    "createdAt"     timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT bpId_messageId UNIQUE ("bpId", "messageId")
);

CREATE INDEX inbound_message_bpid_messageid_idx on inbound_message ("bpId", "messageId");
CREATE INDEX inbound_message_createdat_idx on inbound_message ("createdAt");
