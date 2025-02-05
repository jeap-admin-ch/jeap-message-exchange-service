CREATE TABLE b2bhub_db_table
(
    "sequenceId"    bigint                      NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1),
    "messageId"     character varying           NOT NULL,
    "bpId"          character varying           NOT NULL,
    "topicName"     character varying           NOT NULL,
    "groupId"       character varying,
    "messageType"   character varying           NOT NULL,
    "datePublished" timestamp without time zone NOT NULL DEFAULT now(),
    "partnerTopic"  character varying,
    CONSTRAINT messageId UNIQUE ("messageId")
);

CREATE INDEX b2bhub_db_table_bpid_idx on b2bhub_db_table ("bpId");
CREATE INDEX b2bhub_db_table_topicname_idx on b2bhub_db_table ("topicName");
CREATE INDEX b2bhub_db_table_groupid_idx on b2bhub_db_table ("groupId");
CREATE INDEX b2bhub_db_table_partnertopic_idx on b2bhub_db_table ("partnerTopic");
CREATE INDEX b2bhub_db_table_bpid_sequenceid_idx on b2bhub_db_table ("bpId", "sequenceId");
CREATE INDEX b2bhub_db_table_datepublished_idx on b2bhub_db_table ("datePublished");