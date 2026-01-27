ALTER TABLE b2bhub_db_table ADD COLUMN "partnerExternalReference" character varying;

ALTER TABLE b2bhub_db_table ADD COLUMN "metadata" jsonb;

CREATE INDEX b2bhub_db_table_partnerexternalreference_idx on b2bhub_db_table ("partnerExternalReference");
