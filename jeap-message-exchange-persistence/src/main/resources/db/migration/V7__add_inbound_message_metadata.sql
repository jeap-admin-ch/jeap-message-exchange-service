ALTER TABLE inbound_message
    ADD COLUMN "messageType"              text,
    ADD COLUMN "partnerTopic"             text,
    ADD COLUMN "partnerExternalReference" text,
    ADD COLUMN "contentType"              text,
    ADD COLUMN "scanStatus"               text;
