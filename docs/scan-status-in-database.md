# MES 11.0.0 — Malware scan status and message metadata moved from S3 tags to PostgreSQL

This document describes the breaking change introduced with jeap-message-exchange-service (MES) 11.0.0,
why it was made, how it behaves during and after the upgrade, and the steps operators need to perform.
Installations still on a version smaller than 11 should upgrade directly to the current MES version (12.1.x or later); the steps
below are written for that upgrade.

## Why

MES versions smaller than 11 stored the metadata of partner (inbound) messages — `bpId`, `messageType`, `partnerTopic`,
`partnerExternalReference`, the malware `scanStatus` and `saveTimeInMillis` — as S3 object tags, and updated the
`scanStatus` tag after each malware scan result using `PutObjectTagging`.

`PutObjectTagging` replaces the **complete** tag set of an object. AWS GuardDuty Malware Protection, when
configured to tag scanned objects, uses the same read-merge-write pattern to add its
`GuardDutyMalwareScanStatus` tag. Two uncoordinated writers doing read-merge-write on a full-replace API is a
classic lost-update race: GuardDuty's write can be based on a stale snapshot taken *before* MES updated the
`scanStatus` tag, silently reverting it (e.g. from `NO_THREATS_FOUND` back to `SCAN_PENDING`). The affected
message then stays blocked with HTTP 403 forever, even though the `B2BMessageReceivedEvent` announcing the
clean scan was already published (JEAP-7230).

## What changed

- **PostgreSQL is the single source of truth.** The metadata and the malware scan status of partner messages
  are persisted in the `inbound_message` table (Flyway migrations `V7`-`V9`). Scan results update the database;
  message delivery is gated by the database scan status.
- **Delivery decisions and duplicate detection read the primary database, never a read replica.** The scan
  status is committed before the `B2BMessageReceivedEvent` is published, and the delivery check reads it from
  the primary, so a consumer can retrieve the message immediately upon receiving the event. Installations
  using `jeap.datasource.replica.enabled=true` are not affected by replication lag in these flows.
- **S3 object tags are no longer authoritative.** Message delivery and event publication are based solely on
  the database. While the legacy tag compatibility is enabled (see below), MES still writes and updates the
  tags exactly like MES < 11 did — but a lost tag update can no longer block a message on upgraded instances. Once the
  compatibility is disabled, tags are written exactly once, atomically within the `PutObject` request at
  object creation (the lifecycle tag), and never updated.
- **Backwards compatibility (tag fallback).** For messages stored by MES &lt; 11.0.0 whose information is
  missing in PostgreSQL, the S3 tags are read as a fallback and the database record is backfilled when a scan
  result arrives. This fallback will be removed with the contract story **JEAP-7252**.
- **Self-healing during rolling deployments.** When the database still says `SCAN_PENDING` at delivery time,
  MES consults the object tags: a terminal `scanStatus` written by an MES < 11 instance that processed the scan
  result is adopted into the database (only while the row is still pending — a concurrently written database
  status always wins over the tag), and delivery proceeds accordingly. Messages that got stuck in
  `SCAN_PENDING` because of the tagging race in **earlier versions** (the verdict survives only in the
  `GuardDutyMalwareScanStatus` tag written by GuardDuty itself) are **not** healed automatically. Repair them
  manually after verifying the scan verdict — for messages with a database record:

  ```sql
  UPDATE inbound_message SET "scanStatus" = 'NO_THREATS_FOUND'
   WHERE "messageId" = '<message-id>' AND "scanStatus" = 'SCAN_PENDING';
  ```

  For legacy messages without a database record, correct the `scanStatus` object tag instead (e.g. with the
  AWS CLI `put-object-tagging`, keeping all other tags).

## Zero-downtime deployments with mixed MES < 11 and upgraded instances

Rolling deployments distribute HTTP requests and malware scan results (SQS) randomly across old and new
instances. To keep this safe, 11.0.0 by default keeps the complete MES < 11 tagging behavior: the metadata tags
are written to new S3 objects at creation, and each malware scan result also updates the `scanStatus` tag
exactly like MES < 11 did. Old instances can therefore gate message delivery and process malware scan results for
messages uploaded by new instances (and vice versa) without any behavior change during the deployment window.
This also makes a rollback to MES < 11 safe.

Note that the transitional `scanStatus` tag update uses the same full-replace tagging API as MES < 11 and can
therefore still be raced by the GuardDuty tagging. On upgraded instances such a lost tag update is harmless —
delivery is gated by the database — so the race can only affect GETs served by MES < 11 instances during the
deployment window, which is exactly the pre-upgrade status quo.

## Upgrade steps

The steps depend on whether malware scanning (`jeap.messageexchange.malwarescan.enabled`) was enabled before
the upgrade.

### Malware scanning was enabled before the upgrade

1. **Deploy the current MES version (12.1.x or later) with the default configuration** (rolling deployment
   is fine). The transitional tag writing is enabled by default
   (`jeap.messageexchange.legacy-tag-compatibility.enabled=true`); the database migrations `V7`-`V9` run
   automatically and are compatible with running MES < 11 instances (additive nullable columns, indexes created
   concurrently).
2. **After all instances run the upgraded version**, disable the transitional tag writing:

   ```yaml
   jeap:
     messageexchange:
       legacy-tag-compatibility:
         enabled: false
   ```

   From then on, new S3 objects carry only the `MessageExchangeLifecyclePolicy` tag, and malware scan results
   no longer update object tags — the GuardDuty tagging race is then structurally impossible. Do **not**
   disable the property while MES < 11 instances are still running — they depend on the metadata tags.
3. **Nothing else to do.** The read-only tag fallback and the healing logic stay active regardless of the
   property and will be removed, together with the property and the transitional tag writing, by the contract
   story **JEAP-7252** in a future major release.

### Malware scanning was disabled before the upgrade

Without malware scanning there are no scan results and no scan status gating, so the transitional tag writing
serves no purpose and can be switched off from the start:

1. **Disable the transitional tag writing directly before the upgrade** by adding the following configuration
   together with the upgrade deployment:

   ```yaml
   jeap:
     messageexchange:
       legacy-tag-compatibility:
         enabled: false
   ```

2. **Deploy the current MES version (12.1.x or later)** (rolling deployment is fine; the database migrations
   `V7`-`V9` run automatically). New S3 objects carry only the `MessageExchangeLifecyclePolicy` tag from the
   first upgraded instance on.
3. **Nothing else to do.** The read-only tag fallback stays active regardless of the property, so messages
   stored by MES < 11 instances remain readable.

### Message body handling options (since 12.1.0)

Upgrading also brings the retry-safe upload handling introduced with 12.1.0: message bodies up to
`jeap.messageexchange.objectstorage.connection.upload-retry-memory-buffer-threshold` (default 1MB) are
buffered in memory so transient S3 errors are retried by the AWS SDK; larger bodies are streamed to S3
without buffering and fail fast with the actual S3 error, relying on the client retrying the idempotent PUT.
The defaults require no action — see [Getting Started](getting-started.md) for the configuration and
[Operations](operations.md) for the error-handling behavior.

## Behavior changes to be aware of

- Requesting a malware-blocked partner message returns **403 based on the database status even if the S3
  object has already expired** (previously 404), since the status check no longer requires reading the object.
- Inbound message database records are retained **two days longer** than the configured
  `jeap.messageexchange.housekeeping.expiration-days`, so that the record (the sole source of the scan status)
  always outlives the corresponding S3 object (which expires one day after the configured expiration). The
  lingering database record does not change the API behavior: requesting a deliverable message whose S3
  object has already expired still returns **404** as before — only malware-blocked messages return 403 (see
  above).
- The metric `jeap_mes_objectstore_update_tags` is only recorded while the legacy tag compatibility is
  enabled, and `jeap_mes_repository_save` was replaced by `jeap_mes_repository_upsert_scan_status_and_metadata`;
  new timers cover the scan status updates and the legacy tag/head reads. Blocked deliveries no longer load
  the object payload from S3 (only the tags of a still-pending message are read once for the healing check).
- Flyway now uses a session-level advisory lock (`spring.flyway.postgresql.transactional-lock=false`, set in
  the service defaults) so that the concurrently-created index in migration `V8` cannot deadlock against
  Flyway's schema history lock.

## Configuration reference

| Property | Default | Description |
| --- | --- | --- |
| `jeap.messageexchange.legacy-tag-compatibility.enabled` | `true` | Keep the MES &lt; 11 tagging behavior: write the metadata tags expected by MES &lt; 11.0.0 to new S3 objects and update the scanStatus tag after malware scan results. With malware scanning enabled, keep enabled until all instances run the upgraded version; with malware scanning disabled, disable it directly before the upgrade. Removed with JEAP-7252. |
| `jeap.messageexchange.objectstorage.connection.upload-retry-memory-buffer-threshold` | `1MB` | Message bodies up to this size are buffered in memory before the S3 upload, so transient S3 errors are retried by the AWS SDK. Larger bodies are streamed without buffering and fail fast with the actual S3 error. |
| `jeap.messageexchange.objectstorage.connection.upload-buffering-enabled` | `true` | When disabled, every message body is streamed directly to S3, where transient S3 errors cannot be retried. Only disable to rule out the upload buffering as a problem cause. |
