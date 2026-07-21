# Operations

## Housekeeping

Messages are retained for `jeap.messageexchange.housekeeping.expiration-days` (default: 14 days) and then
cleaned up. Database and object storage use different mechanisms, deliberately staggered so that dependent
data never outlives its prerequisites:

| Data | Mechanism | Deleted after |
| --- | --- | --- |
| Outbound rows (`b2bhub_db_table`) | Scheduled housekeeping job, small delete batches per transaction | `expiration-days` |
| S3 objects (both buckets) | S3 lifecycle rules, created and managed by the MES | `expiration-days` + 1 day |
| Inbound rows (`inbound_message`) | Scheduled housekeeping job | `expiration-days` + 2 days |

The +1/+2 staggering guarantees that an outbound message disappears from partner polling before its payload
expires, and that an inbound row — the sole source of the malware scan status — always outlives its S3
object. A request for a deliverable message whose object has expired returns `404` as before; a
malware-blocked message keeps returning `403` from the database status even after object expiry.

If bucket versioning is enabled despite the recommendation against it, noncurrent versions are expired after
one day by the lifecycle rules — but keep versioning **disabled**, see
[Malware Scanning](malware-scanning.md#scan-results-and-s3-object-versions).

On AWS, set `ignore_lifecycle_rule_updates=true` on the buckets so the MES-managed lifecycle rules are
not removed by Terraform/OpenTofu.

## Monitoring

In addition to the standard Spring Boot metrics, the MES publishes the time series below. Unless noted
otherwise, every series is a Micrometer **timer**: it publishes quantile gauges (label
`quantile=0.5|0.8|0.95|0.99`) plus the `*_count`, `*_sum` and `*_max` series (Prometheus names carry a
`_seconds` suffix). Counters are marked as such.

### HTTP API requests

One timer per resource and operation — request rate (`_count`) and latency histogram.

| Time series | Labels | Purpose |
| --- | --- | --- |
| `jeap_mes_partner_controller_send_message` | — | Partner uploads an inbound message (partner API PUT) |
| `jeap_mes_partner_controller_get_message` | — | Partner downloads an outbound message payload |
| `jeap_mes_partner_controller_get_messages` | — | Partner polls the list of new outbound messages |
| `jeap_mes_partner_controller_get_next_message` | — | Partner fetches the next outbound message after a cursor |
| `jeap_mes_internal_controller_send_message` | — | Internal application publishes an outbound message (internal API PUT) |
| `jeap_mes_internal_controller_get_message` | — | Internal application downloads an inbound message |

### Database operations

One timer per repository operation — query rate and duration.

| Time series | Labels | Purpose |
| --- | --- | --- |
| `jeap_mes_repository_save` | — | Insert of an outbound message row |
| `jeap_mes_repository_find_by_message_id` | — | Outbound message lookup by `messageId` |
| `jeap_mes_repository_find_by_bp_id_message_id` | — | Message lookup by `bpId` and `messageId` (outbound downloads and the inbound duplicate check) |
| `jeap_mes_repository_get_messages` | — | Partner polling query (message list after cursor) |
| `jeap_mes_repository_get_next_message_id` | — | Next-outbound-message query |
| `jeap_mes_repository_find_latest_by_message_id` | — | Newest inbound row by `messageId` (the delivery gate read) |
| `jeap_mes_repository_upsert_scan_status_and_metadata` | — | Inbound row upsert at upload, and legacy backfill from a scan result |
| `jeap_mes_repository_update_scan_status` | — | Scan status update when a malware scan result arrives |
| `jeap_mes_repository_update_scan_status_if_pending` | — | Healing of a pending scan status on read (transitional, JEAP-7252) |
| `jeap_mes_repository_delete_expired` | — | Housekeeping delete batches (outbound and inbound rows) |

### S3 operations

One timer per access pattern — request rate and S3 response time.

| Time series | Labels | Purpose |
| --- | --- | --- |
| `jeap_mes_objectstore_put` | — | Payload upload (`PutObject`, tags included atomically) |
| `jeap_mes_objectstore_get` | `with_tags=false` | Payload download |
| `jeap_mes_objectstore_get` | `with_tags=true` | Payload download including object tags (legacy tag fallback) |
| `jeap_mes_objectstore_get_content_type` | `with_tags=true` (fixed, historical) | Content type lookup via object head |
| `jeap_mes_objectstore_get_head` | — | Object metadata lookup (`HeadObject`, legacy backfill) |
| `jeap_mes_objectstore_get_tags` | — | Object tag read (legacy fallback and healing check) |
| `jeap_mes_objectstore_update_tags` | — | Transitional scanStatus tag update after a scan result — only recorded while legacy tag compatibility is enabled (JEAP-7252) |

### Malware scanning

| Time series | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `jeap_mes_malware_scan_duration_timer` | Timer | `applicationName` | Roundtrip from storing the message in S3 until its scan result arrives; only recorded when the message save time is known |
| `jeap_mes_malware_scan_result_counter` | Counter | `applicationName`, `scanResult=NO_THREATS_FOUND\|THREATS_FOUND\|UNSUPPORTED\|ACCESS_DENIED\|FAILED` | Number of processed malware scan results per verdict |

### Other

| Time series | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `jeap_mes_duplicated_message_id_received` | Counter | `bp_id` | Uploads reusing an already-known `messageId`, per business partner (idempotent retries and 409-rejected conflicts) |
| `jeap_mes_housekeeping` | Timer (no quantiles) | — | Duration of the scheduled housekeeping run |

Watch in particular:

- `jeap_mes_malware_scan_result_counter` vs. partner upload counts — a widening gap means messages are stuck
  in `SCAN_PENDING` (e.g. bucket scanning disabled while MES scanning is enabled).
- Error-level log statements from scan result processing — scan results are acknowledged even when
  processing fails, so the log is the only trace (see
  [Message Flows](message-flows.md#inbound-malware-scan-result-processing)).

## Logging

All log statements carry structured jEAP logging parameters via the SLF4J MDC where applicable: `messageId`,
`bpId`, and message attributes such as type, topic, group and external reference.

| Level | Use case |
| --- | --- |
| DEBUG | Detailed request/response logs, GET request logs |
| INFO | PUT/POST request logs (one statement per request) |
| WARN | Retryable/temporary errors (DB, S3), client errors (invalid requests), dropped scan results |
| ERROR | Permanent errors, failed scan result processing, blocked message deliveries |

## Error handling

The MES returns the same HTTP status codes as the B2B Hub v2 (see [REST API](rest-api.md)). All persistence
operations are implemented idempotently and are safe for client retries — see
[Message Flows](message-flows.md#idempotence) for the complete picture.

| Error class | Handling |
| --- | --- |
| Temporary error (DB connection, S3 temporarily unavailable) | Logged at WARN, retried; then logged at ERROR and answered with an appropriate error status. S3 upload retries only apply to bodies up to `upload-retry-memory-buffer-threshold` (default 1 MB), which are buffered in memory; larger bodies are streamed and fail fast with the actual S3 error — the client should retry the idempotent PUT |
| Permanent error | Logged at ERROR, answered with an appropriate error status |
| Failed malware scan result processing | Logged at ERROR, SQS message acknowledged (no redelivery); the message stays fail-closed |
