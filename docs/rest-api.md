# REST API

The MES exposes two APIs: the **partner API** (used by business partners through a B2B gateway) and the
**internal API** (used by internal applications). Both are fully backwards-compatible with the B2B Hub API.
A running instance serves the OpenAPI specifications via springdoc, grouped per API version
(Swagger UI at `/swagger-ui.html`, OpenAPI JSON at `/v3/api-docs/{group}`).

| API | Base path | Status |
| --- | --- | --- |
| [Partner V4](#partner-v4) | `/api/partner/v4/messages` | Current |
| [Partner V3](#partner-v3-deprecated) | `/api/partner/v3/messages` | Deprecated since 4.0.0, for removal |
| [Internal V3](#internal-v3) | `/api/internal/v3/messages` | Current |

Message payloads are streamed in the request/response body as-is. `mes-metadata` carries a Base64-encoded
JSON object of string key/value pairs (validated against `schema/mes-metadata-schema.json`); it is accepted
on the internal publish operation and returned on the partner V4 download and next-message operations.

## Partner V4

`Content-Type` / `Accept` headers are mandatory, media types are configurable
(see [Media types and size limit](#media-types-and-size-limit)).

| Operation | Method and path |
| --- | --- |
| Upload a message | `PUT /api/partner/v4/messages/{messageId}` |
| Poll for new messages | `GET /api/partner/v4/messages` |
| Download a message | `GET /api/partner/v4/messages/{messageId}` |
| Get next message | `GET /api/partner/v4/messages/{lastMessageId}/next` |

Request headers and query parameters:

| Name | Kind | Operations | Required | Description |
| --- | --- | --- | --- | --- |
| `bp-id` | Header | all | yes | Business partner identification; the token must hold the partner roles for this `bpId` |
| `message-type` | Header | upload | yes | Business type of the message body |
| `Content-Type` | Header | upload | yes | Media type of the payload; must be one of the configured media types |
| `Accept` | Header | download | yes | Expected media type; must match the stored content type of the message |
| `partner-topic` | Header | upload | no | Partner topic stored with the message |
| `partner-external-reference` | Header | upload | no | Partner-defined external reference stored with the message |
| `topicName` | Query | poll, next | no | Return only messages published into the given topic |
| `groupId` | Query | poll | no | Return only messages with the given group ID |
| `lastMessageId` | Query | poll | no | Polling cursor — return only messages published after this message |
| `partnerTopic` | Query | poll, next | no | Return only messages with the given partner topic |
| `partnerExternalReference` | Query | poll, next | no | Return only messages with the given partner external reference |
| `size` | Query | poll | no | Maximum number of messages returned (default 1000) |

Response headers on download and next-message:

| Name | Present | Description |
| --- | --- | --- |
| `message-id` | always | `messageId` of the returned message |
| `partner-topic` | when set | Partner topic of the message |
| `partner-external-reference` | when set | Partner-defined external reference of the message |
| `mes-metadata` | when set | Base64-encoded JSON metadata of the message |

### Example: upload a message (inbound)

```
PUT /api/partner/v4/messages/cc7d5097-4d3f-4fff-af91-fd3680199642 HTTP/1.1
Authorization: Bearer <token issued by the B2B gateway>
bp-id: 123
message-type: InvoiceMessage
partner-topic: invoices
Content-Type: application/xml

<invoice>...</invoice>
```

```
HTTP/1.1 201 Created
```

Re-sending the identical payload for the same `messageId` is an idempotent no-op (`201`); a different
payload for the same `messageId` is rejected with `409 Conflict`.

### Example: poll for new messages (outbound)

`lastMessageId` is the polling cursor — pass the last `messageId` already processed, omit it to start from
the beginning of the retention window:

```
GET /api/partner/v4/messages?topicName=orders&lastMessageId=7f3f6e0a-1b2c-4d5e-8f90-123456789abc&size=100 HTTP/1.1
Authorization: Bearer <token issued by the B2B gateway>
bp-id: 123
```

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "messages": [
    {
      "messageId": "9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d",
      "messageType": "OrderResponseMessage",
      "groupId": "batch-7",
      "partnerTopic": "orders",
      "contentType": "application/xml",
      "partnerExternalReference": "ref-4711",
      "metadata": { "orderId": "4711" }
    }
  ]
}
```

Empty fields are omitted from the JSON response.

### Example: download a message (outbound)

```
GET /api/partner/v4/messages/9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d HTTP/1.1
Authorization: Bearer <token issued by the B2B gateway>
bp-id: 123
Accept: application/xml
```

```
HTTP/1.1 200 OK
message-id: 9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d
partner-topic: orders
partner-external-reference: ref-4711
mes-metadata: eyJvcmRlcklkIjoiNDcxMSJ9
Content-Length: 987

<orderResponse>...</orderResponse>
```

`GET /api/partner/v4/messages/{lastMessageId}/next` behaves like the download but returns the next message
published after `lastMessageId` (or `404` if there is none).

## Partner V3 (deprecated)

Deprecated since 4.0.0 and planned for removal — use [Partner V4](#partner-v4). The operations mirror the
V4 API, but only `application/xml` is supported (regardless of the configured media types), there is no
`partner-external-reference` / metadata support, and the download returns the payload without response
headers (next-message returns only `message-id`).

| Operation | Method and path |
| --- | --- |
| Upload a message (XML) | `PUT /api/partner/v3/messages/{messageId}` |
| Poll for new messages (XML) | `GET /api/partner/v3/messages` |
| Download a message (XML) | `GET /api/partner/v3/messages/{messageId}` |
| Get next message (XML) | `GET /api/partner/v3/messages/{lastMessageId}/next` |

Request headers and query parameters:

| Name | Kind | Operations | Required | Description |
| --- | --- | --- | --- | --- |
| `bp-id` | Header | all | yes | Business partner identification; the token must hold the partner roles for this `bpId` |
| `message-type` | Header | upload | yes | Business type of the message body |
| `topicName` | Query | poll, next | no | Return only messages published into the given topic |
| `groupId` | Query | poll | no | Return only messages with the given group ID |
| `lastMessageId` | Query | poll | no | Polling cursor — return only messages published after this message |
| `partnerTopic` | Query | poll, next | no | Return only messages with the given partner topic |
| `size` | Query | poll | no | Maximum number of messages returned (default 1000) |

The poll returns the message list as XML (`<messages><message>...</message></messages>`, without
`contentType`, `partnerExternalReference` and `metadata`).

## Internal V3

`Content-Type` / `Accept` headers are mandatory, media types are configurable
(see [Media types and size limit](#media-types-and-size-limit)).

| Operation | Method and path |
| --- | --- |
| Publish a message to a partner | `PUT /api/internal/v3/messages/{messageId}?topicName=...` |
| Download an inbound partner message | `GET /api/internal/v3/messages/{messageId}` |

Request headers and query parameters:

| Name | Kind | Operations | Required | Description |
| --- | --- | --- | --- | --- |
| `bp-id` | Header | publish | yes | Identification of the receiving business partner |
| `message-type` | Header | publish | yes | Business type of the message body |
| `Content-Type` | Header | publish | yes | Media type of the payload; must be one of the configured media types |
| `Accept` | Header | download | yes | Expected media type; must match the stored content type of the message |
| `partner-topic` | Header | publish | no | Partner topic stored with the message |
| `partner-external-reference` | Header | publish | no | Partner-defined external reference stored with the message |
| `mes-metadata` | Header | publish | no | Base64-encoded JSON object of string key/value pairs, returned to the partner on download |
| `topicName` | Query | publish | yes | Topic to publish the message into |
| `groupId` | Query | publish | no | Grouping identifier to group multiple messages |

### Example: publish a message to a partner (outbound)

```
PUT /api/internal/v3/messages/7f3f6e0a-1b2c-4d5e-8f90-123456789abc?topicName=orders&groupId=batch-7 HTTP/1.1
Authorization: Bearer <token issued by the system's Keycloak>
bp-id: 123
message-type: OrderResponseMessage
partner-topic: orders
partner-external-reference: ref-4711
mes-metadata: eyJvcmRlcklkIjoiNDcxMSJ9
Content-Type: application/xml

<orderResponse>...</orderResponse>
```

```
HTTP/1.1 201 Created
```

### Example: download an inbound partner message

Typically after receiving the `B2BMessageReceivedEvent` on Kafka:

```
GET /api/internal/v3/messages/cc7d5097-4d3f-4fff-af91-fd3680199642 HTTP/1.1
Authorization: Bearer <token issued by the system's Keycloak>
Accept: application/xml
```

```
HTTP/1.1 200 OK
Content-Length: 1234

<invoice>...</invoice>
```

Returns `403` if the message is blocked by the malware scan status
(see [Malware Scanning](malware-scanning.md)).

## Media types and size limit

Uploaded payloads must match one of the configured media types
(`jeap.messageexchange.api.media-types`, default `application/xml`); the partner V3 API accepts only
`application/xml` regardless of this configuration. On download, the `Accept` header must match the stored
content type. Requests with a body larger than `jeap.messageexchange.api.max-request-body-size-in-bytes`
(default 200 MB) are rejected.

## Backwards compatibility quirks

For compatibility with B2B Hub clients, trailing slashes in resource paths are removed automatically.

## Notable status codes

| Status | Meaning |
| --- | --- |
| `201 Created` | Message stored (uploading identical content for an existing `messageId` is an idempotent no-op) |
| `400 Bad Request` | Invalid `mes-metadata` header (not Base64, not valid JSON, or schema violation), or invalid XML on the partner V3 upload |
| `403 Forbidden` | Token not authorized for the `bp-id` header, or inbound download blocked by the malware scan status (see [Malware Scanning](malware-scanning.md)) |
| `404 Not Found` | Unknown message, or the payload has expired (see [Operations](operations.md#housekeeping)) |
| `406 Not Acceptable` | `Content-Type`/`Accept` does not match the configured media types or the stored content type |
| `409 Conflict` | Upload reusing an existing `messageId` with different content |

## Authorization

Requests are authorized with jEAP semantic roles on two resources: `b2b-message-in` (partner to application)
and `b2b-message-out` (application to partner). `<system>` is the configured
`jeap.security.oauth2.resourceserver.system-name`.

Partner API — tokens issued by the B2B gateway's Keycloak, roles are **business-partner roles** (valid only
for the partner's own `bpId`):

| Verb | Resource | Role |
| --- | --- | --- |
| PUT | Incoming message | `<system>_@b2bmessagein_#write` |
| GET | Outgoing message | `<system>_@b2bmessageout_#read` |

Internal API — tokens issued by the system's Keycloak, roles are **user roles** (valid for any `bpId`):

| Verb | Resource | Role |
| --- | --- | --- |
| PUT | Outgoing message | `<system>_@b2bmessageout_#write` |
| GET | Incoming message | `<system>_@b2bmessagein_#read` |

Example role claims:

```json
// internal application (user roles)
"userroles": [
  "wvs_@b2bmessageout_#write",
  "wvs_@b2bmessagein_#read"
]

// business partner 123 (partner API, token issued by the B2B gateway's Keycloak)
"bproles": {
  "123": [
    "wvs_@b2bmessagein_#write",
    "wvs_@b2bmessageout_#read"
  ]
}
```

See [Getting Started](getting-started.md#4-authorization) for systems acting on behalf of multiple business
partners.
