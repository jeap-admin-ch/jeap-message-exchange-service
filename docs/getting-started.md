# Getting Started

This page describes how to set up and operate your own MES instance. The MES is delivered as a reusable jEAP
microservice: you create a small instance repository that depends on the MES artifacts and adds only
configuration.

Example repositories (BIT-internal): `jme-message-exchange-service-example` (an MES instance) and
`jme-message-exchange-client-example` (an internal application using it).

## 1. Create the instance repository

Create a new repository for your MES instance based on the example. The instance depends on
`jeap-message-exchange-service-instance` and declares the message contracts for the events it publishes:

```java
@JeapMessageProducerContract(value = B2BMessageReceivedEvent.TypeRef.class, topic = "jme-messageexchange-b2bmessagereceived")
@JeapMessageProducerContract(value = B2BMessageSentEvent.TypeRef.class, topic = "jme-messageexchange-b2bmessagesent")
class MessagingContracts {
}
```

The `B2BMessageSentEvent` is only published if `jeap.messageexchange.messagesent.enabled=true`.

When running on AWS, also add:

```xml
<dependency>
    <groupId>ch.admin.bit.jeap</groupId>
    <artifactId>jeap-spring-boot-postgresql-aws-starter</artifactId>
</dependency>
<dependency>
    <groupId>ch.admin.bit.jeap</groupId>
    <artifactId>jeap-spring-boot-config-aws-starter</artifactId>
</dependency>
<dependency>
    <groupId>ch.admin.bit.jeap</groupId>
    <artifactId>jeap-spring-boot-tls-starter</artifactId>
</dependency>
```

## 2. Configuration

Most properties are standard jEAP configuration (Kafka, database, security). The MES-specific properties:

```yaml
jeap:
  messageexchange:
    api:
      max-request-body-size-in-bytes: 500000000   # optional, default 200MB
      media-types: application/json, application/xml  # optional, default application/xml
    kafka:
      topic:
        message-received: jme-messageexchange-b2bmessagereceived
        message-sent: jme-messageexchange-b2bmessagesent
    objectstorage:
      connection:
        bucket-name-partner: my-mes-partner-bucket    # inbound messages
        bucket-name-internal: my-mes-internal-bucket  # outbound messages
        # access-url / access-key / secret-key: only for S3-compatible storage outside AWS
        s3-timeout: 30s                               # optional S3 connect/socket timeout, default 30s
        upload-retry-memory-buffer-threshold: 1MB     # optional, default 1MB: bodies up to this size are
                                                      # buffered in memory so transient S3 errors are retried;
                                                      # larger bodies are streamed and fail fast (see Operations)
        upload-buffering-enabled: true                # optional, default true; false streams every body directly
                                                      # to S3, where transient S3 errors cannot be retried
    housekeeping:
      expiration-days: 14   # retention period, default 14 days (see Operations)
    malwarescan:
      enabled: false        # see Malware Scanning
    messagesent:
      enabled: true         # publish B2BMessageSentEvent for outbound messages
    legacy-tag-compatibility:
      enabled: true         # transitional, see the 11.0.0 upgrade notes

  security:
    oauth2:
      resourceserver:
        system-name: "<yoursystem>"   # scopes the jEAP roles, e.g. wvs_@b2bmessagein_#read
        authorization-server:         # issues tokens for the internal API
          issuer: "https://<your-keycloak>/realms/<your-realm>"
          authentication-contexts: sys
        auth-servers:                 # issues tokens for the partner API (B2B gateway Keycloak)
          - issuer: "https://keymanager-npr.api.admin.ch/keycloak/realms/APIGW"
            jwk-set-uri: "https://keymanager-npr.api.admin.ch/jwks/v2"
            authentication-contexts: b2b
```

The database is configured with the standard jEAP/Spring datasource properties. A single read replica may be
enabled with `jeap.datasource.replica.enabled=true` — do not enable it with more than one replica, and see
[Message Flows](message-flows.md#transactional-concerns) for which reads use it.

### Media types

By default the MES accepts `application/xml`. Additional **text-based** media types (JSON, YAML, CSV — no
binary formats such as PDF) are enabled via `jeap.messageexchange.api.media-types`. Non-default media types
require the partner API v4 and the internal API v3; `Content-Type` (upload) and `Accept` (download) headers
are mandatory there and a mismatch is answered with `406 Not Acceptable`.

## 3. Infrastructure

| Infrastructure | Used for | Requirements |
| --- | --- | --- |
| PostgreSQL database | Indexing outbound messages, inbound message metadata and scan status | One instance; start with one or two CPUs and scale based on monitoring |
| Object storage | Message payloads | Two S3 buckets (inbound/partner and outbound/internal), sized for the retention period. **Versioning must be disabled** (message versioning is not supported by the API, see [Malware Scanning](malware-scanning.md#scan-results-and-s3-object-versions)); no object locking |
| Kafka | Notifying internal applications | A topic for `B2BMessageReceivedEvent` (and optionally `B2BMessageSentEvent`); the MES Kafka user needs write permission |

On AWS, set `ignore_lifecycle_rule_updates=true` in the bucket configuration — the MES manages its own
S3 lifecycle rules and Terraform/OpenTofu would otherwise remove them.

## 4. Authorization

Two resources are involved: `b2b-message-in` (partner to application) and `b2b-message-out` (application to
partner). Roles are scoped to your system name.

Partner API (tokens issued by the B2B gateway's Keycloak, business-partner roles limited to the partner's
own `bpId`):

| Verb | Resource | Role |
| --- | --- | --- |
| PUT | Incoming message | `<system>_@b2bmessagein_#write` |
| GET | Outgoing message | `<system>_@b2bmessageout_#read` |

Internal API (tokens issued by your system's Keycloak, user roles valid for any `bpId`):

| Verb | Resource | Role |
| --- | --- | --- |
| PUT | Outgoing message | `<system>_@b2bmessageout_#write` |
| GET | Incoming message | `<system>_@b2bmessagein_#read` |

A system acting **on behalf of multiple business partners** (e.g. a frontend proxying for small partners)
accesses the *partner* API with user roles (`<system>_@b2bmessagein_#write`,
`<system>_@b2bmessageout_#read`) instead of business-partner roles; tenant filtering then happens in that
system.

## 5. Deploy behind a B2B gateway

Partner traffic is exposed through a B2B gateway that manages API subscriptions and tokens. As the MES is
fully API-compatible with the B2B Hub, migrating an existing hub only requires pointing the gateway's
backend URL at the MES — tokens keep working. Internal applications switch the REST client base URL and
request their tokens from the system's own Keycloak realm.

## 6. Client integration

Internal applications consume the Kafka events and use the internal REST API:

```java
@KafkaListener(topics = "jme-messageexchange-b2bmessagereceived")
public void listenToReceivedEvent(B2BMessageReceivedEvent event, Acknowledgment acknowledgment) {
    UUID messageId = UUID.fromString(event.getReferences().getMessageReference().getMessageId());
    messageExchangeClient.onPartnerMessage(messageId, event.getPayload().getScanStatus());
    acknowledgment.acknowledge();
}
```

```java
@HttpExchange(url = "/api/internal/v3/messages")
public interface MessageExchangeInternalService {

    @PutExchange(value = "/{messageId}")
    void sendMessage(@PathVariable UUID messageId,
                     @RequestHeader("bp-id") String bpId,
                     @RequestHeader("message-type") String messageType,
                     @RequestHeader("partner-topic") String partnerTopic,
                     @RequestParam("topicName") String topicName,
                     @RequestParam(value = "groupId", required = false) String groupId,
                     @RequestHeader(value = "partner-external-reference", required = false) String partnerExternalReference,
                     @RequestHeader(value = "mes-metadata", required = false) String metadata,
                     @RequestHeader(HttpHeaders.CONTENT_TYPE) String contentType,
                     @RequestBody byte[] messageBody);

    @GetExchange(value = "/{messageId}")
    byte[] getMessage(@PathVariable("messageId") UUID messageId, @RequestHeader(HttpHeaders.ACCEPT) String accept);
}
```

Consumers must deduplicate events by their idempotence id — see
[Message Flows](message-flows.md#idempotence).

## Next steps

- [REST API](rest-api.md) — API versions and authorization details
- [Malware Scanning](malware-scanning.md) — enabling/disabling scanning safely
- [Operations](operations.md) — housekeeping, monitoring, logging
