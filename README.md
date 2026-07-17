# jeap-message-exchange-service

The Message Exchange Service (MES) exchanges messages with business partners asynchronously in both
directions, using file-transfer-like semantics: partners upload messages that internal applications are
notified about and download, and internal applications publish messages that partners poll and download.
Payloads are stored in S3, metadata in PostgreSQL, notifications go through Kafka, and inbound messages can
be gated by a malware scan. The MES is a reusable jEAP microservice — teams instantiate and operate their
own MES instance by depending on this project and adding configuration.

## Documentation

- [Getting Started](docs/getting-started.md) — set up and configure an MES instance
- [Architecture](docs/architecture.md) — goals, context, building blocks, data model, decisions
- [Message Flows](docs/message-flows.md) — upload/delivery paths with sequence diagrams, failure modes,
  transactional ordering and idempotence
- [REST API](docs/rest-api.md) — partner and internal API, authorization
- [Malware Scanning](docs/malware-scanning.md) — scan status lifecycle, configuration, S3 object versions
- [Operations](docs/operations.md) — housekeeping, monitoring, logging, error handling
- [11.0.0 upgrade notes](docs/scan-status-in-database-11.0.0.md) — scan status moved from S3 tags to
  PostgreSQL

## Versioning

This library is versioned using [Semantic Versioning](http://semver.org/) and all changes are documented in
[CHANGELOG.md](./CHANGELOG.md) following the format defined in [Keep a Changelog](http://keepachangelog.com/).

## Note

This repository is part of the open source distribution of jEAP. See
[github.com/jeap-admin-ch/jeap](https://github.com/jeap-admin-ch/jeap) for more information.

## License

This repository is Open Source Software licensed under the [Apache License 2.0](./LICENSE).
