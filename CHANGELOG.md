# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.20.0] - 2025-06-13

### Changed

- Update parent from 26.55.0 to 26.57.0

## [2.19.0] - 2025-06-06

### Changed

- Update parent from 26.46.0 to 26.55.0

## [2.18.1] - 2025-04-29

### Changed

- exclude Pacticipants.java from sonar coverage

## [2.18.0] - 2025-04-28

### Changed

- added base pact provider test for the message exchange service
- Update parent from 26.43.2 to 26.46.0

## [2.17.0] - 2025-04-15

### Changed

- Update parent from 26.42.0 to 26.43.2

## [2.16.1] - 2025-04-10

### Changed

- Fix OpenApi response example in MessagePartnerV3Controller

## [2.16.0] - 2025-04-01

### Changed

- Update parent from 26.35.0 to 26.42.0

## [2.15.0] - 2025-03-07

### Changed

- Configure proxy to work around the issue https://github.com/aws/aws-sdk-java-v2/issues/4728 which is coming with the aws sdk update
- Update parent from 26.33.0 to 26.35.0


## [2.14.0] - 2025-03-06

### Changed

- Update parent from 26.24.2 to 26.33.0

## [2.13.0] - 2025-02-13

### Changed

- Update parent from 26.23.0 to 26.24.2
- Disable license plugins for MES instances

## [2.12.1] - 2025-02-11

### Changed

- Publish to maven central

## [2.12.0] - 2025-02-10

### Changed

- Update parent from 26.22.4 to 26.23.0

## [2.11.0] - 2025-01-31

### Added

- Prepare repository for Open Source distribution
- Added partner API v3 with the following changes compared to v2 (note that v2 is unchanged)
- The messages list response now includes the following optional elements:
  - `<groupId>`
  - `<partnerTopic>`
- `partnerTopic` is now defined as a query parameter
  - The HTTP headers `partnerTopic` and `partner-topic` have been removed
- Removed Deprecated HTTP Headers
  - `partnerTopic`
  - `partner-topic`
  - `bpId` (use `bp-id` instead)
  - `messageType` (use `message-type` instead)
  - `Messageid` (use `message-id` instead)
- The deprecated query parameter `lastMessageID` (which was a fallback for the correct name `lastMessageId`) has been
  removed

## [2.10.0] - 2025-01-29

### Changed

- Added the possibility to enable the S3 Object malware scan.
- Updated jeap parent from 26.22.2 to 26.22.3

## [2.9.0] - 2025-01-28

### Changed

- Add new S3 object storage fallback repository in order to retrieve messages from an older S3 bucket (read-only)

## [2.8.0] - 2025-01-08

### Changed

- Added the module jeap-message-exchange-service-instance which will instantiate a MES when used as parent project.
- Updated jeap parent from 26.21.1 to 26.22.2

## [2.7.0] - 2024-12-19

### Changed

- Update parent from 26.20.0 to 26.21.1

## [2.6.0] - 2024-12-16

### Changed

- The MES understands both the old headers and the new standard headers on requests (If a request contains both the old and the new headers and the values are different, the message is rejected)
  - bpId and bp-id
  - partnerTopic and partner-topic
  - messageType and message-type
  
- The MES generates both the old header and the new standard header on responses
  - Messageid and message-id

- Updated jeap-spring-boot-parent from 26.17.0 to 26.20.0

## [2.5.2] - 2024-12-03

### Changed

- Changes to accept nonstandard requests for backward compatibility with the B2B hub
  - Accept trailing slashes in resource paths
  - Accept lastMessageID with uppercase ID

## [2.5.1] - 2024-12-03

### Changed

- Fix housekeeping issue with expirationDays parameter which was mistakenly converted into hours

## [2.5.0] - 2024-11-21

### Changed

- Updated jeap-spring-boot-parent from 26.5.0 to 26.17.0

## [2.4.0] - 2024-11-19

### Changed

- Rename response header messageId to Messageid

## [2.3.0] - 2024-11-15

### Changed

- Add header messageId to Partner-API NextMessage response

## [2.2.1] - 2024-11-05

### Changed

- Fixed scheduled housekeeping job not starting because of missing shedlock lock provider bean.

## [2.2.0] - 2024-10-31

### Changed

- Update parent from 26.4.0 to 26.5.0

## [2.1.0] - 2024-10-17

### Changed

- Update parent from 26.3.0 to 26.4.0

## [2.0.0] - 2024-10-15

### Changed

- Removed support for configuring a privileged business partner that is allowed to access partner messages for all partners.

## [1.4.1] - 2024-10-11

### Changed

- Set Content-Type in S3 to application/xml

## [1.4.0] - 2024-09-20

### Changed

- Update parent from 26.2.1 to 26.3.0

## [1.3.0] - 2024-09-12

### Changed

- Update parent from 25.4.0 to 26.2.0
- Add @TransactionalReadReplica annotations

## [1.2.0] - 2024-09-06

### Changed

- Update parent from 25.4.0 to 26.0.0

## [1.1.1] - 2024-08-28

### Changed

- Fixed: Lifecycle policies are now correctly applied to all buckets

## [1.1.0] - 2024-08-22

### Changed

- Update parent from 25.2.0 to 25.4.0

## [1.0.0] - 2024-07-30

### Changed

- Initial release
