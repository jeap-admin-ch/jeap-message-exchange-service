package ch.admin.bit.jeap.messageexchange.domain.legacy;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Controls the transitional compatibility with MES &lt; 11.0.0 instances during zero-downtime deployments.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@Data
@ConfigurationProperties(prefix = "jeap.messageexchange.legacy-tag-compatibility")
public class LegacyTagCompatibilityProperties {

    /**
     * When enabled, the metadata tags expected by MES &lt; 11.0.0 are still written to new S3 objects - atomically
     * at object creation, never updated afterwards. This keeps rolling deployments (and rollbacks) with mixed
     * 10.x/11.x instances safe: old instances gate message delivery and process malware scan results based on
     * these tags. Keep enabled until all instances run 11.x, then the property can be set to false. The tag
     * writing will be removed entirely with JEAP-7252.
     */
    private boolean enabled = true;
}
