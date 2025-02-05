package ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@RequiredArgsConstructor
@Slf4j
public class S3LifecycleConfigurationFactory {

    private final String policyName;
    private final int expirationDays;

    BucketLifecycleConfiguration createOrUpdateBucketLifecycleConfiguration(GetBucketLifecycleConfigurationResponse response) {
        BucketLifecycleConfiguration configuration = createOrReuseConfiguration(response);
        return addMissingRulesForLifecyclePolicies(configuration);
    }

    private BucketLifecycleConfiguration createOrReuseConfiguration(GetBucketLifecycleConfigurationResponse response) {
        BucketLifecycleConfiguration.Builder builder = BucketLifecycleConfiguration.builder();
        if (response != null && response.hasRules()) {
            builder.rules(response.rules());
        }
        return builder.build();
    }

    private BucketLifecycleConfiguration addMissingRulesForLifecyclePolicies(BucketLifecycleConfiguration configuration) {
        List<LifecycleRule> rules = new ArrayList<>(Objects.requireNonNullElseGet(configuration.rules(), List::of));
        Set<String> existingRuleIds = getRuleIds(rules);

        if (existingRuleIds.contains(ruleId())) {
            return configuration;
        }

        rules.add(createRule(ruleId()));
        return BucketLifecycleConfiguration.builder().rules(rules).build();
    }

    private LifecycleRule createRule(String ruleId) {
        return LifecycleRule.builder()
                .status(ExpirationStatus.ENABLED)
                .id(ruleId)
                .expiration(le -> le.days(expirationDays))
                .noncurrentVersionExpiration(nve -> nve.noncurrentDays(1))
                .filter(lrf -> lrf.tag(lifecyclePolicyTag()))
                .build();
    }

    public Tag lifecyclePolicyTag() {
        return Tag.builder()
                .key(policyName)
                .value(String.valueOf(expirationDays))
                .build();
    }

    private String ruleId() {
        return policyName + "-" + expirationDays;
    }

    private static Set<String> getRuleIds(List<LifecycleRule> rules) {
        return rules.stream()
                .map(LifecycleRule::id)
                .filter(Objects::nonNull)
                .collect(toSet());
    }

    boolean ruleExists(GetBucketLifecycleConfigurationResponse response) {
        String ruleId = ruleId();
        return response != null &&
               response.rules() != null &&
               response.rules().stream().anyMatch(rule -> rule.id().equals(ruleId));
    }
}
