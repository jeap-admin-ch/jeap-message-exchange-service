package ch.admin.bit.jeap.messageexchange.web.api;

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@OpenAPIDefinition(
        info = @Info(
                title = "Message Exchange Service",
                description = "Rest API to exchange messages with business partners"
        ),
        security = {@SecurityRequirement(name = "OIDC")},
        externalDocs = @ExternalDocumentation(url = "https://confluence.bit.admin.ch/display/JEAP/Message+Exchange+Service")
)
@Configuration
public class OpenApiConfig {
    @Bean
    GroupedOpenApi internalApiV2() {
        return GroupedOpenApi.builder()
                .group("MessageExchange-Service-Internal-API-V2")
                .pathsToMatch("/api/internal/v2/**")
                .packagesToScan(this.getClass().getPackageName())
                .build();
    }

    @Bean
    GroupedOpenApi partnerApiV2() {
        return GroupedOpenApi.builder()
                .group("MessageExchange-Service-Partner-API-V2")
                .pathsToMatch("/api/partner/v2/**")
                .packagesToScan(this.getClass().getPackageName())
                .build();
    }

    @Bean
    GroupedOpenApi partnerApiV3() {
        return GroupedOpenApi.builder()
                .group("MessageExchange-Service-Partner-API-V3")
                .pathsToMatch("/api/partner/v3/**")
                .packagesToScan(this.getClass().getPackageName())
                .build();
    }
}
