package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.security.test.resource.configuration.ServletJeapAuthorizationConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("controller-test") // prevent other tests using class path scanning picking up this configuration
@Configuration
@ComponentScan
@EnableConfigurationProperties({MessageExchangeApiProperties.class})
public class RestApiTestContext extends ServletJeapAuthorizationConfig {

    // You have to provide the system name and the application context to the test support base class.
    RestApiTestContext(ApplicationContext applicationContext) {
        super("junit", applicationContext);
    }
}