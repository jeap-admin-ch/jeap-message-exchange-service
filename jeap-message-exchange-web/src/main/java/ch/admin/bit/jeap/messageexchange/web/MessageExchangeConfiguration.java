package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.web.api.MessageExchangeApiProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

@AutoConfiguration
@ComponentScan
@PropertySource("classpath:messageExchangeDefaultProperties.properties")
@EnableConfigurationProperties({MessageExchangeApiProperties.class})
public class MessageExchangeConfiguration {

}
