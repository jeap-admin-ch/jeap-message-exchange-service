package ch.admin.bit.jeap.messageexchange.web.api;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        // Note: This is deprecated and should be replaced with a UrlHandlerFilter after the spring boot 3.4 upgrade
        // https://docs.spring.io/spring-framework/reference/6.2-SNAPSHOT/web/webmvc/filters.html#filters.url-handler
        configurer.setUseTrailingSlashMatch(true);
    }

}
