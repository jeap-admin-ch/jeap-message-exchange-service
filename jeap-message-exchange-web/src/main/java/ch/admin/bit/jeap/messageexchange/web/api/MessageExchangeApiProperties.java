package ch.admin.bit.jeap.messageexchange.web.api;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.MediaType;

import java.util.List;

@Getter
@Setter
@ToString
@ConfigurationProperties("jeap.messageexchange.api")
public class MessageExchangeApiProperties {

    /**
     * Maximal request body size [bytes]. Default is 200'000'0000
     */
    private int maxRequestBodySizeInBytes = 200_000_000;

    /**
     * Allowed media types for message upload. Default is application/xml
     */
    private List<MediaType> mediaTypes = List.of(MediaType.APPLICATION_XML);

}
