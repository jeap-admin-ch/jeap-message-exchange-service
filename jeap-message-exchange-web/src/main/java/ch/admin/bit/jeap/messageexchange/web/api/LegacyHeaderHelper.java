package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.web.api.exception.MissingRequiredHeaderException;
import lombok.experimental.UtilityClass;

import static org.springframework.util.StringUtils.hasText;

@UtilityClass
//TODO: JEAP-5099 delete class
public class LegacyHeaderHelper {

    public static String checkVariables(String value, String valueOld, String label, String labelOld, boolean required) throws MissingRequiredHeaderException {

        if (hasText(value) && hasText(valueOld) && !value.equals(valueOld)) {
            throw MissingRequiredHeaderException.differentHeaders(label, labelOld);
        }

        if (hasText(value)) {
            return value;
        }

        if (hasText(valueOld)) {
            return valueOld;
        }

        if (required) {
            throw MissingRequiredHeaderException.missingHeader(label);
        } else {
            return null;
        }

    }

}
