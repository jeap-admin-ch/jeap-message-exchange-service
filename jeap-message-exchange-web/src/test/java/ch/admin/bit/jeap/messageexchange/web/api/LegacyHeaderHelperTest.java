package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.web.api.exception.MissingRequiredHeaderException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

//TODO: JEAP-5099 delete class
class LegacyHeaderHelperTest {


    @SneakyThrows
    @Test
    void checkVariables_onlyNewValue_returnNewValue(){
        assertThat(LegacyHeaderHelper.checkVariables("new", null, "label", "oldLabel", true)).isEqualTo("new");
    }

    @SneakyThrows
    @Test
    void checkVariables_onlyOldValue_returnOldValue(){
        assertThat(LegacyHeaderHelper.checkVariables(null, "old", "label", "oldLabel", true)).isEqualTo("old");
    }

    @SneakyThrows
    @Test
    void checkVariables_noValues_notRequired_returnNull(){
        assertThat(LegacyHeaderHelper.checkVariables(null, null, "label", "oldLabel", false)).isNull();
    }

    @SneakyThrows
    @Test
    void checkVariables_emptyValues_notRequired_returnNull(){
        assertThat(LegacyHeaderHelper.checkVariables("", "", "label", "oldLabel", false)).isNull();
    }

    @SneakyThrows
    @Test
    void checkVariables_sameValues_returnNewValue(){
        assertThat(LegacyHeaderHelper.checkVariables("same", "same", "label", "oldLabel", true)).isEqualTo("same");
    }

    @SneakyThrows
    @Test
    void checkVariables_emptyValues_required_throwsException(){
        MissingRequiredHeaderException missingRequiredHeaderException = assertThrows(
                MissingRequiredHeaderException.class,
                () -> LegacyHeaderHelper.checkVariables("", "", "label", "oldLabel", true));

        assertThat(missingRequiredHeaderException.getMessage()).isEqualTo("Required header 'label' missing in request.");
    }

    @SneakyThrows
    @Test
    void checkVariables_noValues_required_throwsException(){
        MissingRequiredHeaderException missingRequiredHeaderException = assertThrows(
                MissingRequiredHeaderException.class,
                () -> LegacyHeaderHelper.checkVariables(null, null, "label", "oldLabel", true));

        assertThat(missingRequiredHeaderException.getMessage()).isEqualTo("Required header 'label' missing in request.");
    }

    @SneakyThrows
    @Test
    void checkVariables_differentValues_throwsException(){
        MissingRequiredHeaderException missingRequiredHeaderException = assertThrows(
                MissingRequiredHeaderException.class,
                () -> LegacyHeaderHelper.checkVariables("foo", "bar", "label", "oldLabel", true));

        assertThat(missingRequiredHeaderException.getMessage()).isEqualTo("The supplied values for the 2 headers 'label' and 'oldLabel' are not equal");
    }


}