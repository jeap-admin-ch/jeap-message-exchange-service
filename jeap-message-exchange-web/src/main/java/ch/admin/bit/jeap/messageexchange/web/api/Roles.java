package ch.admin.bit.jeap.messageexchange.web.api;

public final class Roles {
    private Roles() {
        /* This utility class should not be instantiated */
    }

    static final String MESSAGE_OUT = "b2bmessageout";
    static final String READ = "read";
    static final String MESSAGE_IN = "b2bmessagein";
    static final String WRITE = "write";

    private static final String HAS_ROLE_PREFIX = "hasRole('";
    private static final String ROLE_SEPARATOR = "','";
    private static final String HAS_ROLE_SUFFIX = "')";

    static final String HAS_ROLE_WRITE_MESSAGE_IN = HAS_ROLE_PREFIX + Roles.MESSAGE_IN + ROLE_SEPARATOR + Roles.WRITE + HAS_ROLE_SUFFIX;
    static final String HAS_ROLE_READ_MESSAGE_OUT = HAS_ROLE_PREFIX + Roles.MESSAGE_OUT + ROLE_SEPARATOR + Roles.READ + HAS_ROLE_SUFFIX;

    static final String HAS_ROLE_WRITE_MESSAGE_OUT = HAS_ROLE_PREFIX + Roles.MESSAGE_OUT + ROLE_SEPARATOR + Roles.WRITE + HAS_ROLE_SUFFIX;
    static final String HAS_ROLE_READ_MESSAGE_IN = HAS_ROLE_PREFIX + Roles.MESSAGE_IN + ROLE_SEPARATOR + Roles.READ + HAS_ROLE_SUFFIX;
}
