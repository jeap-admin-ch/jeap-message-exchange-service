package ch.admin.bit.jeap.messageexchange.web.api;

public final class Roles {
    static final String MESSAGE_OUT = "b2bmessageout";
    static final String READ = "read";
    static final String MESSAGE_IN = "b2bmessagein";
    static final String WRITE = "write";

    static final String HAS_ROLE_WRITE_MESSAGE_IN = "hasRole('" + Roles.MESSAGE_IN + "','" + Roles.WRITE + "')";
    static final String HAS_ROLE_READ_MESSAGE_OUT = "hasRole('" + Roles.MESSAGE_OUT + "','" + Roles.READ + "')";

    static final String HAS_ROLE_WRITE_MESSAGE_OUT = "hasRole('" + Roles.MESSAGE_OUT + "','" + Roles.WRITE + "')";
    static final String HAS_ROLE_READ_MESSAGE_IN = "hasRole('" + Roles.MESSAGE_IN + "','" + Roles.READ + "')";
}
