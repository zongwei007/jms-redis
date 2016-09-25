package com.ltsoft.jms.message;

import javax.jms.JMSException;
import java.util.Optional;

public final class JmsMessageHeader {

    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_REPLYTO = "JMSReplyTo";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_MESSAGEID = "JMSMessageID";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_CORRELATIONID = "JMSCorrelationID";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_REDELIVERED = "JMSRedelivered";

    public static final String JMSX_GROUPID = "JMSXGroupID";
    public static final String JMSX_GROUPSEQ = "JMSXGroupSeq";
    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    public static final String JMSX_USERID = "JMSXUserID";

    public static <T> Optional<T> getHeader(JmsMessage message, String name, Class<T> type) throws JMSException {
        if (message.propertyExists(name)) {
            return Optional.of(message.getProperty(name, type));
        }
        return Optional.empty();
    }

    public static void setHeader(JmsMessage message, String name, Object value) throws JMSException {
        message.setObjectProperty(name, value);
    }

}
