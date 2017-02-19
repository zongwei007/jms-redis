package com.ltsoft.jms.util;

import javax.jms.MessageFormatRuntimeException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 消息属性
 */
public class MessageProperty {

    protected final HashMap<String, Object> properties = new HashMap<>();

    public void clearProperties() {
        properties.clear();
    }

    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    protected <T> Optional<T> getProperty(String name, Class<T> type) {
        try {
            return Optional.ofNullable(properties.get(name))
                    .map(val -> TypeConversionSupport.convert(val, type));
        } catch (Exception e) {
            throw new MessageFormatRuntimeException(String.format("Get property %s fail：%s", name, e.getMessage()));
        }
    }

    public boolean getBooleanProperty(String name) {
        return getProperty(name, Boolean.class).orElse(false);
    }

    private Supplier<NumberFormatException> numberFormatException(String name) {
        return () -> new NumberFormatException("property " + name + " is null");
    }

    public byte getByteProperty(String name) {
        return getProperty(name, Byte.class).orElseThrow(numberFormatException(name));
    }

    public short getShortProperty(String name) {
        return getProperty(name, Short.class).orElseThrow(numberFormatException(name));
    }

    public char getCharacter(String name) {
        return getProperty(name, Character.class).orElseThrow(numberFormatException(name));
    }

    public int getIntProperty(String name) {
        return getProperty(name, Integer.class).orElseThrow(numberFormatException(name));
    }

    public long getLongProperty(String name) {
        return getProperty(name, Long.class).orElseThrow(numberFormatException(name));
    }

    public float getFloatProperty(String name) {
        return getProperty(name, Float.class).orElseThrow(numberFormatException(name));
    }

    public double getDoubleProperty(String name) {
        return getProperty(name, Double.class).orElseThrow(numberFormatException(name));
    }

    public String getStringProperty(String name) {
        return getProperty(name, String.class).orElse(null);
    }

    public Object getObjectProperty(String name) {
        return getProperty(name, Object.class).orElse(null);
    }

    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    private static void checkValidObject(Object value) {
        boolean valid = value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof Character ||
                value instanceof String ||
                value == null;

        if (!valid) {
            throw new MessageFormatRuntimeException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }

    public void setProperty(String name, Object value) {
        checkValidObject(value);
        properties.put(name, value);
    }

    public void mergeProperty(MessageProperty props) {
        properties.putAll(props.properties);
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
