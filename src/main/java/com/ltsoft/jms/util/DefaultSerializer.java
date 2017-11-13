package com.ltsoft.jms.util;

import java.io.*;

public class DefaultSerializer implements Serializer {
    @Override
    public byte[] serialize(Object source) {

        try {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bao);

            out.writeObject(source);
            out.close();
            bao.close();

            return bao.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Serialize object '%s' fail.", source.getClass().getName()), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] source) {
        try {
            ByteArrayInputStream bai = new ByteArrayInputStream(source);
            ObjectInputStream in = new ObjectInputStream(bai);
            in.close();
            bai.close();

            return (T) in.readObject();
        } catch (IOException e) {
            throw new RuntimeException("Deserialize object fail when read byte array.", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Deserialize object fail case by class not found.", e);
        }
    }
}
