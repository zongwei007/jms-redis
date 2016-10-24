package com.ltsoft.jms.type;

import java.io.Serializable;

public class IntegerType implements Serializable {

    private static final long serialVersionUID = -2407176971561599952L;

    private int integer;

    public int getInteger() {
        return integer;
    }

    public void setInteger(int integer) {
        this.integer = integer;
    }
}
