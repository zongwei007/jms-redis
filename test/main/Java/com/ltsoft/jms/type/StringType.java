package com.ltsoft.jms.type;

import java.io.Serializable;

public class StringType implements Serializable {

    private static final long serialVersionUID = -8404574329044348391L;

    private String string;

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }
}
