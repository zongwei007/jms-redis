package com.ltsoft.jms.util;

/**
 * Created by zongw on 2016/10/9.
 */
class ConversionKey {

    private final Class<?> from;
    private final Class<?> to;
    private final int hashCode;

    ConversionKey(Class<?> from, Class<?> to) {
        this.from = from;
        this.to = to;
        this.hashCode = from.hashCode() ^ (to.hashCode() << 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }

        ConversionKey x = (ConversionKey) o;
        return x.from == from && x.to == to;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
