package dev.sbs.updater.processor;

import lombok.Getter;

public abstract class Processor<R> {

    @Getter private final R resourceResponse;

    public Processor(R resourceResponse) {
        this.resourceResponse = resourceResponse;
    }

    public abstract void process();

    protected static boolean equalsWithNull(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

}
