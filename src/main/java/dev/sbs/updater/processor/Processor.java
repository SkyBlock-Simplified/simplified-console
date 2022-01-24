package dev.sbs.updater.processor;

import dev.sbs.api.util.ConsoleLogger;
import lombok.Getter;

public abstract class Processor<R> {

    @Getter private final R resourceResponse;
    @Getter private final ConsoleLogger log;

    public Processor(R resourceResponse) {
        this.resourceResponse = resourceResponse;
        this.log = new ConsoleLogger(this);
    }

    public abstract void process();

    protected static boolean equalsWithNull(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

}
