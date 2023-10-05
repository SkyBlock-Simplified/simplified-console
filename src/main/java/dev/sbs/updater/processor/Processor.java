package dev.sbs.updater.processor;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Getter
public abstract class Processor<R> {

    private final R resourceResponse;
    private final Logger log;

    public Processor(R resourceResponse) {
        this.resourceResponse = resourceResponse;
        this.log = LogManager.getLogger(this);
    }

    public abstract void process();

    protected static boolean equalsWithNull(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

}
