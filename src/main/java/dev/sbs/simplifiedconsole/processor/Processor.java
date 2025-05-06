package dev.sbs.simplifiedconsole.processor;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.jetbrains.annotations.NotNull;

@Getter
@Log4j2
public abstract class Processor<R> {

    private final @NotNull R resourceResponse;

    public Processor(@NotNull R resourceResponse) {
        this.resourceResponse = resourceResponse;
        Configurator.setLevel(log, Level.INFO);
    }

    public final @NotNull Logger getLog() {
        return log;
    }

    public abstract void process();

    protected static boolean equalsWithNull(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

}
