package dev.sbs.updater;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.updater.tasks.HypixelResourceTask;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatabaseUpdater {

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(0);
    private static final Object exitLock = new Object();

    static {
        SimplifiedApi.enableDatabase();
    }

    public static void main(String[] args) throws InterruptedException {
        scheduler.scheduleAtFixedRate(HypixelResourceTask::run, 0, HypixelResourceTask.getFixedRateMs(), TimeUnit.MILLISECONDS);

        synchronized (exitLock) {
            exitLock.wait();
        }
    }

}
