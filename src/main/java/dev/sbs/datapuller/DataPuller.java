package dev.sbs.datapuller;

import dev.sbs.datapuller.tasks.HypixelResourceTask;
import dev.sbs.api.SimplifiedApi;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataPuller {
    private static final ScheduledExecutorService scheduler;
    private static final Object exitLock;

    static {
        SimplifiedApi.enableDatabase();
        scheduler = Executors.newScheduledThreadPool(0);
        exitLock = new Object();
    }

    public static void main(String[] args) throws InterruptedException {
        scheduler.scheduleAtFixedRate(HypixelResourceTask::run, 0,
                HypixelResourceTask.getFixedRateMs(), TimeUnit.MILLISECONDS);
        synchronized (exitLock) {
            exitLock.wait();
        }
    }
}
