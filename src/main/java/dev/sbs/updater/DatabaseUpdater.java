package dev.sbs.updater;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.updater.tasks.HypixelResourceTask;

public class DatabaseUpdater {

    private static final Object exitLock = new Object();

    static {
        SimplifiedApi.enableDatabase();
    }

    public static void main(String[] args) throws InterruptedException {
        SimplifiedApi.getScheduler().scheduleAsync(HypixelResourceTask::run, 0, HypixelResourceTask.getFixedRateMs());

        synchronized (exitLock) {
            exitLock.wait();
        }
    }

}
