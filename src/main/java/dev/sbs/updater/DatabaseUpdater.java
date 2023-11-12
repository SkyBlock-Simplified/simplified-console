package dev.sbs.updater;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.request.HypixelResourceRequest;
import dev.sbs.updater.processor.resource.ResourceCollectionsProcessor;
import dev.sbs.updater.processor.resource.ResourceItemsProcessor;
import dev.sbs.updater.processor.resource.ResourceSkillsProcessor;
import dev.sbs.updater.util.UpdaterConfig;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.io.File;

@Getter
@Log4j2
public class DatabaseUpdater {

    private static final HypixelResourceRequest HYPIXEL_RESOURCE_REQUEST = SimplifiedApi.getWebApi(HypixelResourceRequest.class);

    public DatabaseUpdater() {
        UpdaterConfig config;
        try {
            File currentDir = new File(SimplifiedApi.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            config = new UpdaterConfig(currentDir.getParentFile(), "updater");
        } catch (Exception exception) {
            throw new IllegalArgumentException("Unable to retrieve current directory", exception); // Should never get here
        }

        log.info("Connecting to Database");
        SimplifiedApi.getSessionManager().connectSql(config);
        log.info("Database Initialized in {}ms", SimplifiedApi.getSessionManager().getSession().getInitializationTime());
        log.info("Database Cached in {}ms", SimplifiedApi.getSessionManager().getSession().getStartupTime());

        log.info("Loading Processors");
        ResourceItemsProcessor itemsProcessor = new ResourceItemsProcessor(HYPIXEL_RESOURCE_REQUEST.getItems());
        ResourceSkillsProcessor skillsProcessor = new ResourceSkillsProcessor(HYPIXEL_RESOURCE_REQUEST.getSkills());
        ResourceCollectionsProcessor collectionsProcessor = new ResourceCollectionsProcessor(HYPIXEL_RESOURCE_REQUEST.getCollections());

        try {
            log.info("Processing Items");
            itemsProcessor.process();
            log.info("Processing Skills");
            skillsProcessor.process();
            log.info("Processing Collections");
            collectionsProcessor.process();
        } catch (Exception exception) {
            log.atError()
                .withThrowable(exception)
                .log("An error occurred while processing the resource API.");
        }

        System.exit(0);
    }

    public static void main(String[] args) {
        new DatabaseUpdater();
    }

}
