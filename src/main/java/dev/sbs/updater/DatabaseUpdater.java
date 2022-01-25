package dev.sbs.updater;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.implementation.HypixelResourceData;
import dev.sbs.api.util.ConsoleLogger;
import dev.sbs.updater.processor.resource.ResourceCollectionsProcessor;
import dev.sbs.updater.processor.resource.ResourceItemsProcessor;
import dev.sbs.updater.processor.resource.ResourceSkillsProcessor;
import lombok.Getter;

public class DatabaseUpdater {

    private static final HypixelResourceData hypixelResourceData = SimplifiedApi.getWebApi(HypixelResourceData.class);
    @Getter private final ConsoleLogger log;

    public DatabaseUpdater() {
        this.log = new ConsoleLogger(this);
        this.connectDatabase();

        this.getLog().info("Loading Processors");
        ResourceSkillsProcessor skillsProcessor = new ResourceSkillsProcessor(hypixelResourceData.getSkills());
        ResourceItemsProcessor itemsProcessor = new ResourceItemsProcessor(hypixelResourceData.getItems());
        ResourceCollectionsProcessor collectionsProcessor = new ResourceCollectionsProcessor(hypixelResourceData.getCollections());

        try {
            this.getLog().info("Processing Skills");
            skillsProcessor.process();
            this.getLog().info("Processing Items");
            itemsProcessor.process();
            this.getLog().info("Processing Collections");
            collectionsProcessor.process();
        } catch (Exception exception) {
            exception.printStackTrace(); // TODO: Handle exception logging
        }
    }

    public static void main(String[] args) {
        new DatabaseUpdater();
    }

    private void connectDatabase() {
        this.getLog().info("Connecting to Database");
        SimplifiedApi.enableDatabase();
        this.getLog().info("Database Initialized in {0}ms", SimplifiedApi.getSqlSession().getInitializationTime());
        this.getLog().info("Database Cached in {0}ms", SimplifiedApi.getSqlSession().getStartupTime());
    }

}
