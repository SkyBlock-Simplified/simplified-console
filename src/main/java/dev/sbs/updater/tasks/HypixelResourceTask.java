package dev.sbs.updater.tasks;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.implementation.HypixelResourceData;
import dev.sbs.api.client.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.client.hypixel.response.resource.ResourceItemsResponse;
import dev.sbs.api.client.hypixel.response.resource.ResourceSkillsResponse;
import dev.sbs.updater.processor.resource.ResourceCollectionsProcessor;
import dev.sbs.updater.processor.resource.ResourceItemsProcessor;
import dev.sbs.updater.processor.resource.ResourceSkillsProcessor;

public class HypixelResourceTask {

    private static final HypixelResourceData hypixelResourceData = SimplifiedApi.getWebApi(HypixelResourceData.class);

    public static long getFixedRateMs() {
        return 600_000L; // 10 minutes
    }

    public static void run() {
        // Load Responses
        ResourceSkillsProcessor skillsProcessor = new ResourceSkillsProcessor(hypixelResourceData.getSkills());
        ResourceItemsProcessor itemsProcessor = new ResourceItemsProcessor(hypixelResourceData.getItems());
        ResourceCollectionsProcessor collectionsProcessor = new ResourceCollectionsProcessor(hypixelResourceData.getCollections());

        // remove this when i'm done
        ResourceSkillsResponse skills = skillsProcessor.getResourceResponse();
        ResourceItemsResponse items = itemsProcessor.getResourceResponse();
        ResourceCollectionsResponse collections = collectionsProcessor.getResourceResponse();

        try {
            skillsProcessor.process();
            itemsProcessor.process();
            collectionsProcessor.process();
        } catch (Exception exception) {
            exception.printStackTrace(); // TODO: Handle exception logging
        }
    }

}
