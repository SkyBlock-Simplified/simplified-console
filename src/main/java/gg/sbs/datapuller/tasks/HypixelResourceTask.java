package gg.sbs.datapuller.tasks;

import gg.sbs.api.SimplifiedApi;
import gg.sbs.api.apiclients.hypixel.implementation.HypixelResourceData;
import gg.sbs.api.apiclients.hypixel.response.ResourceCollectionsResponse;
import gg.sbs.api.apiclients.hypixel.response.ResourceItemsResponse;
import gg.sbs.api.apiclients.hypixel.response.ResourceSkillsResponse;
import gg.sbs.api.data.sql.exception.SqlException;
import gg.sbs.api.data.sql.models.collectionitems.CollectionItemModel;
import gg.sbs.api.data.sql.models.collectionitems.CollectionItemRefreshable;
import gg.sbs.api.data.sql.models.collectionitems.CollectionItemRepository;
import gg.sbs.api.data.sql.models.collectionitemtiers.CollectionItemTierModel;
import gg.sbs.api.data.sql.models.collectionitemtiers.CollectionItemTierRefreshable;
import gg.sbs.api.data.sql.models.collectionitemtiers.CollectionItemTierRepository;
import gg.sbs.api.data.sql.models.collections.CollectionModel;
import gg.sbs.api.data.sql.models.collections.CollectionRefreshable;
import gg.sbs.api.data.sql.models.collections.CollectionRepository;
import gg.sbs.api.data.sql.models.items.ItemRefreshable;
import gg.sbs.api.data.sql.models.items.ItemRepository;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelModel;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRefreshable;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRepository;
import gg.sbs.api.data.sql.models.skills.SkillModel;
import gg.sbs.api.data.sql.models.skills.SkillRefreshable;
import gg.sbs.api.data.sql.models.skills.SkillRepository;
import gg.sbs.api.util.Pair;

import java.util.Map;

public class HypixelResourceTask {
    private static final SkillRefreshable skillRefreshable;
    private static final SkillLevelRefreshable skillLevelRefreshable;
    private static final CollectionRefreshable collectionRefreshable;
    private static final CollectionItemRefreshable collectionItemRefreshable;
    private static final CollectionItemTierRefreshable collectionItemTierRefreshable;
    private static final ItemRefreshable itemRefreshable;
    private static final SkillRepository skillRepository;
    private static final SkillLevelRepository skillLevelRepository;
    private static final CollectionRepository collectionRepository;
    private static final CollectionItemRepository collectionItemRepository;
    private static final CollectionItemTierRepository collectionItemTierRepository;
    private static final ItemRepository itemRepository;
    private static final HypixelResourceData hypixelResourceData;

    static {
        skillRefreshable = SimplifiedApi.getSqlRefreshable(SkillRefreshable.class);
        skillLevelRefreshable = SimplifiedApi.getSqlRefreshable(SkillLevelRefreshable.class);
        collectionRefreshable = SimplifiedApi.getSqlRefreshable(CollectionRefreshable.class);
        collectionItemRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemRefreshable.class);
        collectionItemTierRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemTierRefreshable.class);
        itemRefreshable = SimplifiedApi.getSqlRefreshable(ItemRefreshable.class);
        skillRepository = SimplifiedApi.getSqlRepository(SkillRepository.class);
        skillLevelRepository = SimplifiedApi.getSqlRepository(SkillLevelRepository.class);
        collectionRepository = SimplifiedApi.getSqlRepository(CollectionRepository.class);
        collectionItemRepository = SimplifiedApi.getSqlRepository(CollectionItemRepository.class);
        collectionItemTierRepository = SimplifiedApi.getSqlRepository(CollectionItemTierRepository.class);
        itemRepository = SimplifiedApi.getSqlRepository(ItemRepository.class);
        hypixelResourceData = SimplifiedApi.getWebApi(HypixelResourceData.class);
    }

    public static long getFixedRateMs() {
        return 600_000L; // 10 minutes
    }

    public static void run() {
        ResourceSkillsResponse skills = hypixelResourceData.getSkills();
        ResourceCollectionsResponse collections = hypixelResourceData.getCollections();
        ResourceItemsResponse items = hypixelResourceData.getItems();
        try {
            for (Map.Entry<String, ResourceSkillsResponse.Skill> skillEntry : skills.getSkills().entrySet()) {
                SkillModel skill = updateSkill(skillEntry.getValue(), skillEntry.getKey());
                for (ResourceSkillsResponse.SkillLevel skillLevel : skillEntry.getValue().getLevels()) {
                    updateSkillLevel(skillLevel, skill);
                }
            }
            for (Map.Entry<String, ResourceCollectionsResponse.Collection> collectionEntry : collections.getCollections().entrySet()) {
                CollectionModel collection = updateCollection(collectionEntry.getValue(), collectionEntry.getKey());
                for (Map.Entry<String, ResourceCollectionsResponse.CollectionItem> collectionItemEntry : collectionEntry.getValue().getItems().entrySet()) {
                    CollectionItemModel collectionItem = updateCollectionItem(collectionItemEntry.getValue(), collectionItemEntry.getKey(), collection);
                    for (ResourceCollectionsResponse.CollectionTier collectionTier : collectionItemEntry.getValue().getTiers()) {
                        updateCollectionTier(collectionTier, collectionItem);
                    }
                }
            }
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }

    private static SkillModel updateSkill(ResourceSkillsResponse.Skill skill, String key) throws SqlException {
        SkillModel existingSkill = skillRefreshable.findFirstOrNull(
                SkillModel::getSkillKey, key
        );
        if (existingSkill != null) {
            if (!(existingSkill.getName().equals(skill.getName())
                    && existingSkill.getDescription().equals(skill.getDescription())
                    && existingSkill.getMaxLevel() == skill.getMaxLevel()
            )) {
                existingSkill.setName(skill.getName());
                existingSkill.setDescription(skill.getDescription());
                existingSkill.setMaxLevel(skill.getMaxLevel());
                skillRepository.update(existingSkill);
                skillRefreshable.refreshItems();
            }
            return existingSkill;
        } else {
            SkillModel newSkill = new SkillModel();
            newSkill.setSkillKey(key);
            newSkill.setName(skill.getName());
            newSkill.setDescription(skill.getDescription());
            newSkill.setMaxLevel(skill.getMaxLevel());
            long id = skillRepository.save(newSkill);
            skillRefreshable.refreshItems();
            return skillRefreshable.findFirstOrNull(SkillModel::getId, id);
        }
    }

    private static void updateSkillLevel(ResourceSkillsResponse.SkillLevel skillLevel, SkillModel skill) throws SqlException {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        SkillLevelModel existingSkillLevel = skillLevelRefreshable.findFirstOrNull(
                new Pair<>(SkillLevelModel::getSkill, skill),
                new Pair<>(SkillLevelModel::getSkillLevel, skillLevel.getLevel())
        );
        if (existingSkillLevel != null) {
            if (!(existingSkillLevel.getUnlocks().equals(skillLevel.getUnlocks())
                    && existingSkillLevel.getTotalExpRequired() == skillLevel.getTotalExpRequired()
            )) {
                existingSkillLevel.setUnlocks(skillLevel.getUnlocks());
                existingSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
                skillLevelRepository.update(existingSkillLevel);
                skillLevelRefreshable.refreshItems();
            }
        } else {
            SkillLevelModel newSkillLevel = new SkillLevelModel();
            newSkillLevel.setSkill(skill);
            newSkillLevel.setSkillLevel(skillLevel.getLevel());
            newSkillLevel.setUnlocks(skillLevel.getUnlocks());
            newSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
            skillLevelRepository.save(newSkillLevel);
            skillLevelRefreshable.refreshItems();
        }
    }

    private static CollectionModel updateCollection(ResourceCollectionsResponse.Collection collection, String key) throws  SqlException {
        CollectionModel existingCollection = collectionRefreshable.findFirstOrNull(
                CollectionModel::getCollectionKey, key
        );
        if (existingCollection != null) {
            if (!(existingCollection.getName().equals(collection.getName()))) {
                existingCollection.setName(collection.getName());
                collectionRepository.update(existingCollection);
                collectionRefreshable.refreshItems();
            }
            return existingCollection;
        } else {
            CollectionModel newCollection = new CollectionModel();
            newCollection.setCollectionKey(key);
            newCollection.setName(collection.getName());
            long id = collectionRepository.save(newCollection);
            collectionRefreshable.refreshItems();
            return collectionRefreshable.findFirstOrNull(CollectionModel::getId, id);
        }
    }

    private static CollectionItemModel updateCollectionItem(ResourceCollectionsResponse.CollectionItem collectionItem, String key, CollectionModel collection) throws SqlException {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemModel existingCollectionItem = collectionItemRefreshable.findFirstOrNull(
                new Pair<>(CollectionItemModel::getCollection, collection),
                new Pair<>(CollectionItemModel::getItemKey, key)
        );
        if (existingCollectionItem != null) {
            if (!(existingCollectionItem.getName().equals(collectionItem.getName())
                    && existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers()
            )) {
                existingCollectionItem.setName(collectionItem.getName());
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                collectionItemRepository.update(existingCollectionItem);
                collectionItemRefreshable.refreshItems();
            }
            return existingCollectionItem;
        } else {
            CollectionItemModel newCollectionItem = new CollectionItemModel();
            newCollectionItem.setItemKey(key);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setName(collectionItem.getName());
            newCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
            long id = collectionItemRepository.save(newCollectionItem);
            collectionItemRefreshable.refreshItems();
            return collectionItemRefreshable.findFirstOrNull(CollectionItemModel::getId, id);
        }
    }

    private static void updateCollectionTier(ResourceCollectionsResponse.CollectionTier collectionTier, CollectionItemModel collectionItem) throws SqlException {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemTierModel existingCollectionTier = collectionItemTierRefreshable.findFirstOrNull(
                new Pair<>(CollectionItemTierModel::getCollectionItem, collectionItem),
                new Pair<>(CollectionItemTierModel::getTier, collectionTier.getTier())
        );
        if (existingCollectionTier != null) {
            if (!(existingCollectionTier.getUnlocks().equals(collectionTier.getUnlocks())
                    && existingCollectionTier.getAmountRequired() == collectionTier.getAmountRequired()
            )) {
                existingCollectionTier.setUnlocks(collectionTier.getUnlocks());
                existingCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
                collectionItemTierRepository.update(existingCollectionTier);
                collectionItemTierRefreshable.refreshItems();
            }
        } else {
            CollectionItemTierModel newCollectionTier = new CollectionItemTierModel();
            newCollectionTier.setCollectionItem(collectionItem);
            newCollectionTier.setTier(collectionTier.getTier());
            newCollectionTier.setUnlocks(collectionTier.getUnlocks());
            newCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
            collectionItemTierRepository.save(newCollectionTier);
            collectionItemTierRefreshable.refreshItems();
        }
    }
}
