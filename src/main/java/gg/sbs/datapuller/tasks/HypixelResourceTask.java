package gg.sbs.datapuller.tasks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import gg.sbs.api.data.sql.models.items.ItemModel;
import gg.sbs.api.data.sql.models.items.ItemRefreshable;
import gg.sbs.api.data.sql.models.items.ItemRepository;
import gg.sbs.api.data.sql.models.rarities.RarityModel;
import gg.sbs.api.data.sql.models.rarities.RarityRefreshable;
import gg.sbs.api.data.sql.models.rarities.RarityRepository;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelModel;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRefreshable;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRepository;
import gg.sbs.api.data.sql.models.skills.SkillModel;
import gg.sbs.api.data.sql.models.skills.SkillRefreshable;
import gg.sbs.api.data.sql.models.skills.SkillRepository;
import gg.sbs.api.util.Pair;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class HypixelResourceTask {
    private static final SkillRefreshable skillRefreshable;
    private static final SkillLevelRefreshable skillLevelRefreshable;
    private static final CollectionRefreshable collectionRefreshable;
    private static final CollectionItemRefreshable collectionItemRefreshable;
    private static final CollectionItemTierRefreshable collectionItemTierRefreshable;
    private static final ItemRefreshable itemRefreshable;
    private static final RarityRefreshable rarityRefreshable;
    private static final SkillRepository skillRepository;
    private static final SkillLevelRepository skillLevelRepository;
    private static final CollectionRepository collectionRepository;
    private static final CollectionItemRepository collectionItemRepository;
    private static final CollectionItemTierRepository collectionItemTierRepository;
    private static final ItemRepository itemRepository;
    private static final RarityRepository rarityRepository;
    private static final HypixelResourceData hypixelResourceData;
    private static final Gson gson;

    static {
        skillRefreshable = SimplifiedApi.getSqlRefreshable(SkillRefreshable.class);
        skillLevelRefreshable = SimplifiedApi.getSqlRefreshable(SkillLevelRefreshable.class);
        collectionRefreshable = SimplifiedApi.getSqlRefreshable(CollectionRefreshable.class);
        collectionItemRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemRefreshable.class);
        collectionItemTierRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemTierRefreshable.class);
        itemRefreshable = SimplifiedApi.getSqlRefreshable(ItemRefreshable.class);
        rarityRefreshable = SimplifiedApi.getSqlRefreshable(RarityRefreshable.class);
        skillRepository = SimplifiedApi.getSqlRepository(SkillRepository.class);
        skillLevelRepository = SimplifiedApi.getSqlRepository(SkillLevelRepository.class);
        collectionRepository = SimplifiedApi.getSqlRepository(CollectionRepository.class);
        collectionItemRepository = SimplifiedApi.getSqlRepository(CollectionItemRepository.class);
        collectionItemTierRepository = SimplifiedApi.getSqlRepository(CollectionItemTierRepository.class);
        itemRepository = SimplifiedApi.getSqlRepository(ItemRepository.class);
        rarityRepository = SimplifiedApi.getSqlRepository(RarityRepository.class);
        hypixelResourceData = SimplifiedApi.getWebApi(HypixelResourceData.class);
        gson = new GsonBuilder().create();
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
            for (ResourceItemsResponse.Item item : items.getItems()) {
                updateItem(item);
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
            if (!(equalsWithNull(existingSkill.getName(), skill.getName())
                    && equalsWithNull(existingSkill.getDescription(), skill.getDescription())
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
            if (!(equalsWithNull(existingSkillLevel.getUnlocks(), skillLevel.getUnlocks())
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

    private static CollectionModel updateCollection(ResourceCollectionsResponse.Collection collection, String key) throws SqlException {
        CollectionModel existingCollection = collectionRefreshable.findFirstOrNull(
                CollectionModel::getCollectionKey, key
        );
        if (existingCollection != null) {
            if (!(equalsWithNull(existingCollection.getName(), collection.getName()))) {
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
            if (!(equalsWithNull(existingCollectionItem.getName(), collectionItem.getName())
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
            if (!(equalsWithNull(existingCollectionTier.getUnlocks(), collectionTier.getUnlocks())
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

    @SuppressWarnings("unchecked")
    public static void updateItem(ResourceItemsResponse.Item item) throws SqlException {
        ItemModel existingItem = itemRefreshable.findFirstOrNull(
                ItemModel::getItemId, item.getId()
        );
        Map<String, Object> requirements = gson.fromJson(gson.toJson(item.getRequirements()), HashMap.class);
        Map<String, Object> catacombsRequirements = gson.fromJson(gson.toJson(item.getCatacombsRequirements()), HashMap.class);
        Map<String, Object> essence = gson.fromJson(gson.toJson(item.getEssence()), HashMap.class);
        if (item.getTier() != null) {
            RarityModel existingRarity = rarityRefreshable.findFirstOrNull(
                    new Pair<>(RarityModel::getHypixelName, item.getTier()),
                    new Pair<>(RarityModel::isHasHypixelName, true)
            );
            if (existingRarity == null) {
                RarityModel newRarity = new RarityModel();
                newRarity.setName(item.getTier().substring(0, 1).toUpperCase()
                        + item.getTier().substring(1).toLowerCase()
                );
                newRarity.setHasHypixelName(true);
                newRarity.setHypixelName(item.getTier());
                rarityRepository.save(newRarity);
                rarityRefreshable.refreshItems();
            }
        }
        RarityModel rarity = rarityRefreshable.findFirstOrNull(
                new Pair<>(RarityModel::getHypixelName, item.getTier()),
                new Pair<>(RarityModel::isHasHypixelName, true)
        );
        if (existingItem != null) {
            if (!(equalsWithNull(existingItem.getName(), item.getName())
                    && equalsWithNull(existingItem.getMaterial(), item.getMaterial())
                    && existingItem.getDurability() == item.getDurability()
                    && equalsWithNull(existingItem.getSkin(), item.getSkin())
                    && equalsWithNull(existingItem.getFurniture(), item.getFurniture())
                    && equalsWithNull(existingItem.getRarity(), rarity)
                    && equalsWithNull(existingItem.getItemId(), item.getId())
                    && equalsWithNull(existingItem.getGenerator(), item.getGenerator())
                    && existingItem.getGeneratorTier() == item.getGeneratorTier()
                    && existingItem.isGlowing() == item.isGlowing()
                    && equalsWithNull(existingItem.getStats(), item.getStats())
                    && existingItem.getNpcSellPrice() == item.getNpcSellPrice()
                    && existingItem.isUnstackable() == item.isUnstackable()
                    && equalsWithNull(existingItem.getColor(), item.getColor())
                    && equalsWithNull(existingItem.getTieredStats(), item.getTieredStats())
                    && existingItem.getGearScore() == item.getGearScore()
                    && equalsWithNull(existingItem.getRequirements(), requirements)
                    && equalsWithNull(existingItem.getCatacombsRequirements(), catacombsRequirements)
                    && equalsWithNull(existingItem.getEssence(), essence)
                    && equalsWithNull(existingItem.getDescription(), item.getDescription())
                    && existingItem.getAbilityDamageScaling() == item.getAbilityDamageScaling()
                    && equalsWithNull(existingItem.getEnchantments(), item.getEnchantments())
                    && equalsWithNull(existingItem.getCrystal(), item.getCrystal())
                    && equalsWithNull(existingItem.getPrivateIsland(), item.getPrivateIsland())
                    && equalsWithNull(existingItem.getCategory(), item.getCategory())
            )) {
                existingItem.setName(item.getName());
                existingItem.setMaterial(item.getMaterial());
                existingItem.setDurability(item.getDurability());
                existingItem.setSkin(item.getSkin());
                existingItem.setFurniture(item.getFurniture());
                existingItem.setRarity(rarity);
                existingItem.setItemId(item.getId());
                existingItem.setGenerator(item.getGenerator());
                existingItem.setGeneratorTier(item.getGeneratorTier());
                existingItem.setGlowing(item.isGlowing());
                existingItem.setStats(item.getStats());
                existingItem.setNpcSellPrice(item.getNpcSellPrice());
                existingItem.setUnstackable(item.isUnstackable());
                existingItem.setColor(item.getColor());
                existingItem.setTieredStats(item.getTieredStats());
                existingItem.setGearScore(item.getGearScore());
                existingItem.setRequirements(requirements);
                existingItem.setCatacombsRequirements(catacombsRequirements);
                existingItem.setEssence(essence);
                existingItem.setDescription(item.getDescription());
                existingItem.setAbilityDamageScaling(item.getAbilityDamageScaling());
                existingItem.setEnchantments(item.getEnchantments());
                existingItem.setCrystal(item.getCrystal());
                existingItem.setPrivateIsland(item.getPrivateIsland());
                existingItem.setCategory(item.getCategory());
                itemRepository.update(existingItem);
                itemRefreshable.refreshItems();
            }
        } else {
            ItemModel newItem = new ItemModel();
            newItem.setItemId(item.getId());
            newItem.setName(item.getName());
            newItem.setMaterial(item.getMaterial());
            newItem.setDurability(item.getDurability());
            newItem.setSkin(item.getSkin());
            newItem.setFurniture(item.getFurniture());
            newItem.setRarity(rarity);
            newItem.setItemId(item.getId());
            newItem.setGenerator(item.getGenerator());
            newItem.setGeneratorTier(item.getGeneratorTier());
            newItem.setGlowing(item.isGlowing());
            newItem.setStats(item.getStats());
            newItem.setNpcSellPrice(item.getNpcSellPrice());
            newItem.setUnstackable(item.isUnstackable());
            newItem.setColor(item.getColor());
            newItem.setTieredStats(item.getTieredStats());
            newItem.setGearScore(item.getGearScore());
            newItem.setRequirements(requirements);
            newItem.setCatacombsRequirements(catacombsRequirements);
            newItem.setEssence(essence);
            newItem.setDescription(item.getDescription());
            newItem.setAbilityDamageScaling(item.getAbilityDamageScaling());
            newItem.setEnchantments(item.getEnchantments());
            newItem.setCrystal(item.getCrystal());
            newItem.setPrivateIsland(item.getPrivateIsland());
            newItem.setCategory(item.getCategory());
            itemRepository.saveOrUpdate(newItem);
            itemRefreshable.refreshItems();
        }
    }

    private static boolean equalsWithNull(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
}
