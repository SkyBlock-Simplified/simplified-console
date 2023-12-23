package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.impl.hypixel.response.resource.ResourceItemsResponse;
import dev.sbs.api.data.model.skyblock.accessory_data.accessories.AccessorySqlModel;
import dev.sbs.api.data.model.skyblock.item_types.ItemTypeSqlModel;
import dev.sbs.api.data.model.skyblock.items.ItemSqlModel;
import dev.sbs.api.data.model.skyblock.minion_data.minion_tiers.MinionTierSqlModel;
import dev.sbs.api.data.model.skyblock.minion_data.minions.MinionSqlModel;
import dev.sbs.api.data.model.skyblock.rarities.RaritySqlModel;
import dev.sbs.api.data.sql.SqlRepository;
import dev.sbs.api.util.collection.concurrent.Concurrent;
import dev.sbs.api.util.collection.concurrent.ConcurrentList;
import dev.sbs.api.util.collection.search.SearchFunction;
import dev.sbs.api.util.helper.StringUtil;
import dev.sbs.api.util.mutable.tuple.pair.Pair;
import dev.sbs.updater.processor.Processor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("all")
public class ResourceItemsProcessor extends Processor<ResourceItemsResponse> {

    // Repositories
    private static final SqlRepository<RaritySqlModel> rarityRepository = (SqlRepository<RaritySqlModel>) SimplifiedApi.getRepositoryOf(RaritySqlModel.class);
    private static final SqlRepository<ItemTypeSqlModel> itemTypeRepository = (SqlRepository<ItemTypeSqlModel>) SimplifiedApi.getRepositoryOf(ItemTypeSqlModel.class);
    private static final SqlRepository<ItemSqlModel> itemRepository = (SqlRepository<ItemSqlModel>) SimplifiedApi.getRepositoryOf(ItemSqlModel.class);
    private static final SqlRepository<AccessorySqlModel> accessoryRepository = (SqlRepository<AccessorySqlModel>) SimplifiedApi.getRepositoryOf(AccessorySqlModel.class);
    private static final SqlRepository<MinionSqlModel> minionRepository = (SqlRepository<MinionSqlModel>) SimplifiedApi.getRepositoryOf(MinionSqlModel.class);
    private static final SqlRepository<MinionTierSqlModel> minionTierRepository = (SqlRepository<MinionTierSqlModel>) SimplifiedApi.getRepositoryOf(MinionTierSqlModel.class);

    // Caches
    private static final ConcurrentList<RaritySqlModel> rarityCache = Concurrent.newList(rarityRepository.findAll());
    private static final ConcurrentList<ItemTypeSqlModel> itemTypeCache = Concurrent.newList(itemTypeRepository.findAll());
    private static final ConcurrentList<ItemSqlModel> itemCache = Concurrent.newList(itemRepository.findAll());
    private static final ConcurrentList<AccessorySqlModel> accessoryCache = Concurrent.newList(accessoryRepository.findAll());
    private static final ConcurrentList<MinionSqlModel> minionCache = Concurrent.newList(minionRepository.findAll());
    private static final ConcurrentList<MinionTierSqlModel> minionTierCache = Concurrent.newList(minionTierRepository.findAll());

    public ResourceItemsProcessor(ResourceItemsResponse resourceItemsResponse) {
        super(resourceItemsResponse);
    }

    @Override
    public void process() {
        ConcurrentList<ResourceItemsResponse.Item> items = this.getResourceResponse().getItems();

        items.forEach(itemEntry -> {
            this.getLog().info("Processing {} : {}/{}", itemEntry.getId(), items.indexOf(itemEntry), items.size());
            this.updateRarity(itemEntry); // Update `rarities`
            this.updateItemType(itemEntry); // Update `item_types`

            ItemSqlModel itemModel = this.updateItem(itemEntry); // Update `items`

            if (itemModel.getType() != null && itemModel.getType().getKey().equals("ACCESSORY"))
                this.updateAccessory(itemModel); // Update `accessories`

            if (StringUtil.isNotEmpty(itemModel.getGenerator())) {
                MinionSqlModel minion = this.updateMinion(itemModel); // Update `minions`
                MinionTierSqlModel minionTier = this.updateMinionTier(minion, itemModel); // Update `minion_tiers`
            }
        });
    }

    private AccessorySqlModel updateAccessory(ItemSqlModel item) {
        AccessorySqlModel existingAccessory = accessoryCache.findFirstOrNull(SearchFunction.combine(AccessorySqlModel::getItem, ItemSqlModel::getItemId), item.getItemId());
        Map<String, Double> stats = Concurrent.newMap(item.getStats());

        if (existingAccessory != null) {
            if (!equalsWithNull(item.getStats(), stats)
                || !equalsWithNull(item, existingAccessory.getItem())
                || !equalsWithNull(item.getRarity(), existingAccessory.getRarity())
                || !equalsWithNull(item.getName(), existingAccessory.getName())
            ) {
                existingAccessory.setItem(item);
                existingAccessory.setRarity(item.getRarity());
                existingAccessory.setName(item.getName());
                existingAccessory.setEffects(item.getStats());
                this.getLog().info("Updating existing accessory {}", existingAccessory.getItem().getItemId());
                existingAccessory.update();
            }

            return existingAccessory;
        } else {
            AccessorySqlModel newAccessory = new AccessorySqlModel();
            newAccessory.setItem(item);
            newAccessory.setName(item.getName());
            newAccessory.setRarity(item.getRarity());
            newAccessory.setFamilyRank(-1);
            newAccessory.setEffects(stats);
            this.getLog().info("Adding new accessory {}", newAccessory.getItem().getItemId());
            accessoryCache.add(newAccessory.save());
            return newAccessory;
        }
    }

    private MinionSqlModel updateMinion(ItemSqlModel item) {
        MinionSqlModel existingMinion = minionCache.findFirstOrNull(MinionSqlModel::getKey, item.getGenerator());
        String minionName = StringUtil.capitalizeFully(item.getGenerator().replace("_", " "));

        if (existingMinion != null) {
            if (!equalsWithNull(existingMinion.getName(), minionName)) {
                existingMinion.setName(minionName);
                this.getLog().info("Updating existing minion {} : {}", existingMinion.getKey(), minionName);
                existingMinion.update();
            }

            return existingMinion;
        } else {
            MinionSqlModel newMinion = new MinionSqlModel();
            newMinion.setKey(item.getGenerator());
            newMinion.setName(minionName);
            newMinion.setCollection(null);
            this.getLog().info("Adding new minion {}", newMinion.getKey());
            minionCache.add(newMinion.save());
            return newMinion;
        }
    }

    private MinionTierSqlModel updateMinionTier(MinionSqlModel minion, ItemSqlModel item) {
        MinionTierSqlModel existingMinionTier = minionTierCache.findFirstOrNull(
            Pair.of(MinionTierSqlModel::getMinion, minion),
            Pair.of(MinionTierSqlModel::getItem, item)
        );

        if (existingMinionTier != null) {
            if (!equalsWithNull(existingMinionTier.getMinion(), minion)
                || !equalsWithNull(existingMinionTier.getItem(), item)
            ) {
                existingMinionTier.setMinion(minion);
                existingMinionTier.setItem(item);
                this.getLog().info("Updating existing minion tier {}", existingMinionTier.getItem().getItemId());
                existingMinionTier.update();
            }

            return existingMinionTier;
        } else {
            MinionTierSqlModel newMinionTier = new MinionTierSqlModel();
            newMinionTier.setMinion(minion);
            newMinionTier.setItem(item);
            newMinionTier.setSpeed(-1);
            this.getLog().info("Adding new minion tier {}", newMinionTier.getItem().getItemId());
            minionTierCache.add(newMinionTier.save());
            return newMinionTier;
        }
    }

    private void updateRarity(ResourceItemsResponse.Item item) {
        if (StringUtil.isNotEmpty(item.getRarity())) {
            Optional<RaritySqlModel> existingRarity = rarityCache.findFirst(Pair.of(RaritySqlModel::getKey, item.getRarity()));

            if (existingRarity.isEmpty()) {
                RaritySqlModel newRarity = new RaritySqlModel();
                newRarity.setKey(item.getRarity());
                newRarity.setName(StringUtil.capitalize(item.getRarity()));
                newRarity.setOrdinal(
                    rarityCache.stream()
                        .mapToInt(RaritySqlModel::getOrdinal)
                        .max()
                        .orElseThrow() + 1
                );
                newRarity.setEnrichable(false);
                newRarity.setMagicPowerMultiplier(0);
                this.getLog().info("Adding new rarity {}", newRarity.getKey());
                rarityCache.add(newRarity.save());
            }
        }
    }

    private void updateItemType(ResourceItemsResponse.Item item) {
        if (StringUtil.isNotEmpty(item.getItemType()) && !item.getItemType().equals("NONE")) {
            Optional<ItemTypeSqlModel> existingItemType = itemTypeCache.findFirst(Pair.of(ItemTypeSqlModel::getKey, item.getItemType()));

            if (existingItemType.isEmpty()) {
                ItemTypeSqlModel newItemType = new ItemTypeSqlModel();
                newItemType.setKey(item.getItemType().toUpperCase());
                newItemType.setName(StringUtil.capitalizeFully(item.getItemType().replace("_", " ")));
                this.getLog().info("Adding new item type {}", newItemType.getKey());
                itemTypeCache.add(newItemType.save());
            }
        }
    }

    private ItemSqlModel updateItem(ResourceItemsResponse.Item item) {
        ItemSqlModel updateItem = itemCache.findFirstOrNull(ItemSqlModel::getItemId, item.getId());
        RaritySqlModel rarity = rarityCache.findFirstOrNull(RaritySqlModel::getKey, StringUtil.defaultIfEmpty(item.getRarity(), "COMMON").toUpperCase());
        ItemTypeSqlModel itemType = itemTypeCache.findFirstOrNull(ItemTypeSqlModel::getKey, item.getItemType());
        boolean updating = false;
        boolean isNew = false;

        // Wrap Null Values
        List<Map<String, Object>> requirements = Concurrent.newList(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getRequirements()), List.class));
        List<Map<String, Object>> catacombsRequirements = Concurrent.newList(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getCatacombsRequirements()), List.class));
        List<List<Map<String, Object>>> upgradeCosts = Concurrent.newList(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getUpgradeCosts()), List.class));
        List<Map<String, Object>> gemstoneSlots = Concurrent.newList(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getGemstoneSlots()), List.class));
        List<Map<String, Object>> salvages = Concurrent.newList(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getSalvages()), List.class));

        if (Objects.isNull(updateItem)) {
            updateItem = new ItemSqlModel();
            updating = true;
            isNew = true;
            this.getLog().info("Adding new item {}", item.getId());
        } else if (!equalsWithNull(updateItem.getItemId(), item.getId()) // Always true
            || !equalsWithNull(updateItem.getName(), item.getName())
            || !equalsWithNull(updateItem.getMaterial(), item.getMaterial())
            || updateItem.getDurability() != item.getDurability()
            || !equalsWithNull(updateItem.getDescription(), item.getDescription())
            || !equalsWithNull(updateItem.getRarity(), rarity)
            || !equalsWithNull(updateItem.getType(), itemType)
            || !equalsWithNull(updateItem.getColor(), item.getColor())
            || (rarity.getKey().equals("UNOBTAINABLE") && updateItem.isObtainable())
            || updateItem.isGlowing() != item.isGlowing()
            || updateItem.isUnstackable() != item.isUnstackable()
            || updateItem.isInSpecialMuseum() != item.isMuseum()
            || updateItem.isDungeonItem() != item.isDungeonItem()
            || updateItem.isAttributable() != item.isAttributable()
            || updateItem.isHiddenFromViewrecipe() != item.isHiddenFromViewrecipe()
            || updateItem.isSalvageableFromRecipe() != item.isSalvageableFromRecipe()
            || updateItem.isNotReforgeable() != item.isNotReforgeable()
            || updateItem.isRiftTransferrable() != item.isRiftTransferrable()
            || updateItem.isRiftLoseMotesValueOnTransfer() != item.isRiftLoseMotesValueOnTransfer()
            || updateItem.getRiftMotesSellPrice() != item.getRiftMotesSellPrice()
            || updateItem.getNpcSellPrice() != item.getNpcSellPrice()
            || updateItem.getGearScore() != item.getGearScore()
            || !equalsWithNull(updateItem.getGenerator(), item.getGenerator())
            || updateItem.getGeneratorTier() != item.getGeneratorTier()
            || updateItem.getAbilityDamageScaling() != item.getAbilityDamageScaling()
            || !equalsWithNull(updateItem.getOrigin(), item.getOrigin())
            || !equalsWithNull(updateItem.getSoulbound(), item.getSoulbound())
            || !equalsWithNull(updateItem.getFurniture(), item.getFurniture())
            || !equalsWithNull(updateItem.getSwordType(), item.getSwordType())
            || !equalsWithNull(updateItem.getSkin(), item.getSkin())
            || !equalsWithNull(updateItem.getCrystal(), item.getCrystal())
            || !equalsWithNull(updateItem.getPrivateIsland(), item.getPrivateIsland())
            || !equalsWithNull(updateItem.getStats(), item.getStats())
            || !equalsWithNull(updateItem.getTieredStats(), item.getTieredStats())
            || !equalsWithNull(updateItem.getRequirements(), requirements)
            || !equalsWithNull(updateItem.getCatacombsRequirements(), catacombsRequirements)
            || !equalsWithNull(updateItem.getUpgradeCosts(), upgradeCosts)
            || !equalsWithNull(updateItem.getGemstoneSlots(), gemstoneSlots)
            || !equalsWithNull(updateItem.getEnchantments(), item.getEnchantments())
            || !equalsWithNull(updateItem.getDungeonItemConversionCost(), item.getDungeonItemConversionCost())
            || !equalsWithNull(updateItem.getPrestige(), item.getPrestige())
            || !equalsWithNull(updateItem.getItemSpecific(), item.getItemSpecific())
            || !equalsWithNull(updateItem.getSalvages(), salvages)
        ) {
            updating = true;
            this.getLog().info("Updating existing item {}", updateItem.getItemId());
        }

        if (updating) {
            updateItem.setItemId(item.getId());
            updateItem.setName(item.getName());
            updateItem.setMaterial(item.getMaterial());
            updateItem.setDurability(item.getDurability());
            updateItem.setDescription(item.getDescription());
            updateItem.setRarity(rarity);
            updateItem.setType(itemType);
            updateItem.setColor(item.getColor());
            updateItem.setObtainable(!rarity.getKey().equals("UNOBTAINABLE"));
            updateItem.setGlowing(item.isGlowing());
            updateItem.setUnstackable(item.isUnstackable());
            updateItem.setInSpecialMuseum(item.isMuseum());
            updateItem.setDungeonItem(item.isDungeonItem());
            updateItem.setAttributable(item.isAttributable());
            updateItem.setHiddenFromViewrecipe(item.isHiddenFromViewrecipe());
            updateItem.setSalvageableFromRecipe(item.isSalvageableFromRecipe());
            updateItem.setNotReforgeable(item.isNotReforgeable());
            updateItem.setRiftTransferrable(item.isRiftTransferrable());
            updateItem.setRiftLoseMotesValueOnTransfer(item.isRiftLoseMotesValueOnTransfer());
            updateItem.setRiftMotesSellPrice(item.getRiftMotesSellPrice());
            updateItem.setNpcSellPrice(item.getNpcSellPrice());
            updateItem.setGearScore(item.getGearScore());
            updateItem.setGenerator(item.getGenerator());
            updateItem.setGeneratorTier(item.getGeneratorTier());
            updateItem.setAbilityDamageScaling(item.getAbilityDamageScaling());
            updateItem.setOrigin(item.getOrigin());
            updateItem.setSoulbound(item.getSoulbound());
            updateItem.setFurniture(item.getFurniture());
            updateItem.setSwordType(item.getSwordType());
            updateItem.setSkin(item.getSkin());
            updateItem.setCrystal(item.getCrystal());
            updateItem.setPrivateIsland(item.getPrivateIsland());
            updateItem.setStats(item.getStats());
            updateItem.setTieredStats(item.getTieredStats());
            updateItem.setRequirements(requirements);
            updateItem.setCatacombsRequirements(catacombsRequirements);
            updateItem.setUpgradeCosts(upgradeCosts);
            updateItem.setGemstoneSlots(gemstoneSlots);
            updateItem.setEnchantments(item.getEnchantments());
            updateItem.setDungeonItemConversionCost(item.getDungeonItemConversionCost());
            updateItem.setPrestige(item.getPrestige());
            updateItem.setItemSpecific(item.getItemSpecific());
            updateItem.setSalvages(salvages);

            if (isNew) {
                updateItem = updateItem.save();
                itemCache.add(updateItem);
            } else
                updateItem = updateItem.update();
        }

        return updateItem;
    }

}
