package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceItemsResponse;
import dev.sbs.api.data.model.skyblock.accessories.AccessorySqlModel;
import dev.sbs.api.data.model.skyblock.accessories.AccessorySqlRepository;
import dev.sbs.api.data.model.skyblock.items.ItemSqlModel;
import dev.sbs.api.data.model.skyblock.items.ItemSqlRepository;
import dev.sbs.api.data.model.skyblock.minion_tiers.MinionTierSqlModel;
import dev.sbs.api.data.model.skyblock.minion_tiers.MinionTierSqlRepository;
import dev.sbs.api.data.model.skyblock.minions.MinionSqlModel;
import dev.sbs.api.data.model.skyblock.minions.MinionSqlRepository;
import dev.sbs.api.data.model.skyblock.rarities.RaritySqlModel;
import dev.sbs.api.data.model.skyblock.rarities.RaritySqlRepository;
import dev.sbs.api.util.collection.concurrent.Concurrent;
import dev.sbs.api.util.collection.concurrent.ConcurrentList;
import dev.sbs.api.util.collection.search.function.SearchFunction;
import dev.sbs.api.util.data.tuple.Pair;
import dev.sbs.api.util.helper.StringUtil;
import dev.sbs.api.util.helper.WordUtil;
import dev.sbs.updater.processor.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("all")
public class ResourceItemsProcessor extends Processor<ResourceItemsResponse> {

    private static final RaritySqlRepository rarityRepository = (RaritySqlRepository) SimplifiedApi.getRepositoryOf(RaritySqlModel.class);
    private static final ItemSqlRepository itemRepository = (ItemSqlRepository) SimplifiedApi.getRepositoryOf(ItemSqlModel.class);
    private static final AccessorySqlRepository accessoryRepository = (AccessorySqlRepository) SimplifiedApi.getRepositoryOf(AccessorySqlModel.class);
    private static final MinionSqlRepository minionRepository = (MinionSqlRepository) SimplifiedApi.getRepositoryOf(MinionSqlModel.class);
    private static final MinionTierSqlRepository minionTierRepository = (MinionTierSqlRepository) SimplifiedApi.getRepositoryOf(MinionTierSqlModel.class);

    public ResourceItemsProcessor(ResourceItemsResponse resourceItemsResponse) {
        super(resourceItemsResponse);
    }

    @Override
    public void process() {
        ConcurrentList<ResourceItemsResponse.Item> items = this.getResourceResponse().getItems();

        items.forEach(itemEntry -> {
            this.getLog().info("Processing {0} : {1}/{2}", itemEntry.getId(), items.indexOf(itemEntry), items.size());
            this.updateRarity(itemEntry); // Update `rarities`

            ItemSqlModel itemModel = this.updateItem(itemEntry); // Update `items`

            if ("ACCESSORY".equals(itemModel.getCategory()))
                this.updateAccessory(itemModel); // Update `accessories`

            if (StringUtil.isNotEmpty(itemModel.getGenerator())) {
                MinionSqlModel minion = this.updateMinion(itemModel); // Update `minions`
                MinionTierSqlModel minionTier = this.updateMinionTier(minion, itemModel); // Update `minion_tiers`
            }
        });
    }

    private AccessorySqlModel updateAccessory(ItemSqlModel item) {
        AccessorySqlModel existingAccessory = accessoryRepository.findFirstOrNull(SearchFunction.combine(AccessorySqlModel::getItem, ItemSqlModel::getItemId), item.getItemId());
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
                this.getLog().info("Updating existing accessory {0}", existingAccessory.getItem().getItemId());
                existingAccessory.update();
            }

            return existingAccessory;
        } else {
            AccessorySqlModel newAccessory = new AccessorySqlModel();
            newAccessory.setItem(item);
            newAccessory.setName(item.getName());
            newAccessory.setRarity(item.getRarity());
            newAccessory.setFamilyRank(-1);
            newAccessory.setAttainable(true);
            newAccessory.setEffects(stats);
            this.getLog().info("Adding new accessory {0}", newAccessory.getItem().getItemId());
            return newAccessory.save();
        }
    }

    private MinionSqlModel updateMinion(ItemSqlModel item) {
        MinionSqlModel existingMinion = minionRepository.findFirstOrNull(MinionSqlModel::getKey, item.getGenerator());
        String minionName = WordUtil.capitalizeFully(item.getGenerator().replace("_", " "));

        if (existingMinion != null) {
            if (!equalsWithNull(existingMinion.getName(), minionName)) {
                existingMinion.setName(minionName);
                this.getLog().info("Updating existing minion {0} : {1}", existingMinion.getKey(), minionName);
                existingMinion.update();
            }

            return existingMinion;
        } else {
            MinionSqlModel newMinion = new MinionSqlModel();
            newMinion.setKey(item.getGenerator());
            newMinion.setName(minionName);
            newMinion.setCollection(null);
            this.getLog().info("Adding new minion {0}", newMinion.getKey());
            return newMinion.save();
        }
    }

    private MinionTierSqlModel updateMinionTier(MinionSqlModel minion, ItemSqlModel item) {
        MinionTierSqlModel existingMinionTier = minionTierRepository.findFirstOrNull(
            Pair.of(MinionTierSqlModel::getMinion, minion),
            Pair.of(MinionTierSqlModel::getItem, item)
        );

        if (existingMinionTier != null) {
            if (!equalsWithNull(existingMinionTier.getMinion(), minion)
                || !equalsWithNull(existingMinionTier.getItem(), item)
            ) {
                existingMinionTier.setMinion(minion);
                existingMinionTier.setItem(item);
                this.getLog().info("Updating existing minion tier {0}", existingMinionTier.getItem().getItemId());
                existingMinionTier.update();
            }

            return existingMinionTier;
        } else {
            MinionTierSqlModel newMinionTier = new MinionTierSqlModel();
            newMinionTier.setMinion(minion);
            newMinionTier.setItem(item);
            newMinionTier.setSpeed(-1);
            this.getLog().info("Adding new minion tier {0}", newMinionTier.getItem().getItemId());
            return newMinionTier.save();
        }
    }

    private void updateRarity(ResourceItemsResponse.Item item) {
        if (StringUtil.isNotEmpty(item.getTier())) {
            Optional<RaritySqlModel> existingRarity = rarityRepository.findFirst(Pair.of(RaritySqlModel::getKey, item.getTier()));

            if (existingRarity.isEmpty()) {
                RaritySqlModel newRarity = new RaritySqlModel();
                newRarity.setKey(item.getTier());
                newRarity.setName(WordUtil.capitalize(item.getTier()));
                newRarity.setKeyValid(true);
                this.getLog().info("Adding new rarity {0}", newRarity.getKey());
                newRarity.save();
            }
        }
    }

    private ItemSqlModel updateItem(ResourceItemsResponse.Item item) {
        ItemSqlModel existingItem = itemRepository.findFirstOrNull(ItemSqlModel::getItemId, item.getId());
        RaritySqlModel rarity = rarityRepository.findFirstOrNull(RaritySqlModel::getKey, StringUtil.defaultIfEmpty(item.getTier(), "COMMON").toUpperCase());

        // Wrap Null Values
        Map<String, Object> requirements = Concurrent.newMap(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getRequirements()), HashMap.class));
        Map<String, Object> catacombsRequirements = Concurrent.newMap(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getCatacombsRequirements()), HashMap.class));
        Map<String, Object> essence = Concurrent.newMap(SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getEssence()), HashMap.class));
        
        if (existingItem != null) {
            if (!equalsWithNull(existingItem.getName(), item.getName())
                || !equalsWithNull(existingItem.getMaterial(), item.getMaterial())
                || existingItem.getDurability() != item.getDurability()
                || !equalsWithNull(existingItem.getSkin(), item.getSkin())
                || !equalsWithNull(existingItem.getFurniture(), item.getFurniture())
                || !equalsWithNull(existingItem.getRarity(), rarity)
                || !equalsWithNull(existingItem.getItemId(), item.getId())
                || !equalsWithNull(existingItem.getGenerator(), item.getGenerator())
                || existingItem.getGeneratorTier() != item.getGeneratorTier()
                || existingItem.isGlowing() != item.isGlowing()
                || !equalsWithNull(existingItem.getCategory(), item.getCategory())
                || !equalsWithNull(existingItem.getStats(), item.getStats())
                || existingItem.getNpcSellPrice() != item.getNpcSellPrice()
                || existingItem.isUnstackable() != item.isUnstackable()
                || existingItem.isDungeonItem() != item.isDungeonItem()
                || !equalsWithNull(existingItem.getColor(), item.getColor())
                || !equalsWithNull(existingItem.getTieredStats(), item.getTieredStats())
                || existingItem.getGearScore() != item.getGearScore()
                || !equalsWithNull(existingItem.getRequirements(), requirements)
                || !equalsWithNull(existingItem.getCatacombsRequirements(), catacombsRequirements)
                || !equalsWithNull(existingItem.getEssence(), essence)
                || !equalsWithNull(existingItem.getDescription(), item.getDescription())
                || existingItem.getAbilityDamageScaling() != item.getAbilityDamageScaling()
                || !equalsWithNull(existingItem.getEnchantments(), item.getEnchantments())
                || !equalsWithNull(existingItem.getCrystal(), item.getCrystal())
                || !equalsWithNull(existingItem.getPrivateIsland(), item.getPrivateIsland())
            ) {
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
                existingItem.setCategory(item.getCategory());
                existingItem.setStats(item.getStats());
                existingItem.setNpcSellPrice(item.getNpcSellPrice());
                existingItem.setUnstackable(item.isUnstackable());
                existingItem.setDungeonItem(item.isDungeonItem());
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
                this.getLog().info("Updating existing item {0}", existingItem.getItemId());
                existingItem.update();
            }
            
            return existingItem;
        } else {
            ItemSqlModel newItem = new ItemSqlModel();
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
            newItem.setCategory(item.getCategory());
            newItem.setStats(item.getStats());
            newItem.setNpcSellPrice(item.getNpcSellPrice());
            newItem.setUnstackable(item.isUnstackable());
            newItem.setDungeonItem(item.isDungeonItem());
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
            this.getLog().info("Adding new item {0}", newItem.getItemId());
            return newItem.save();
        }
    }

}
