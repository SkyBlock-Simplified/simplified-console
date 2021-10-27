package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceItemsResponse;
import dev.sbs.api.data.sql.function.FilterFunction;
import dev.sbs.api.model.sql.accessories.AccessoryRepository;
import dev.sbs.api.model.sql.accessories.AccessorySqlModel;
import dev.sbs.api.model.sql.items.ItemRepository;
import dev.sbs.api.model.sql.items.ItemSqlModel;
import dev.sbs.api.model.sql.minions.MinionRepository;
import dev.sbs.api.model.sql.minions.MinionSqlModel;
import dev.sbs.api.model.sql.minions.miniontiers.MinionTierRepository;
import dev.sbs.api.model.sql.minions.miniontiers.MinionTierSqlModel;
import dev.sbs.api.model.sql.rarities.RarityRepository;
import dev.sbs.api.model.sql.rarities.RaritySqlModel;
import dev.sbs.api.util.concurrent.Concurrent;
import dev.sbs.api.util.concurrent.ConcurrentMap;
import dev.sbs.api.util.helper.StringUtil;
import dev.sbs.api.util.helper.WordUtil;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("all")
public class ResourceItemsProcessor extends Processor<ResourceItemsResponse> {

    private static final RarityRepository rarityRepository = SimplifiedApi.getSqlRepository(RarityRepository.class);
    private static final ItemRepository itemRepository = SimplifiedApi.getSqlRepository(ItemRepository.class);
    private static final AccessoryRepository accessoryRepository = SimplifiedApi.getSqlRepository(AccessoryRepository.class);
    private static final MinionRepository minionRepository = SimplifiedApi.getSqlRepository(MinionRepository.class);
    private static final MinionTierRepository minionTierRepository = SimplifiedApi.getSqlRepository(MinionTierRepository.class);

    public ResourceItemsProcessor(ResourceItemsResponse resourceItemsResponse) {
        super(resourceItemsResponse);
    }

    @Override
    public void process() {
        for (ResourceItemsResponse.Item itemEntry : super.getResourceResponse().getItems()) {
            RaritySqlModel rarity = updateRarity(itemEntry); // Update `rarities`
            ItemSqlModel item = updateItem(itemEntry); // Update `items`

            if ("ACCESSORY".equals(item.getCategory()))
                updateAccessory(item); // Update `accessories`

            if (StringUtil.isNotEmpty(item.getGenerator())) {
                MinionSqlModel minion = updateMinion(item); // Update `minions`
                MinionTierSqlModel minionTier = updateMinionTier(minion, item); // Update `minion_tiers`
            }
        }
    }

    @SneakyThrows
    private static AccessorySqlModel updateAccessory(ItemSqlModel item) {
        AccessorySqlModel existingAccessory = accessoryRepository.findFirstOrNullCached(FilterFunction.combine(AccessorySqlModel::getItem, ItemSqlModel::getItemId), item.getItemId());

        ConcurrentMap<String, Object> stats = Concurrent.newMap(item.getStats());
        stats.forEach(stat -> { // TODO: Automate this using stats table?
            if (stat.getKey().equals("WALK_SPEED")) {
                stats.put(stat.getKey().replace("WALK_", ""), stat.getValue());
                stats.remove(stat.getKey());
            } else if (stat.getKey().contains("CRITICAL_")) {
                stats.put(stat.getKey().replace("CRITICAL_", "CRIT_"), stat.getValue());
                stats.remove(stat.getKey());
            }
        });

        if (existingAccessory != null) {
            if (!stats.equals(existingAccessory.getEffects())
                    && !equalsWithNull(item, existingAccessory.getItem())
                    && !equalsWithNull(item.getRarity(), existingAccessory.getRarity())
                    && !equalsWithNull(item.getName(), existingAccessory.getName())
            ) {
                existingAccessory.setItem(item);
                existingAccessory.setRarity(item.getRarity());
                existingAccessory.setName(item.getName());
                existingAccessory.setEffects(stats);
                accessoryRepository.update(existingAccessory);
                accessoryRepository.refreshItems();
            }

            return existingAccessory;
        } else {
            AccessorySqlModel newAccessory = new AccessorySqlModel();
            newAccessory.setItem(item);
            newAccessory.setRarity(item.getRarity());
            newAccessory.setName(item.getName());
            newAccessory.setEffects(stats);
            long id = accessoryRepository.save(newAccessory);
            accessoryRepository.refreshItems();
            return accessoryRepository.findFirstOrNullCached(AccessorySqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static MinionSqlModel updateMinion(ItemSqlModel item) {
        MinionSqlModel existingMinion = minionRepository.findFirstOrNullCached(MinionSqlModel::getKey, item.getGenerator());
        String minionName = WordUtil.capitalize(item.getGenerator().replace("_", ""));

        if (existingMinion != null) {
            if (!equalsWithNull(existingMinion.getName(), minionName)) {
                existingMinion.setName(minionName);
                minionRepository.update(existingMinion);
                minionRepository.refreshItems();
            }

            return existingMinion;
        } else {
            MinionSqlModel newMinion = new MinionSqlModel();
            newMinion.setKey(item.getGenerator());
            newMinion.setName(minionName);
            newMinion.setCollection(null);
            long id = minionRepository.save(newMinion);
            minionRepository.refreshItems();
            return minionRepository.findFirstOrNullCached(MinionSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static MinionTierSqlModel updateMinionTier(MinionSqlModel minion, ItemSqlModel item) {
        MinionTierSqlModel existingMinionTier = minionTierRepository.findFirstOrNullCached(FilterFunction.combine(MinionTierSqlModel::getMinion, MinionSqlModel::getKey), minion.getKey());

        if (existingMinionTier != null) {
            if (!equalsWithNull(existingMinionTier.getMinion(), minion)
                    && !(equalsWithNull(existingMinionTier.getItem(), item))
            ) {
                existingMinionTier.setMinion(minion);
                existingMinionTier.setItem(item);
                minionTierRepository.update(existingMinionTier);
                minionTierRepository.refreshItems();
            }

            return existingMinionTier;
        } else {
            MinionTierSqlModel newMinionTier = new MinionTierSqlModel();
            newMinionTier.setMinion(minion);
            newMinionTier.setItem(item);
            long id = minionTierRepository.save(newMinionTier);
            minionTierRepository.refreshItems();
            return minionTierRepository.findFirstOrNullCached(MinionTierSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static RaritySqlModel updateRarity(ResourceItemsResponse.Item item) {
        if (item.getTier() != null) {
            RaritySqlModel existingRarity = rarityRepository.findFirstOrNullCached(Pair.of(RaritySqlModel::getKey, item.getTier()));

            if (existingRarity == null) {
                RaritySqlModel newRarity = new RaritySqlModel();
                newRarity.setKey(item.getTier());
                newRarity.setName(WordUtil.capitalize(item.getTier()));
                newRarity.setKeyValid(true);
                long id = rarityRepository.save(newRarity);
                rarityRepository.refreshItems();
                return rarityRepository.findFirstOrNullCached(RaritySqlModel::getId, id);
            }

            return existingRarity;
        }

        return null;
    }

    @SneakyThrows
    private static ItemSqlModel updateItem(ResourceItemsResponse.Item item) {
        ItemSqlModel existingItem = itemRepository.findFirstOrNullCached(ItemSqlModel::getItemId, item.getId());
        Map<String, Object> requirements = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getRequirements()), HashMap.class);
        Map<String, Object> catacombsRequirements = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getCatacombsRequirements()), HashMap.class);
        Map<String, Object> essence = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getEssence()), HashMap.class);
        
        RaritySqlModel rarity = rarityRepository.findFirstOrNullCached(
                Pair.of(RaritySqlModel::getName, item.getTier()),
                Pair.of(RaritySqlModel::isKeyValid, true)
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
                itemRepository.refreshItems();
            }
            
            return existingItem;
        } else {
            ItemSqlModel newItem = new ItemSqlModel();
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
            long id = itemRepository.save(newItem);
            itemRepository.refreshItems();
            return itemRepository.findFirstOrNullCached(ItemSqlModel::getId, id);
        }
    }

}
