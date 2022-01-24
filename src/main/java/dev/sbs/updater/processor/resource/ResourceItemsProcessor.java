package dev.sbs.updater.processor.resource;

import ch.qos.logback.classic.Level;
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
import dev.sbs.api.data.sql.function.FilterFunction;
import dev.sbs.api.util.concurrent.Concurrent;
import dev.sbs.api.util.concurrent.ConcurrentList;
import dev.sbs.api.util.helper.StringUtil;
import dev.sbs.api.util.helper.WordUtil;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("all")
public class ResourceItemsProcessor extends Processor<ResourceItemsResponse> {

    private static final ConcurrentList<Pair<String, String>> statRepair = Concurrent.newList(
        Pair.of("WALK_SPEED", "SPEED"),
        Pair.of("CRTICIAL_DAMAGE", "CRIT_DAMAGE"),
        Pair.of("ATTACK_SPEED", "BONUS_ATTACK_SPEED")
    );
    private static final RaritySqlRepository rarityRepository = (RaritySqlRepository) SimplifiedApi.getRepositoryOf(RaritySqlModel.class);
    private static final ItemSqlRepository itemRepository = (ItemSqlRepository) SimplifiedApi.getRepositoryOf(ItemSqlModel.class);
    private static final AccessorySqlRepository accessoryRepository = (AccessorySqlRepository) SimplifiedApi.getRepositoryOf(AccessorySqlModel.class);
    private static final MinionSqlRepository minionRepository = (MinionSqlRepository) SimplifiedApi.getRepositoryOf(MinionSqlModel.class);
    private static final MinionTierSqlRepository minionTierRepository = (MinionTierSqlRepository) SimplifiedApi.getRepositoryOf(MinionTierSqlModel.class);
    private static final RaritySqlModel commonRarityModel = SimplifiedApi.getRepositoryOf(RaritySqlModel.class).findFirstOrNull(RaritySqlModel::getKey, "COMMON");

    public ResourceItemsProcessor(ResourceItemsResponse resourceItemsResponse) {
        super(resourceItemsResponse);
    }

    @Override
    public void process() {
        for (ResourceItemsResponse.Item itemEntry : super.getResourceResponse().getItems()) {
            this.updateRarity(itemEntry); // Update `rarities`
            ItemSqlModel item = this.updateItem(itemEntry); // Update `items`

            if ("ACCESSORY".equals(item.getCategory()))
                this.updateAccessory(item); // Update `accessories`

            if (StringUtil.isNotEmpty(item.getGenerator())) {
                SimplifiedApi.getConfig().setLoggingLevel(Level.DEBUG);
                MinionSqlModel minion = this.updateMinion(item); // Update `minions`
                MinionTierSqlModel minionTier = this.updateMinionTier(minion, item); // Update `minion_tiers`
                SimplifiedApi.getConfig().setLoggingLevel(Level.WARN);
            }
        }
    }

    private AccessorySqlModel updateAccessory(ItemSqlModel item) {
        AccessorySqlModel existingAccessory = accessoryRepository.findFirstOrNull(FilterFunction.combine(AccessorySqlModel::getItem, ItemSqlModel::getItemId), item.getItemId());

        if (existingAccessory != null) {
            if (!item.getStats().equals(existingAccessory.getEffects())
                    || !equalsWithNull(item, existingAccessory.getItem())
                    || !equalsWithNull(item.getRarity(), existingAccessory.getRarity())
                    || !equalsWithNull(item.getName(), existingAccessory.getName())
            ) {
                existingAccessory.setItem(item);
                existingAccessory.setRarity(item.getRarity());
                existingAccessory.setName(item.getName());
                existingAccessory.setEffects(item.getStats());
                this.getLog().info("Updating existing accessory {0}", existingAccessory.getItem().getItemId());
                accessoryRepository.update(existingAccessory);
            }

            return existingAccessory;
        } else {
            AccessorySqlModel newAccessory = new AccessorySqlModel();
            newAccessory.setItem(item);
            newAccessory.setRarity(item.getRarity());
            newAccessory.setName(item.getName());
            newAccessory.setEffects(item.getStats());
            this.getLog().info("Adding new accessory {0}", newAccessory.getItem().getItemId());
            long id = accessoryRepository.save(newAccessory);
            return accessoryRepository.findFirstOrNull(AccessorySqlModel::getId, id);
        }
    }

    private MinionSqlModel updateMinion(ItemSqlModel item) {
        MinionSqlModel existingMinion = minionRepository.findFirstOrNull(MinionSqlModel::getKey, item.getGenerator());
        String minionName = WordUtil.capitalizeFully(item.getGenerator().replace("_", " "));

        if (existingMinion != null) {
            if (!equalsWithNull(existingMinion.getName(), minionName)) {
                existingMinion.setName(minionName);
                this.getLog().info("Updating existing minion {0} : {1}", existingMinion.getKey(), minionName);
                minionRepository.update(existingMinion);
            }

            return existingMinion;
        } else {
            MinionSqlModel newMinion = new MinionSqlModel();
            newMinion.setKey(item.getGenerator());
            newMinion.setName(minionName);
            newMinion.setCollection(null);
            this.getLog().info("Adding new minion {0}", newMinion.getKey());
            long id = minionRepository.save(newMinion);
            return minionRepository.findFirstOrNull(MinionSqlModel::getId, id);
        }
    }

    private MinionTierSqlModel updateMinionTier(MinionSqlModel minion, ItemSqlModel item) {
        MinionTierSqlModel existingMinionTier = minionTierRepository.findFirstOrNull(
            Pair.of(MinionTierSqlModel::getMinion, minion),
            Pair.of(MinionTierSqlModel::getItem, item)
        );

        if (existingMinionTier != null) {
            if (!equalsWithNull(existingMinionTier.getMinion(), minion)
                    || !(equalsWithNull(existingMinionTier.getItem(), item))
            ) {
                existingMinionTier.setMinion(minion);
                existingMinionTier.setItem(item);
                this.getLog().info("Updating existing minion tier {0}", existingMinionTier.getMinion().getKey());
                minionTierRepository.update(existingMinionTier);
            }

            return existingMinionTier;
        } else {
            MinionTierSqlModel newMinionTier = new MinionTierSqlModel();
            newMinionTier.setMinion(minion);
            newMinionTier.setItem(item);
            this.getLog().info("Adding new minion tier {0}", newMinionTier.getMinion().getKey());
            long id = minionTierRepository.save(newMinionTier);
            return minionTierRepository.findFirstOrNull(MinionTierSqlModel::getId, id);
        }
    }

    private RaritySqlModel updateRarity(ResourceItemsResponse.Item item) {
        if (StringUtil.isNotEmpty(item.getTier())) {
            RaritySqlModel existingRarity = rarityRepository.findFirstOrNull(Pair.of(RaritySqlModel::getKey, item.getTier()));

            if (existingRarity == null) {
                RaritySqlModel newRarity = new RaritySqlModel();
                newRarity.setKey(item.getTier());
                newRarity.setName(WordUtil.capitalize(item.getTier()));
                newRarity.setKeyValid(true);
                this.getLog().info("Adding new rarity {0}", newRarity.getKey());
                long id = rarityRepository.save(newRarity);
                return rarityRepository.findFirstOrNull(RaritySqlModel::getId, id);
            }

            return existingRarity;
        }

        return null;
    }

    private ItemSqlModel updateItem(ResourceItemsResponse.Item item) {
        ItemSqlModel existingItem = itemRepository.findFirstOrNull(ItemSqlModel::getItemId, item.getId());
        Map<String, Object> requirements = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getRequirements()), HashMap.class);
        Map<String, Object> catacombsRequirements = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getCatacombsRequirements()), HashMap.class);
        Map<String, Object> essence = SimplifiedApi.getGson().fromJson(SimplifiedApi.getGson().toJson(item.getEssence()), HashMap.class);
        RaritySqlModel rarity = rarityRepository.findFirst(Pair.of(RaritySqlModel::getName, item.getTier())).orElse(commonRarityModel);

        // Fix Stats
        for (Pair<String, String> pair : statRepair) {
            if (item.getStats().containsKey(pair.getLeft())) {
                item.getStats().put(pair.getRight(), item.getStats().get(pair.getLeft()));
                item.getStats().remove(pair.getLeft());
            }
        }
        
        if (existingItem != null) {
            if (!equalsWithNull(existingItem.getName(), item.getName())
                    || !equalsWithNull(existingItem.getMaterial(), item.getMaterial())
                    || existingItem.getDurability() != item.getDurability()
                    || !equalsWithNull(existingItem.getSkin(), item.getSkin())
                    && !equalsWithNull(existingItem.getFurniture(), item.getFurniture())
                    && !equalsWithNull(existingItem.getRarity(), rarity)
                    && !equalsWithNull(existingItem.getItemId(), item.getId())
                    && !equalsWithNull(existingItem.getGenerator(), item.getGenerator())
                    && existingItem.getGeneratorTier() != item.getGeneratorTier()
                    && existingItem.isGlowing() != item.isGlowing()
                    && !equalsWithNull(existingItem.getStats(), item.getStats())
                    && existingItem.getNpcSellPrice() != item.getNpcSellPrice()
                    && existingItem.isUnstackable() != item.isUnstackable()
                    && !equalsWithNull(existingItem.getColor(), item.getColor())
                    && !equalsWithNull(existingItem.getTieredStats(), item.getTieredStats())
                    && existingItem.getGearScore() != item.getGearScore()
                    && !equalsWithNull(existingItem.getRequirements(), requirements)
                    && !equalsWithNull(existingItem.getCatacombsRequirements(), catacombsRequirements)
                    && !equalsWithNull(existingItem.getEssence(), essence)
                    && !equalsWithNull(existingItem.getDescription(), item.getDescription())
                    && existingItem.getAbilityDamageScaling() != item.getAbilityDamageScaling()
                    && !equalsWithNull(existingItem.getEnchantments(), item.getEnchantments())
                    && !equalsWithNull(existingItem.getCrystal(), item.getCrystal())
                    && !equalsWithNull(existingItem.getPrivateIsland(), item.getPrivateIsland())
                    && !equalsWithNull(existingItem.getCategory(), item.getCategory())
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
                this.getLog().info("Updating existing item {0}", existingItem.getItemId());
                itemRepository.update(existingItem);
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
            this.getLog().info("Adding new item {0}", newItem.getItemId());
            long id = itemRepository.save(newItem);
            return itemRepository.findFirstOrNull(ItemSqlModel::getId, id);
        }
    }

}
