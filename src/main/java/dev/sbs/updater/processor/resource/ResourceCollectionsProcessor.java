package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.data.sql.function.FilterFunction;
import dev.sbs.api.model.sql.collections.CollectionRepository;
import dev.sbs.api.model.sql.collections.CollectionSqlModel;
import dev.sbs.api.model.sql.collections.collectionitems.CollectionItemRepository;
import dev.sbs.api.model.sql.collections.collectionitems.CollectionItemSqlModel;
import dev.sbs.api.model.sql.collections.collectionitemtiers.CollectionItemTierRepository;
import dev.sbs.api.model.sql.collections.collectionitemtiers.CollectionItemTierSqlModel;
import dev.sbs.api.model.sql.items.ItemRepository;
import dev.sbs.api.model.sql.items.ItemSqlModel;
import dev.sbs.api.model.sql.skills.SkillRepository;
import dev.sbs.api.model.sql.skills.SkillSqlModel;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;
import lombok.SneakyThrows;

import java.util.Map;

public class ResourceCollectionsProcessor extends Processor<ResourceCollectionsResponse> {

    private static final CollectionRepository collectionRepository = SimplifiedApi.getSqlRepository(CollectionRepository.class);
    private static final CollectionItemRepository collectionItemRepository = SimplifiedApi.getSqlRepository(CollectionItemRepository.class);
    private static final CollectionItemTierRepository collectionItemTierRepository = SimplifiedApi.getSqlRepository(CollectionItemTierRepository.class);
    private static final SkillRepository skillRepository = SimplifiedApi.getSqlRepository(SkillRepository.class);
    private static final ItemRepository itemRepository = SimplifiedApi.getSqlRepository(ItemRepository.class);

    public ResourceCollectionsProcessor(ResourceCollectionsResponse resourceResponse) {
        super(resourceResponse);
    }

    @Override
    public void process() {
        for (Map.Entry<String, ResourceCollectionsResponse.Collection> collectionEntry : super.getResourceResponse().getCollections().entrySet()) {
            CollectionSqlModel collection = updateCollection(collectionEntry.getValue(), collectionEntry.getKey()); // Update `collections`

            for (Map.Entry<String, ResourceCollectionsResponse.CollectionItem> collectionItemEntry : collectionEntry.getValue().getItems().entrySet()) {
                CollectionItemSqlModel collectionItem = updateCollectionItem(collectionItemEntry.getValue(), collectionItemEntry.getKey(), collection); // Update `collectionitems`

                for (ResourceCollectionsResponse.CollectionTier collectionTier : collectionItemEntry.getValue().getTiers())
                    updateCollectionTier(collectionTier, collectionItem); // Update `collectiontiers`
            }
        }
    }

    @SneakyThrows
    private static CollectionSqlModel updateCollection(ResourceCollectionsResponse.Collection collection, String key) {
        CollectionSqlModel existingCollection = collectionRepository.findFirstOrNullCached(FilterFunction.combine(CollectionSqlModel::getSkill, SkillSqlModel::getKey), key);

        if (existingCollection != null) {
            if (!(equalsWithNull(existingCollection.getSkill().getName(), collection.getName()))) {
                SkillSqlModel skill = skillRepository.findFirstOrNullCached(SkillSqlModel::getKey, collection.getName());
                existingCollection.setSkill(skill);
                collectionRepository.update(existingCollection);
                collectionRepository.refreshItems();
            }

            return existingCollection;
        } else {
            CollectionSqlModel newCollection = new CollectionSqlModel();
            SkillSqlModel skill = skillRepository.findFirstOrNullCached(SkillSqlModel::getKey, key);
            newCollection.setSkill(skill);
            long id = collectionRepository.save(newCollection);
            collectionRepository.refreshItems();
            return collectionRepository.findFirstOrNullCached(CollectionSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static CollectionItemSqlModel updateCollectionItem(ResourceCollectionsResponse.CollectionItem collectionItem, String key, CollectionSqlModel collection) {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemSqlModel existingCollectionItem = collectionItemRepository.findFirstOrNullCached(
                Pair.of(CollectionItemSqlModel::getCollection, collection),
                Pair.of(CollectionItemSqlModel::getItem, key)
        );

        if (existingCollectionItem != null) {
            if (!(equalsWithNull(existingCollectionItem.getName(), collectionItem.getName())
                    && existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers()
            )) {
                existingCollectionItem.setName(collectionItem.getName());
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                collectionItemRepository.update(existingCollectionItem);
                collectionItemRepository.refreshItems();
            }

            return existingCollectionItem;
        } else {
            CollectionItemSqlModel newCollectionItem = new CollectionItemSqlModel();
            ItemSqlModel item = itemRepository.findFirstOrNullCached(ItemSqlModel::getItemId, key);
            newCollectionItem.setItem(item);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setName(collectionItem.getName());
            newCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
            long id = collectionItemRepository.save(newCollectionItem);
            collectionItemRepository.refreshItems();
            return collectionItemRepository.findFirstOrNullCached(CollectionItemSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static void updateCollectionTier(ResourceCollectionsResponse.CollectionTier collectionTier, CollectionItemSqlModel collectionItem) {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemTierSqlModel existingCollectionTier = collectionItemTierRepository.findFirstOrNullCached(
                Pair.of(CollectionItemTierSqlModel::getCollectionItem, collectionItem),
                Pair.of(CollectionItemTierSqlModel::getTier, collectionTier.getTier())
        );

        if (existingCollectionTier != null) {
            if (!(equalsWithNull(existingCollectionTier.getUnlocks(), collectionTier.getUnlocks())
                    && existingCollectionTier.getAmountRequired() == collectionTier.getAmountRequired()
            )) {
                existingCollectionTier.setUnlocks(collectionTier.getUnlocks());
                existingCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
                collectionItemTierRepository.update(existingCollectionTier);
                collectionItemTierRepository.refreshItems();
            }
        } else {
            CollectionItemTierSqlModel newCollectionTier = new CollectionItemTierSqlModel();
            newCollectionTier.setCollectionItem(collectionItem);
            newCollectionTier.setTier(collectionTier.getTier());
            newCollectionTier.setUnlocks(collectionTier.getUnlocks());
            newCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
            collectionItemTierRepository.save(newCollectionTier);
            collectionItemTierRepository.refreshItems();
        }
    }

}
