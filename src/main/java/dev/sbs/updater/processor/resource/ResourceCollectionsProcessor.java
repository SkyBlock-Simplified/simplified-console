package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.data.sql.function.FilterFunction;
import dev.sbs.api.data.model.collections.CollectionSqlRepository;
import dev.sbs.api.data.model.collections.CollectionSqlModel;
import dev.sbs.api.data.model.collection_items.CollectionItemSqlRepository;
import dev.sbs.api.data.model.collection_items.CollectionItemSqlModel;
import dev.sbs.api.data.model.collection_item_tiers.CollectionItemTierSqlRepository;
import dev.sbs.api.data.model.collection_item_tiers.CollectionItemTierSqlModel;
import dev.sbs.api.data.model.items.ItemSqlRepository;
import dev.sbs.api.data.model.items.ItemSqlModel;
import dev.sbs.api.data.model.skills.SkillSqlRepository;
import dev.sbs.api.data.model.skills.SkillSqlModel;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;
import lombok.SneakyThrows;

import java.util.Map;

public class ResourceCollectionsProcessor extends Processor<ResourceCollectionsResponse> {

    private static final CollectionSqlRepository collectionRepository = SimplifiedApi.getSqlRepository(CollectionSqlRepository.class);
    private static final CollectionItemSqlRepository collectionItemRepository = SimplifiedApi.getSqlRepository(CollectionItemSqlRepository.class);
    private static final CollectionItemTierSqlRepository collectionItemTierRepository = SimplifiedApi.getSqlRepository(CollectionItemTierSqlRepository.class);
    private static final SkillSqlRepository skillRepository = SimplifiedApi.getSqlRepository(SkillSqlRepository.class);
    private static final ItemSqlRepository itemRepository = SimplifiedApi.getSqlRepository(ItemSqlRepository.class);

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
                Pair.of(FilterFunction.combine(CollectionItemSqlModel::getItem, ItemSqlModel::getItemId), key)
        );

        if (existingCollectionItem != null) {
            if (!(existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers())) {
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                collectionItemRepository.update(existingCollectionItem);
                collectionItemRepository.refreshItems();
            }

            return existingCollectionItem;
        } else {
            CollectionItemSqlModel newCollectionItem = new CollectionItemSqlModel();
            ItemSqlModel item = itemRepository.findFirstOrNullCached(ItemSqlModel::getItemId, key);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setItem(item);
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
