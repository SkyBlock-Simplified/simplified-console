package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.data.Repository;
import dev.sbs.api.data.model.collection_item_tiers.CollectionItemTierSqlModel;
import dev.sbs.api.data.model.collection_item_tiers.CollectionItemTierSqlRepository;
import dev.sbs.api.data.model.collection_items.CollectionItemSqlModel;
import dev.sbs.api.data.model.collection_items.CollectionItemSqlRepository;
import dev.sbs.api.data.model.collections.CollectionSqlModel;
import dev.sbs.api.data.model.collections.CollectionSqlRepository;
import dev.sbs.api.data.model.items.ItemSqlModel;
import dev.sbs.api.data.model.skills.SkillSqlModel;
import dev.sbs.api.data.sql.function.FilterFunction;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;
import lombok.SneakyThrows;

import java.util.Map;

public class ResourceCollectionsProcessor extends Processor<ResourceCollectionsResponse> {

    private static final Repository<CollectionSqlModel> collectionRepository = SimplifiedApi.getRepositoryOf(CollectionSqlModel.class);
    private static final Repository<CollectionItemSqlModel> collectionItemRepository = SimplifiedApi.getRepositoryOf(CollectionItemSqlModel.class);
    private static final Repository<CollectionItemTierSqlModel> collectionItemTierRepository = SimplifiedApi.getRepositoryOf(CollectionItemTierSqlModel.class);
    private static final Repository<SkillSqlModel> skillRepository = SimplifiedApi.getRepositoryOf(SkillSqlModel.class);
    private static final Repository<ItemSqlModel> itemRepository = SimplifiedApi.getRepositoryOf(ItemSqlModel.class);

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
        CollectionSqlModel existingCollection = collectionRepository.findFirstOrNull(FilterFunction.combine(CollectionSqlModel::getSkill, SkillSqlModel::getKey), key);

        if (existingCollection != null) {
            if (!(equalsWithNull(existingCollection.getSkill().getName(), collection.getName()))) {
                SkillSqlModel skill = skillRepository.findFirstOrNull(SkillSqlModel::getKey, collection.getName());
                existingCollection.setSkill(skill);
                ((CollectionSqlRepository) collectionRepository).update(existingCollection);
            }

            return existingCollection;
        } else {
            CollectionSqlModel newCollection = new CollectionSqlModel();
            SkillSqlModel skill = skillRepository.findFirstOrNull(SkillSqlModel::getKey, key);
            newCollection.setSkill(skill);
            long id = ((CollectionSqlRepository) collectionRepository).save(newCollection);
            return collectionRepository.findFirstOrNull(CollectionSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static CollectionItemSqlModel updateCollectionItem(ResourceCollectionsResponse.CollectionItem collectionItem, String key, CollectionSqlModel collection) {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemSqlModel existingCollectionItem = collectionItemRepository.findFirstOrNull(
                Pair.of(CollectionItemSqlModel::getCollection, collection),
                Pair.of(FilterFunction.combine(CollectionItemSqlModel::getItem, ItemSqlModel::getItemId), key)
        );

        if (existingCollectionItem != null) {
            if (!(existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers())) {
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                ((CollectionItemSqlRepository) collectionItemRepository).update(existingCollectionItem);
            }

            return existingCollectionItem;
        } else {
            CollectionItemSqlModel newCollectionItem = new CollectionItemSqlModel();
            ItemSqlModel item = itemRepository.findFirstOrNull(ItemSqlModel::getItemId, key);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setItem(item);
            newCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
            long id = ((CollectionItemSqlRepository) collectionItemRepository).save(newCollectionItem);
            return collectionItemRepository.findFirstOrNull(CollectionItemSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static void updateCollectionTier(ResourceCollectionsResponse.CollectionTier collectionTier, CollectionItemSqlModel collectionItem) {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        CollectionItemTierSqlModel existingCollectionTier = collectionItemTierRepository.findFirstOrNull(
                Pair.of(CollectionItemTierSqlModel::getCollectionItem, collectionItem),
                Pair.of(CollectionItemTierSqlModel::getTier, collectionTier.getTier())
        );

        if (existingCollectionTier != null) {
            if (!(equalsWithNull(existingCollectionTier.getUnlocks(), collectionTier.getUnlocks())
                    && existingCollectionTier.getAmountRequired() == collectionTier.getAmountRequired()
            )) {
                existingCollectionTier.setUnlocks(collectionTier.getUnlocks());
                existingCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
                ((CollectionItemTierSqlRepository) collectionItemTierRepository).update(existingCollectionTier);
            }
        } else {
            CollectionItemTierSqlModel newCollectionTier = new CollectionItemTierSqlModel();
            newCollectionTier.setCollectionItem(collectionItem);
            newCollectionTier.setTier(collectionTier.getTier());
            newCollectionTier.setUnlocks(collectionTier.getUnlocks());
            newCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
            ((CollectionItemTierSqlRepository) collectionItemTierRepository).save(newCollectionTier);
        }
    }

}
