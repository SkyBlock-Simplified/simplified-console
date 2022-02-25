package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.data.model.skyblock.collection_item_tiers.CollectionItemTierSqlModel;
import dev.sbs.api.data.model.skyblock.collection_item_tiers.CollectionItemTierSqlRepository;
import dev.sbs.api.data.model.skyblock.collection_items.CollectionItemSqlModel;
import dev.sbs.api.data.model.skyblock.collection_items.CollectionItemSqlRepository;
import dev.sbs.api.data.model.skyblock.collections.CollectionSqlModel;
import dev.sbs.api.data.model.skyblock.collections.CollectionSqlRepository;
import dev.sbs.api.data.model.skyblock.items.ItemSqlModel;
import dev.sbs.api.data.model.skyblock.items.ItemSqlRepository;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlModel;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlRepository;
import dev.sbs.api.util.collection.search.function.SearchFunction;
import dev.sbs.api.util.data.tuple.Pair;
import dev.sbs.updater.processor.Processor;

import java.util.Map;

@SuppressWarnings("all")
public class ResourceCollectionsProcessor extends Processor<ResourceCollectionsResponse> {

    private static final CollectionSqlRepository collectionRepository = (CollectionSqlRepository) SimplifiedApi.getRepositoryOf(CollectionSqlModel.class);
    private static final CollectionItemSqlRepository collectionItemRepository = (CollectionItemSqlRepository) SimplifiedApi.getRepositoryOf(CollectionItemSqlModel.class);
    private static final CollectionItemTierSqlRepository collectionItemTierRepository = (CollectionItemTierSqlRepository) SimplifiedApi.getRepositoryOf(CollectionItemTierSqlModel.class);
    private static final SkillSqlRepository skillRepository = (SkillSqlRepository) SimplifiedApi.getRepositoryOf(SkillSqlModel.class);
    private static final ItemSqlRepository itemRepository = (ItemSqlRepository) SimplifiedApi.getRepositoryOf(ItemSqlModel.class);

    public ResourceCollectionsProcessor(ResourceCollectionsResponse resourceResponse) {
        super(resourceResponse);
    }

    @Override
    public void process() {
        for (Map.Entry<String, ResourceCollectionsResponse.Collection> collectionEntry : super.getResourceResponse().getCollections().entrySet()) {
            this.getLog().info("Processing {0}", collectionEntry.getKey());
            CollectionSqlModel collection = this.updateCollection(collectionEntry.getValue(), collectionEntry.getKey()); // Update `collections`

            for (Map.Entry<String, ResourceCollectionsResponse.CollectionItem> collectionItemEntry : collectionEntry.getValue().getItems().entrySet()) {
                CollectionItemSqlModel collectionItem = this.updateCollectionItem(collectionItemEntry.getValue(), collectionItemEntry.getKey(), collection); // Update `collectionitems`

                for (ResourceCollectionsResponse.CollectionTier collectionTier : collectionItemEntry.getValue().getTiers())
                    this.updateCollectionTier(collectionTier, collectionItem); // Update `collectiontiers`
            }
        }
    }

    private CollectionSqlModel updateCollection(ResourceCollectionsResponse.Collection collection, String key) {
        CollectionSqlModel existingCollection = collectionRepository.findFirstOrNull(SearchFunction.combine(CollectionSqlModel::getSkill, SkillSqlModel::getKey), key);

        if (existingCollection != null) {
            if (!(equalsWithNull(existingCollection.getSkill().getName(), collection.getName()))) {
                SkillSqlModel skill = skillRepository.findFirstOrNull(SkillSqlModel::getKey, collection.getName());
                existingCollection.setSkill(skill);
                this.getLog().info("Updating existing collection {0}", existingCollection.getSkill().getName());
                existingCollection.update();
            }

            return existingCollection;
        } else {
            CollectionSqlModel newCollection = new CollectionSqlModel();
            SkillSqlModel skill = skillRepository.findFirstOrNull(SkillSqlModel::getKey, key);
            newCollection.setSkill(skill);
            this.getLog().info("Adding new collection {0}", newCollection.getSkill().getKey());
            return newCollection.save();
        }
    }

    private CollectionItemSqlModel updateCollectionItem(ResourceCollectionsResponse.CollectionItem collectionItem, String key, CollectionSqlModel collection) {
        CollectionItemSqlModel existingCollectionItem = collectionItemRepository.findFirstOrNull(
                Pair.of(CollectionItemSqlModel::getCollection, collection),
                Pair.of(SearchFunction.combine(CollectionItemSqlModel::getItem, ItemSqlModel::getItemId), key)
        );

        if (existingCollectionItem != null) {
            if (!(existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers())) {
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                this.getLog().info("Updating existing collection item {0} in {1}", existingCollectionItem.getItem().getItemId(), existingCollectionItem.getCollection().getSkill().getKey());
                existingCollectionItem.update();
            }

            return existingCollectionItem;
        } else {
            CollectionItemSqlModel newCollectionItem = new CollectionItemSqlModel();
            ItemSqlModel item = itemRepository.findFirstOrNull(ItemSqlModel::getItemId, key);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setItem(item);
            newCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
            this.getLog().info("Adding new collection item {0} in {1}", newCollectionItem.getItem().getItemId(), newCollectionItem.getCollection().getSkill().getKey());
            return newCollectionItem.save();
        }
    }

    private void updateCollectionTier(ResourceCollectionsResponse.CollectionTier collectionTier, CollectionItemSqlModel collectionItem) {
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
                this.getLog().info("Updating existing collection tier {0} in {1}", existingCollectionTier.getTier(), existingCollectionTier.getCollectionItem().getItem().getItemId());
                existingCollectionTier.update();
            }
        } else {
            CollectionItemTierSqlModel newCollectionTier = new CollectionItemTierSqlModel();
            newCollectionTier.setCollectionItem(collectionItem);
            newCollectionTier.setTier(collectionTier.getTier());
            newCollectionTier.setUnlocks(collectionTier.getUnlocks());
            newCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
            this.getLog().info("Adding new collection tier {0} in {1}", newCollectionTier.getTier(), newCollectionTier.getCollectionItem().getItem().getItemId());
            newCollectionTier.save();
        }
    }

}
