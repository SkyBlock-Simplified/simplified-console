package dev.sbs.simplifiedconsole.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.impl.hypixel.response.resource.ResourceCollectionsResponse;
import dev.sbs.api.collection.concurrent.ConcurrentList;
import dev.sbs.api.collection.search.SearchFunction;
import dev.sbs.api.data.model.skyblock.collection_data.collection_item_tiers.CollectionItemTierSqlModel;
import dev.sbs.api.data.model.skyblock.collection_data.collection_items.CollectionItemSqlModel;
import dev.sbs.api.data.model.skyblock.collection_data.collections.CollectionSqlModel;
import dev.sbs.api.data.model.skyblock.items.ItemSqlModel;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlModel;
import dev.sbs.api.data.sql.SqlRepository;
import dev.sbs.api.mutable.pair.Pair;
import dev.sbs.api.util.StringUtil;
import dev.sbs.simplifiedconsole.processor.Processor;

import java.util.Map;

@SuppressWarnings("all")
public class ResourceCollectionsProcessor extends Processor<ResourceCollectionsResponse> {

    // Repositories
    private static final SqlRepository<CollectionSqlModel> collectionRepository = (SqlRepository<CollectionSqlModel>) SimplifiedApi.getRepositoryOf(CollectionSqlModel.class);
    private static final SqlRepository<CollectionItemSqlModel> collectionItemRepository = (SqlRepository<CollectionItemSqlModel>) SimplifiedApi.getRepositoryOf(CollectionItemSqlModel.class);
    private static final SqlRepository<CollectionItemTierSqlModel> collectionItemTierRepository = (SqlRepository<CollectionItemTierSqlModel>) SimplifiedApi.getRepositoryOf(CollectionItemTierSqlModel.class);
    private static final SqlRepository<SkillSqlModel> skillRepository = (SqlRepository<SkillSqlModel>) SimplifiedApi.getRepositoryOf(SkillSqlModel.class);
    private static final SqlRepository<ItemSqlModel> itemRepository = (SqlRepository<ItemSqlModel>) SimplifiedApi.getRepositoryOf(ItemSqlModel.class);

    // Caches
    private static final ConcurrentList<CollectionSqlModel> collectionCache = collectionRepository.findAll();
    private static final ConcurrentList<CollectionItemSqlModel> collectionItemCache = collectionItemRepository.findAll();
    private static final ConcurrentList<CollectionItemTierSqlModel> collectionItemTierCache = collectionItemTierRepository.findAll();
    private static final ConcurrentList<SkillSqlModel> skillCache = skillRepository.findAll();
    private static final ConcurrentList<ItemSqlModel> itemCache = itemRepository.findAll();

    public ResourceCollectionsProcessor(ResourceCollectionsResponse resourceResponse) {
        super(resourceResponse);
    }

    @Override
    public void process() {
        for (Map.Entry<String, ResourceCollectionsResponse.Collection> collectionEntry : super.getResourceResponse().getCollections().entrySet()) {
            this.getLog().info("Processing Collection {}", collectionEntry.getKey());
            CollectionSqlModel collection = this.updateCollection(collectionEntry.getValue(), collectionEntry.getKey()); // Update `collections`

            for (Map.Entry<String, ResourceCollectionsResponse.CollectionItem> collectionItemEntry : collectionEntry.getValue().getItems().entrySet()) {
                CollectionItemSqlModel collectionItem = this.updateCollectionItem(collectionItemEntry.getValue(), collectionItemEntry.getKey(), collection); // Update `collectionitems`

                for (ResourceCollectionsResponse.CollectionTier collectionTier : collectionItemEntry.getValue().getTiers())
                    this.updateCollectionTier(collectionTier, collectionItem); // Update `collectiontiers`
            }
        }
    }

    private CollectionSqlModel updateCollection(ResourceCollectionsResponse.Collection collection, String key) {
        CollectionSqlModel existingCollection = collectionCache.findFirstOrNull(CollectionSqlModel::getKey, key);

        if (existingCollection == null) {
            CollectionSqlModel newCollection = new CollectionSqlModel();
            SkillSqlModel skill = skillCache.findFirstOrNull(SkillSqlModel::getKey, key);
            newCollection.setKey(key);
            newCollection.setName(StringUtil.capitalizeFully(key.replace("_", " ")));
            this.getLog().info("Adding new collection {}", key);
            collectionCache.add(newCollection.save());
            return newCollection;
        }

        return existingCollection;
    }

    private CollectionItemSqlModel updateCollectionItem(ResourceCollectionsResponse.CollectionItem collectionItem, String key, CollectionSqlModel collection) {
        CollectionItemSqlModel existingCollectionItem = collectionItemRepository.findFirstOrNull(
                Pair.of(CollectionItemSqlModel::getCollection, collection),
                Pair.of(SearchFunction.combine(CollectionItemSqlModel::getItem, ItemSqlModel::getItemId), key)
        );

        if (existingCollectionItem != null) {
            if (!(existingCollectionItem.getMaxTiers() == collectionItem.getMaxTiers())) {
                existingCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
                this.getLog().info("Updating existing collection item {} in {}", existingCollectionItem.getItem().getItemId(), existingCollectionItem.getCollection().getKey());
                existingCollectionItem.update();
            }

            return existingCollectionItem;
        } else {
            CollectionItemSqlModel newCollectionItem = new CollectionItemSqlModel();
            ItemSqlModel item = itemCache.findFirstOrNull(ItemSqlModel::getItemId, key);
            newCollectionItem.setCollection(collection);
            newCollectionItem.setItem(item);
            newCollectionItem.setMaxTiers(collectionItem.getMaxTiers());
            this.getLog().info("Adding new collection item {} in {}", newCollectionItem.getItem().getItemId(), newCollectionItem.getCollection().getKey());
            collectionItemCache.add(newCollectionItem.save());
            return newCollectionItem;
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
                this.getLog().info("Updating existing collection tier {} in {}", existingCollectionTier.getTier(), existingCollectionTier.getCollectionItem().getItem().getItemId());
                existingCollectionTier.update();
            }
        } else {
            CollectionItemTierSqlModel newCollectionTier = new CollectionItemTierSqlModel();
            newCollectionTier.setCollectionItem(collectionItem);
            newCollectionTier.setTier(collectionTier.getTier());
            newCollectionTier.setUnlocks(collectionTier.getUnlocks());
            newCollectionTier.setAmountRequired(collectionTier.getAmountRequired());
            this.getLog().info("Adding new collection tier {} in {}", newCollectionTier.getTier(), newCollectionTier.getCollectionItem().getItem().getItemId());
            collectionItemTierCache.add(newCollectionTier.save());
        }
    }

}
