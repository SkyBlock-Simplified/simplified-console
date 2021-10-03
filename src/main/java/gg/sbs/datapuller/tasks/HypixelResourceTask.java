package gg.sbs.datapuller.tasks;

import gg.sbs.api.SimplifiedApi;
import gg.sbs.api.apiclients.hypixel.implementation.HypixelResourceData;
import gg.sbs.api.apiclients.hypixel.response.ResourceCollectionsResponse;
import gg.sbs.api.apiclients.hypixel.response.ResourceItemsResponse;
import gg.sbs.api.apiclients.hypixel.response.ResourceSkillsResponse;
import gg.sbs.api.data.sql.exception.SqlException;
import gg.sbs.api.data.sql.models.collectionitems.CollectionItemRefreshable;
import gg.sbs.api.data.sql.models.collectionitems.CollectionItemRepository;
import gg.sbs.api.data.sql.models.collectionitemtiers.CollectionItemTierRefreshable;
import gg.sbs.api.data.sql.models.collectionitemtiers.CollectionItemTierRepository;
import gg.sbs.api.data.sql.models.collections.CollectionRefreshable;
import gg.sbs.api.data.sql.models.collections.CollectionRepository;
import gg.sbs.api.data.sql.models.items.ItemRefreshable;
import gg.sbs.api.data.sql.models.items.ItemRepository;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRefreshable;
import gg.sbs.api.data.sql.models.skilllevels.SkillLevelRepository;
import gg.sbs.api.data.sql.models.skills.SkillModel;
import gg.sbs.api.data.sql.models.skills.SkillRefreshable;
import gg.sbs.api.data.sql.models.skills.SkillRepository;

import java.util.Map;

import static gg.sbs.api.util.TimeUtil.ONE_MINUTE_MS;

public class HypixelResourceTask {
    private static final SkillRefreshable skillRefreshable;
    private static final SkillLevelRefreshable skillLevelRefreshable;
    private static final CollectionRefreshable collectionRefreshable;
    private static final CollectionItemRefreshable collectionItemRefreshable;
    private static final CollectionItemTierRefreshable collectionItemTierRefreshable;
    private static final ItemRefreshable itemRefreshable;
    private static final SkillRepository skillRepository;
    private static final SkillLevelRepository skillLevelRepository;
    private static final CollectionRepository collectionRepository;
    private static final CollectionItemRepository collectionItemRepository;
    private static final CollectionItemTierRepository collectionItemTierRepository;
    private static final ItemRepository itemRepository;
    private static final HypixelResourceData hypixelResourceData;

    static {
        skillRefreshable = SimplifiedApi.getSqlRefreshable(SkillRefreshable.class);
        skillLevelRefreshable = SimplifiedApi.getSqlRefreshable(SkillLevelRefreshable.class);
        collectionRefreshable = SimplifiedApi.getSqlRefreshable(CollectionRefreshable.class);
        collectionItemRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemRefreshable.class);
        collectionItemTierRefreshable = SimplifiedApi.getSqlRefreshable(CollectionItemTierRefreshable.class);
        itemRefreshable = SimplifiedApi.getSqlRefreshable(ItemRefreshable.class);
        skillRepository = SimplifiedApi.getSqlRepository(SkillRepository.class);
        skillLevelRepository = SimplifiedApi.getSqlRepository(SkillLevelRepository.class);
        collectionRepository = SimplifiedApi.getSqlRepository(CollectionRepository.class);
        collectionItemRepository = SimplifiedApi.getSqlRepository(CollectionItemRepository.class);
        collectionItemTierRepository = SimplifiedApi.getSqlRepository(CollectionItemTierRepository.class);
        itemRepository = SimplifiedApi.getSqlRepository(ItemRepository.class);
        hypixelResourceData = SimplifiedApi.getWebApi(HypixelResourceData.class);
    }

    public static long getFixedRateMs() {
        return ONE_MINUTE_MS;
    }

    public static void run() {
        ResourceSkillsResponse skills = hypixelResourceData.getSkills();
        ResourceCollectionsResponse collections = hypixelResourceData.getCollections();
        ResourceItemsResponse items = hypixelResourceData.getItems();
        try {
            for (Map.Entry<String, ResourceSkillsResponse.Skill> skillEntry : skills.getSkills().entrySet()) {
                SkillModel existingSkill = skillRefreshable.findFirstOrNull(
                        SkillModel::getSkillKey, skillEntry.getKey()
                );
                if (existingSkill != null) {
                    existingSkill.setName(skillEntry.getValue().getName());
                    existingSkill.setDescription(skillEntry.getValue().getDescription());
                    existingSkill.setMaxLevel(skillEntry.getValue().getMaxLevel());
                    skillRepository.save(existingSkill);
                } else {
                    SkillModel newSkill = new SkillModel();
                    newSkill.setSkillKey(skillEntry.getKey());
                    newSkill.setName(skillEntry.getValue().getName());
                    newSkill.setDescription(skillEntry.getValue().getDescription());
                    newSkill.setMaxLevel(skillEntry.getValue().getMaxLevel());
                    skillRepository.save(newSkill);
                }
            }
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }
}
