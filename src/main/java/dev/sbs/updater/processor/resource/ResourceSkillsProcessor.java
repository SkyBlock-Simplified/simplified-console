package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.impl.hypixel.response.resource.ResourceSkillsResponse;
import dev.sbs.api.collection.concurrent.ConcurrentList;
import dev.sbs.api.data.model.skyblock.skill_levels.SkillLevelSqlModel;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlModel;
import dev.sbs.api.data.sql.SqlRepository;
import dev.sbs.api.mutable.pair.Pair;
import dev.sbs.updater.processor.Processor;

import java.util.Map;

@SuppressWarnings("all")
public class ResourceSkillsProcessor extends Processor<ResourceSkillsResponse> {

    // Repositories
    private static final SqlRepository<SkillSqlModel> skillRepository = (SqlRepository<SkillSqlModel>) SimplifiedApi.getRepositoryOf(SkillSqlModel.class);
    private static final SqlRepository<SkillLevelSqlModel> skillLevelRepository = (SqlRepository<SkillLevelSqlModel>) SimplifiedApi.getRepositoryOf(SkillLevelSqlModel.class);

    // Caches
    private static ConcurrentList<SkillSqlModel> skillCache = skillRepository.findAll();
    private static ConcurrentList<SkillLevelSqlModel> skillLevelCache = skillLevelRepository.findAll();

    public ResourceSkillsProcessor(ResourceSkillsResponse resourceResponse) {
        super(resourceResponse);
    }

    @Override
    public void process() {
        for (Map.Entry<String, ResourceSkillsResponse.Skill> skillEntry : super.getResourceResponse().getSkills().entrySet()) {
            this.getLog().info("Processing {} Skill", skillEntry.getKey());
            SkillSqlModel skill = this.updateSkill(skillEntry.getValue(), skillEntry.getKey()); // Update `skills`

            for (ResourceSkillsResponse.SkillLevel skillLevel : skillEntry.getValue().getLevels()) {
                this.getLog().info("Processing {} Skill: Level {}", skillEntry.getKey(), skillLevel.getLevel());
                this.updateSkillLevel(skillLevel, skill); // Update `skilllevels`
            }
        }
    }

    private SkillSqlModel updateSkill(ResourceSkillsResponse.Skill skill, String key) {
        SkillSqlModel existingSkill = skillCache.findFirstOrNull(SkillSqlModel::getKey, key);

        if (existingSkill != null) {
            if (!equalsWithNull(existingSkill.getName(), skill.getName())
                    || !equalsWithNull(existingSkill.getDescription(), skill.getDescription())
                    || existingSkill.getMaxLevel() != skill.getMaxLevel()
            ) {
                existingSkill.setName(skill.getName());
                existingSkill.setDescription(skill.getDescription());
                existingSkill.setMaxLevel(skill.getMaxLevel());
                this.getLog().info("Updating existing skill {}", existingSkill.getKey());
                existingSkill.update();
            }

            return existingSkill;
        } else {
            SkillSqlModel newSkill = new SkillSqlModel();
            newSkill.setKey(key);
            newSkill.setName(skill.getName());
            newSkill.setDescription(skill.getDescription());
            newSkill.setMaxLevel(skill.getMaxLevel());
            this.getLog().info("Adding new skill {}", newSkill.getKey());
            skillCache.add(newSkill.save());
            return newSkill;
        }
    }

    private SkillLevelSqlModel updateSkillLevel(ResourceSkillsResponse.SkillLevel skillLevel, SkillSqlModel skill) {
        SkillLevelSqlModel existingSkillLevel = skillLevelCache.findFirstOrNull(
                Pair.of(SkillLevelSqlModel::getSkill, skill),
                Pair.of(SkillLevelSqlModel::getLevel, skillLevel.getLevel())
        );

        if (existingSkillLevel != null) {
            if (!equalsWithNull(existingSkillLevel.getUnlocks(), skillLevel.getUnlocks()) || existingSkillLevel.getTotalExpRequired() != skillLevel.getTotalExpRequired()) {
                existingSkillLevel.setUnlocks(skillLevel.getUnlocks());
                existingSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
                this.getLog().info("Updating existing skill level {} for {}", existingSkillLevel.getLevel(), existingSkillLevel.getSkill().getKey());
                existingSkillLevel.update();
            }

            return existingSkillLevel;
        } else {
            SkillLevelSqlModel newSkillLevel = new SkillLevelSqlModel();
            newSkillLevel.setSkill(skill);
            newSkillLevel.setLevel(skillLevel.getLevel());
            newSkillLevel.setUnlocks(skillLevel.getUnlocks());
            newSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
            this.getLog().info("Adding new skill level {} for {}", newSkillLevel.getLevel(), newSkillLevel.getSkill().getKey());
            skillLevelCache.add(newSkillLevel.save());
            return newSkillLevel;
        }
    }

}
