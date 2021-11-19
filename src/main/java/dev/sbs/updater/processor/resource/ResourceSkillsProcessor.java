package dev.sbs.updater.processor.resource;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.hypixel.response.resource.ResourceSkillsResponse;
import dev.sbs.api.data.model.skyblock.skill_levels.SkillLevelSqlModel;
import dev.sbs.api.data.model.skyblock.skill_levels.SkillLevelSqlRepository;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlModel;
import dev.sbs.api.data.model.skyblock.skills.SkillSqlRepository;
import dev.sbs.api.util.tuple.Pair;
import dev.sbs.updater.processor.Processor;
import lombok.SneakyThrows;

import java.util.Map;

public class ResourceSkillsProcessor extends Processor<ResourceSkillsResponse> {

    private static final SkillSqlRepository skillRepository = (SkillSqlRepository) SimplifiedApi.getRepositoryOf(SkillSqlModel.class);
    private static final SkillLevelSqlRepository skillLevelRepository = (SkillLevelSqlRepository) SimplifiedApi.getRepositoryOf(SkillLevelSqlModel.class);

    public ResourceSkillsProcessor(ResourceSkillsResponse resourceResponse) {
        super(resourceResponse);
    }

    @Override
    public void process() {
        for (Map.Entry<String, ResourceSkillsResponse.Skill> skillEntry : super.getResourceResponse().getSkills().entrySet()) {
            SkillSqlModel skill = updateSkill(skillEntry.getValue(), skillEntry.getKey()); // Update `skills`

            for (ResourceSkillsResponse.SkillLevel skillLevel : skillEntry.getValue().getLevels())
                updateSkillLevel(skillLevel, skill); // Update `skilllevels`
        }
    }

    @SneakyThrows
    private static SkillSqlModel updateSkill(ResourceSkillsResponse.Skill skill, String key) {
        SkillSqlModel existingSkill = skillRepository.findFirstOrNull(SkillSqlModel::getKey, key);

        if (existingSkill != null) {
            if (!(equalsWithNull(existingSkill.getName(), skill.getName())
                    && equalsWithNull(existingSkill.getDescription(), skill.getDescription())
                    && existingSkill.getMaxLevel() == skill.getMaxLevel()
            )) {
                existingSkill.setName(skill.getName());
                existingSkill.setDescription(skill.getDescription());
                existingSkill.setMaxLevel(skill.getMaxLevel());
                skillRepository.update(existingSkill);
            }

            return existingSkill;
        } else {
            SkillSqlModel newSkill = new SkillSqlModel();
            newSkill.setKey(key);
            newSkill.setName(skill.getName());
            newSkill.setDescription(skill.getDescription());
            newSkill.setMaxLevel(skill.getMaxLevel());
            long id = skillRepository.save(newSkill);
            return skillRepository.findFirstOrNull(SkillSqlModel::getId, id);
        }
    }

    @SneakyThrows
    private static void updateSkillLevel(ResourceSkillsResponse.SkillLevel skillLevel, SkillSqlModel skill) {
        @SuppressWarnings({"unchecked"}) // Doesn't matter because findFirstOrNull uses generics
        SkillLevelSqlModel existingSkillLevel = skillLevelRepository.findFirstOrNull(
                Pair.of(SkillLevelSqlModel::getSkill, skill),
                Pair.of(SkillLevelSqlModel::getLevel, skillLevel.getLevel())
        );
        if (existingSkillLevel != null) {
            if (!(equalsWithNull(existingSkillLevel.getUnlocks(), skillLevel.getUnlocks())
                    && existingSkillLevel.getTotalExpRequired() == skillLevel.getTotalExpRequired()
            )) {
                existingSkillLevel.setUnlocks(skillLevel.getUnlocks());
                existingSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
                skillLevelRepository.update(existingSkillLevel);
            }
        } else {
            SkillLevelSqlModel newSkillLevel = new SkillLevelSqlModel();
            newSkillLevel.setSkill(skill);
            newSkillLevel.setLevel(skillLevel.getLevel());
            newSkillLevel.setUnlocks(skillLevel.getUnlocks());
            newSkillLevel.setTotalExpRequired(skillLevel.getTotalExpRequired());
            skillLevelRepository.save(newSkillLevel);
        }
    }

}
