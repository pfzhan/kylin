/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.rest.service;

import static io.kyligence.kap.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS;
import static io.kyligence.kap.metadata.favorite.FavoriteRule.FAVORITE_RULE_NAMES;
import static io.kyligence.kap.metadata.favorite.FavoriteRule.MIN_HIT_COUNT;
import static io.kyligence.kap.metadata.favorite.FavoriteRule.UPDATE_FREQUENCY;
import static org.apache.kylin.common.exception.ServerErrorCode.ONGOING_OPTIMIZATION;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.response.ProjectStatisticsResponse;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.rest.service.task.RecommendationTopNUpdateScheduler;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("projectSmartService")
public class ProjectSmartService extends BasicService implements ProjectSmartServiceSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ProjectSmartSupporter projectSmartSupporter;

    @Autowired
    @Qualifier("recommendationUpdateScheduler")
    RecommendationTopNUpdateScheduler recommendationTopNUpdateScheduler;

    @Autowired
    private ProjectModelSupporter projectModelSupporter;

    private void updateSingleRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
        boolean isEnabled = false;
        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            isEnabled = request.isFreqEnable();
            conds.add(new FavoriteRule.Condition(null, request.getFreqValue()));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            isEnabled = request.isCountEnable();
            conds.add(new FavoriteRule.Condition(null, request.getCountValue()));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUsers()))
                request.getUsers().forEach(user -> conds.add(new FavoriteRule.Condition(null, user)));
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUserGroups()))
                request.getUserGroups().forEach(userGroup -> conds.add(new FavoriteRule.Condition(null, userGroup)));
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            isEnabled = request.isDurationEnable();
            conds.add(new FavoriteRule.Condition(request.getMinDuration(), request.getMaxDuration()));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            isEnabled = request.isRecommendationEnable();
            conds.add(new FavoriteRule.Condition(null, request.getRecommendationsValue()));
            break;
        case FavoriteRule.EXCLUDED_TABLES_RULE:
            isEnabled = request.isExcludeTablesEnable();
            conds.add(new FavoriteRule.Condition(null,
                    request.getExcludedTables() == null ? "" : request.getExcludedTables()));
            break;
        case EFFECTIVE_DAYS:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getEffectiveDays()));
            break;
        case UPDATE_FREQUENCY:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getUpdateFrequency()));
            break;
        case MIN_HIT_COUNT:
            isEnabled = true;
            conds.add(new FavoriteRule.Condition(null, request.getMinHitCount()));
            break;
        default:
            break;
        }
        boolean updateFrequencyChange = isChangeFreqRule(project, ruleName, request);
        getManager(FavoriteRuleManager.class, project).updateRule(conds, isEnabled, ruleName);
        if (updateFrequencyChange) {
            recommendationTopNUpdateScheduler.reScheduleProject(project);
        }
    }

    private boolean isChangeFreqRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        if (!UPDATE_FREQUENCY.equals(ruleName)) {
            return false;
        }
        String currentVal = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getValue(UPDATE_FREQUENCY);
        return !currentVal.equals(request.getUpdateFrequency());
    }

    @Transaction(project = 0)
    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        FAVORITE_RULE_NAMES.forEach(ruleName -> updateSingleRule(project, ruleName, request));

        NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listAllModels().forEach(model ->
                projectModelSupporter.onModelUpdate(project, model.getUuid()));
    }

    public ProjectStatisticsResponse getProjectStatistics(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);

        ProjectStatisticsResponse response = new ProjectStatisticsResponse();
        int[] datasourceStatistics = getDatasourceStatistics(project);
        response.setDatabaseSize(datasourceStatistics[0]);
        response.setTableSize(datasourceStatistics[1]);

        int[] recPatternCount = getRecPatternCount(project);
        response.setAdditionalRecPatternCount(recPatternCount[0]);
        response.setRemovalRecPatternCount(recPatternCount[1]);
        response.setRecPatternCount(recPatternCount[2]);

        response.setEffectiveRuleSize(getFavoriteRuleSize(project));

        int[] approvedRecsCount = getApprovedRecsCount(project);
        response.setApprovedAdditionalRecCount(approvedRecsCount[0]);
        response.setApprovedRemovalRecCount(approvedRecsCount[1]);
        response.setApprovedRecCount(approvedRecsCount[2]);

        Map<String, Set<Integer>> modelToRecMap = getModelToRecMap(project);
        response.setModelSize(modelToRecMap.size());
        if (getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            Set<Integer> allRecSet = Sets.newHashSet();
            modelToRecMap.values().forEach(allRecSet::addAll);
            response.setAcceptableRecSize(allRecSet.size());
            response.setMaxRecShowSize(getRecommendationSizeToShow(project));
        } else {
            response.setAcceptableRecSize(-1);
            response.setMaxRecShowSize(-1);
        }

        AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) AsyncTaskManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> userRefreshTag = asyncAcceleration.getUserRefreshedTagMap();
        response.setRefreshed(userRefreshTag.getOrDefault(aclEvaluate.getCurrentUserName(), false));

        return response;
    }

    public Set<Integer> accelerateManually(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        Map<String, Set<Integer>> modelToRecMap = getModelToRecMap(project);
        if (getAsyncAccTask(project).isAlreadyRunning()) {
            throw new KylinException(ONGOING_OPTIMIZATION, MsgPicker.getMsg().getProjectOngoingOptimization());
        }

        QueryHistoryTaskScheduler scheduler = QueryHistoryTaskScheduler.getInstance(project);
        if (scheduler.hasStarted()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                AsyncAccelerationTask accTask = getAsyncAccTask(project);
                accTask.setAlreadyRunning(true);
                accTask.getUserRefreshedTagMap().put(aclEvaluate.getCurrentUserName(), false);
                AsyncTaskManager.getInstance(KylinConfig.getInstanceFromEnv(), project).save(accTask);
                return null;
            }, project);

            val accelerateRunner = scheduler.new QueryHistoryAccelerateRunner(true);
            try {
                Future<?> future = scheduler.scheduleImmediately(accelerateRunner);
                future.get();
                if (projectSmartSupporter != null) {
                    projectSmartSupporter.onUpdateCost(project);
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                log.error("Accelerate failed", e);
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    AsyncAccelerationTask accTask = getAsyncAccTask(project);
                    accTask.setAlreadyRunning(false);
                    AsyncTaskManager.getInstance(KylinConfig.getInstanceFromEnv(), project).save(accTask);
                    return null;
                }, project);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        Map<String, Set<Integer>> deltaRecsMap = getDeltaRecs(modelToRecMap, project);
        Set<Integer> deltaRecSet = Sets.newHashSet();
        deltaRecsMap.forEach((k, deltaRecs) -> deltaRecSet.addAll(deltaRecs));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask accTask = getAsyncAccTask(project);
            accTask.setAlreadyRunning(false);
            accTask.getUserRefreshedTagMap().put(aclEvaluate.getCurrentUserName(), !deltaRecSet.isEmpty());
            AsyncTaskManager.getInstance(KylinConfig.getInstanceFromEnv(), project).save(accTask);
            return null;
        }, project);
        return deltaRecSet;
    }

    @Override
    public void cleanupGarbage(String project) throws Exception {
        accelerateImmediately(project);
        updateStatMetaImmediately(project);
    }

    public void accelerateImmediately(String project) {
        QueryHistoryTaskScheduler scheduler = QueryHistoryTaskScheduler.getInstance(project);
        if (scheduler.hasStarted()) {
            log.info("Schedule QueryHistoryAccelerateRunner job, project [{}].", project);
            Future<?> future = scheduler.scheduleImmediately(scheduler.new QueryHistoryAccelerateRunner(false));
            try {
                future.get();
            } catch (InterruptedException e) {
                log.error("Accelerate failed with interruption", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Accelerate failed", e);
            }
        }
    }

    public void updateStatMetaImmediately(String project) {
        QueryHistoryTaskScheduler scheduler = QueryHistoryTaskScheduler.getInstance(project);
        if (scheduler.hasStarted()) {
            Future<?> future = scheduler.scheduleImmediately(scheduler.new QueryHistoryMetaUpdateRunner());
            try {
                future.get();
            } catch (InterruptedException e) {
                log.error("updateStatMeta failed with interruption", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("updateStatMeta failed", e);
            }
        }
    }

    private int getFavoriteRuleSize(String project) {
        if (!getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            return -1;
        }

        return (int) getManager(FavoriteRuleManager.class, project).listAll().stream().filter(FavoriteRule::isEnabled).count();
    }

    private int[] getRecPatternCount(String project) {
        if (!getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            return new int[] { -1, -1, -1 };
        }
        int[] array = new int[3];
        RawRecManager recManager = RawRecManager.getInstance(project);
        Map<RawRecItem.RawRecType, Integer> recPatternCountMap = recManager.getCandidatesByProject(project);
        array[0] = recPatternCountMap.get(RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        array[1] = recPatternCountMap.get(RawRecItem.RawRecType.REMOVAL_LAYOUT);
        array[2] = array[0] + array[1];
        return array;
    }

    private int[] getDatasourceStatistics(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        int[] arr = new int[2];
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<String> databaseSet = Sets.newHashSet();
        List<TableDesc> tables = tblMgr.listAllTables().stream()
                .filter(NTableMetadataManager::isTableAccessible).map(tableDesc -> {
                    databaseSet.add(tableDesc.getDatabase());
                    return tableDesc;
                }).collect(Collectors.toList());
        arr[0] = databaseSet.size();
        arr[1] = tables.size();
        return arr;
    }

    private int[] getApprovedRecsCount(String project) {
        ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);
        if (!projectInstance.isSemiAutoMode()) {
            return new int[] { -1, -1, -1 };
        }

        int[] allApprovedRecs = new int[3];
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        for (IndexPlan indexPlan : indexPlanManager.listAllIndexPlans()) {
            if (!indexPlan.isBroken()) {
                allApprovedRecs[0] += indexPlan.getApprovedAdditionalRecs();
                allApprovedRecs[1] += indexPlan.getApprovedRemovalRecs();
            }
        }
        allApprovedRecs[2] = allApprovedRecs[0] + allApprovedRecs[1];
        return allApprovedRecs;
    }

    private int getRecommendationSizeToShow(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        return projectSmartSupporter != null ? projectSmartSupporter.onShowSize(project) : 0;
    }

    private AsyncAccelerationTask getAsyncAccTask(String project) {
        return (AsyncAccelerationTask) AsyncTaskManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
    }

    private Map<String, Set<Integer>> getDeltaRecs(Map<String, Set<Integer>> modelToRecMap, String project) {
        Map<String, Set<Integer>> updatedModelToRecMap = getModelToRecMap(project);
        modelToRecMap.forEach((modelId, recSet) -> {
            if (updatedModelToRecMap.containsKey(modelId)) {
                updatedModelToRecMap.get(modelId).removeAll(recSet);
            }
        });
        updatedModelToRecMap.entrySet().removeIf(pair -> pair.getValue().isEmpty());
        return updatedModelToRecMap;
    }

    public Map<String, Set<Integer>> getModelToRecMap(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);

        List<NDataModel> dataModels = getManager(NDataModelManager.class, project).listAllModels().stream()
                .filter(model -> model.isBroken() || !model.fusionModelBatchPart())
                .filter(NDataModelManager::isModelAccessible).collect(Collectors.toList());
        Map<String, Set<Integer>> map = Maps.newHashMap();
        dataModels.forEach(model -> map.putIfAbsent(model.getId(), Sets.newHashSet()));
        if (getManager(NProjectManager.class).getProject(project).isSemiAutoMode()) {
            OptRecManagerV2 optRecManager = OptRecManagerV2.getInstance(project);
            for (NDataModel model : dataModels) {
                OptRecV2 optRecV2 = optRecManager.loadOptRecV2(model.getUuid());
                map.get(model.getId()).addAll(optRecV2.getAdditionalLayoutRefs().keySet());
                map.get(model.getId()).addAll(optRecV2.getRemovalLayoutRefs().keySet());
            }
        }
        return map;
    }

    @Override
    public Map<String, Object> getFavoriteRules(String project) {
        Map<String, Object> result = Maps.newHashMap();

        for (String ruleName : FAVORITE_RULE_NAMES) {
            getSingleRule(project, ruleName, result);
        }

        return result;
    }

    private void getSingleRule(String project, String ruleName, Map<String, Object> result) {
        FavoriteRule rule = getFavoriteRule(project, ruleName);
        List<FavoriteRule.Condition> conds = (List<FavoriteRule.Condition>) (List<?>) rule.getConds();

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            result.put("freq_enable", rule.isEnabled());
            String frequency = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getRightThreshold();
            result.put("freq_value", StringUtils.isEmpty(frequency) ? null : Float.parseFloat(frequency));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            result.put("count_enable", rule.isEnabled());
            String count = conds.get(0).getRightThreshold();
            result.put("count_value", StringUtils.isEmpty(count) ? null : Float.parseFloat(count));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            List<String> users = Lists.newArrayList();
            conds.forEach(cond -> users.add(cond.getRightThreshold()));
            result.put("submitter_enable", rule.isEnabled());
            result.put("users", users);
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            List<String> userGroups = Lists.newArrayList();
            conds.forEach(cond -> userGroups.add(cond.getRightThreshold()));
            result.put("user_groups", userGroups);
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            result.put("duration_enable", rule.isEnabled());
            String minDuration = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getLeftThreshold();
            String maxDuration = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getRightThreshold();
            result.put("min_duration", StringUtils.isEmpty(minDuration) ? null : Long.parseLong(minDuration));
            result.put("max_duration", StringUtils.isEmpty(maxDuration) ? null : Long.parseLong(maxDuration));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            result.put("recommendation_enable", rule.isEnabled());
            String upperBound = conds.get(0).getRightThreshold();
            result.put("recommendations_value", StringUtils.isEmpty(upperBound) ? null : Long.parseLong(upperBound));
            break;
        case FavoriteRule.EXCLUDED_TABLES_RULE:
            result.put("excluded_tables_enable", rule.isEnabled());
            String excludedTables = conds.get(0).getRightThreshold();
            result.put("excluded_tables", excludedTables);
            break;
        case FavoriteRule.MIN_HIT_COUNT:
            result.put("min_hit_count", parseInt(conds.get(0).getRightThreshold()));
            break;
        case FavoriteRule.EFFECTIVE_DAYS:
            result.put("effective_days", parseInt(conds.get(0).getRightThreshold()));
            break;
        case FavoriteRule.UPDATE_FREQUENCY:
            result.put("update_frequency", parseInt(conds.get(0).getRightThreshold()));
            break;
        default:
            break;
        }
    }

    private Integer parseInt(String str) {
        return StringUtils.isEmpty(str) ? null : Integer.parseInt(str);
    }

    private FavoriteRule getFavoriteRule(String project, String ruleName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(StringUtils.isNotEmpty(ruleName));

        return getManager(FavoriteRuleManager.class, project).getOrDefaultByName(ruleName);
    }
}
