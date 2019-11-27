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

package io.kyligence.kap.smart;

import static io.kyligence.kap.metadata.favorite.FavoriteQuery.CHANNEL_FROM_IMPORTED;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSmartMaster {

    private static final String MODEL_ALIAS_PREFIX = "AUTO_MODEL_";

    private NSmartContext context;
    private NProposerProvider proposerProvider;
    private String project;

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls);
        this.proposerProvider = NProposerProvider.create(context);
    }

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls, String draftVersion) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls, draftVersion);
        this.proposerProvider = NProposerProvider.create(context);
    }

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls, boolean reuseExistedModel) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls, reuseExistedModel, false);
        this.proposerProvider = NProposerProvider.create(context);
    }

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls, boolean reuseExistedModel,
            boolean couldCreateNewModel) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls, reuseExistedModel, couldCreateNewModel);
        this.proposerProvider = NProposerProvider.create(context);
    }

    public NSmartContext getContext() {
        return context;
    }

    public void analyzeSQLs() {
        long start = System.currentTimeMillis();
        log.info("Start sql analysis.");
        proposerProvider.getSQLAnalysisProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("SQL analysis completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void selectModel() {
        long start = System.currentTimeMillis();
        log.info("Start model selection.");
        proposerProvider.getModelSelectProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("Model selection completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void optimizeModel() {
        long start = System.currentTimeMillis();
        log.info("Start model optimization.");
        proposerProvider.getModelOptProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("Model optimization completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    private void adjustModelInfoByPrjMode() {
        long start = System.currentTimeMillis();
        log.info("Start adjust model atrribute.");
        proposerProvider.getAutoOrSemiModeInfoAdjustProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("Model adjust completed, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void selectIndexPlan() {
        long start = System.currentTimeMillis();
        log.info("Start indexPlan selection.");
        proposerProvider.getIndexPlanSelectProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("IndexPlan selection completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void optimizeIndexPlan() {
        long start = System.currentTimeMillis();
        log.info("Start indexPlan optimization.");
        proposerProvider.getIndexPlanOptProposer().propose();
        val nums = getAccelerationNumMap();
        log.info("IndexPlan optimization completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void shrinkIndexPlan() {
        proposerProvider.getIndexPlanShrinkProposer().propose();
    }

    public void shrinkModel() {
        proposerProvider.getModelShrinkProposer().propose();
    }

    public void renameModel() {
        long start = System.currentTimeMillis();
        log.info("Start renaming alias of all proposed model.");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<String> usedNames = dataModelManager.listAllModelAlias();
        List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        for (NSmartContext.NModelContext modelCtx : modelContexts) {
            if (modelCtx.withoutTargetModel()) {
                continue;
            }

            NDataModel targetModel = modelCtx.getTargetModel();
            String alias = modelCtx.getOriginModel() == null //
                    ? proposeModelAlias(targetModel, usedNames) //
                    : modelCtx.getOriginModel().getAlias();
            targetModel.setAlias(alias);
        }
        log.info("Model renaming completed successfully, takes {}ms", System.currentTimeMillis() - start);
    }

    public void selectExistedModel() {
        analyzeSQLs();
        selectModel();
    }

    public void selectAndOptimize() {
        analyzeSQLs();
        selectModel();
        optimizeModel();
        adjustModelInfoByPrjMode();
        renameModel();
        selectIndexPlan();
        optimizeIndexPlan();
        shrinkIndexPlan();
    }

    public Map<NDataModel, OptimizeRecommendation> selectAndGenRecommendation() {
        selectAndOptimize();
        return genOptRecommendations();
    }

    private void createNewModelAndIndex() {
        analyzeSQLs();
        optimizeModel();
        adjustModelInfoByPrjMode();
        renameModel();
        selectIndexPlan();
        optimizeIndexPlan();
        shrinkIndexPlan();
    }

    public Map<NDataModel, OptimizeRecommendation> recommendNewModelAndIndex() {
        Preconditions.checkArgument(!this.context.isReuseExistedModel(),
                "If need to create new model, the flag ReuseExistedModel MUST be FALSE !");
        createNewModelAndIndex();
        return Maps.newHashMap();
    }

    public List<NDataModel> getRecommendedModels() {
        if (CollectionUtils.isEmpty(getContext().getModelContexts())) {
            return Lists.newArrayList();
        }

        List<NDataModel> models = Lists.newArrayList();
        for (NSmartContext.NModelContext modelContext : getContext().getModelContexts()) {
            NDataModel model = modelContext.getTargetModel();
            if (model == null)
                continue;

            models.add(modelContext.getTargetModel());
        }

        return models;
    }

    private void save() {
        saveModel();
        saveIndexPlan();
    }

    // this method now only used for testing
    public void runAll() {
        runAllAndForContext(null);
    }

    @VisibleForTesting
    public void saveModelOnlyForTest() {
        UnitOfWork.doInTransactionWithRetry(new UnitOfWork.Callback<Object>() {
            @Override
            public Object process() {
                save();
                return null;
            }
        }, project);
    }

    // optimize recommendation
    public void runOptRecommendation(Consumer<NSmartContext> hook) {
        long start = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(new UnitOfWork.Callback<Object>() {
                @Override
                public Object process() {
                    selectAndOptimize();
                    Map<NDataModel, OptimizeRecommendation> recommendationMap = genOptRecommendations();
                    recommendationMap.forEach((model, recommendation) -> saveRecommendation(model, recommendation));
                    if (hook != null) {
                        hook.accept(getContext());
                    }
                    return null;
                }

                @Override
                public void onProcessError(Throwable throwable) {
                    recordError(throwable);
                }
            }, project);
        } finally {
            log.info("The whole process of auto-recommendation takes {}ms", System.currentTimeMillis() - start);
            saveAccelerationInfoInTransaction();
        }
    }

    public void runAllAndForContext(Consumer<NSmartContext> hook) {
        long start = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(new UnitOfWork.Callback<Object>() {
                @Override
                public Object process() {
                    selectAndOptimize();
                    save();
                    if (hook != null) {
                        hook.accept(getContext());
                    }
                    return null;
                }

                @Override
                public void onProcessError(Throwable throwable) {
                    recordError(throwable);
                }
            }, project);
        } finally {
            log.info("The whole process of auto-modeling takes {}ms", System.currentTimeMillis() - start);
            saveAccelerationInfoInTransaction();
        }
    }

    private void saveAccelerationInfoInTransaction() {
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                genDiagnoseInfo();
                saveAccelerateInfo();
                return null;
            }, project);
        } catch (Exception e) {
            log.error("save acceleration info error", e);
        }
    }

    private void recordError(Throwable throwable) {
        context.getAccelerateInfoMap().forEach((key, value) -> {
            value.getRelatedLayouts().clear();
            value.setFailedCause(throwable);
        });
    }

    void saveIndexPlan() {
        log.info("Start saving optimized indexPlan to metadata.");
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                context.getProject());
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                context.getProject());
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.withoutTargetModel() || modelCtx.withoutAnyIndexes()) {
                continue;
            }
            IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
            if (indexPlanManager.getIndexPlan(indexPlan.getUuid()) == null) {
                indexPlanManager.createIndexPlan(indexPlan);
                dataflowManager.createDataflow(indexPlan, indexPlan.getModel().getOwner());
            } else {
                indexPlanManager.updateIndexPlan(indexPlan);
            }
        }
        log.info("Saving optimized indexPlan to metadata completed successfully.");
    }

    void saveAccelerateInfo() {
        val favoriteQueryMgr = FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((sqlPattern, accelerateInfo) -> {
            if (accelerateInfo.isNotSucceed()) {
                return;
            }
            List<FavoriteQueryRealization> favoriteQueryRealizations = Lists.newArrayList();
            for (val layout : accelerateInfo.getRelatedLayouts()) {
                FavoriteQueryRealization realization = new FavoriteQueryRealization();
                realization.setSemanticVersion(layout.getSemanticVersion());
                realization.setModelId(layout.getModelId());
                realization.setLayoutId(layout.getLayoutId());
                favoriteQueryRealizations.add(realization);
            }
            favoriteQueryMgr.resetRealizations(sqlPattern, favoriteQueryRealizations);
        });
    }

    // it must be wrapped by a transaction for OptManager.getOrCrate will write metadata !!!
    private Map<NDataModel, OptimizeRecommendation> genOptRecommendations() {
        log.info("Semi-Auto-Mode project:{} start to generate optimized recommendations.", project);

        Map<NDataModel, OptimizeRecommendation> recommendationMap = Maps.newHashMap();
        UnitOfWork.doInTransactionWithRetry(new UnitOfWork.Callback<Object>() {
            @Override
            public Object process() throws Exception {
                OptimizeRecommendationManager optRecMgr = OptimizeRecommendationManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), project);
                for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
                    if (modelCtx.withoutTargetModel() || modelCtx.withoutAnyIndexes()) {
                        log.info(
                                "Semi-Auto-Mode project:{} skip model optimize, withoutTargetModel: {}, withoutAnyIndexes: {}",
                                project, modelCtx.withoutTargetModel(), modelCtx.withoutAnyIndexes());
                        continue;
                    }

                    NDataModel model = modelCtx.getTargetModel();
                    IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
                    if (modelCtx.getOriginModel() != null) {
                        OptimizeRecommendation recommendations = optRecMgr.optimize(model, indexPlan);
                        optRecMgr.logOptimizeRecommendation(model.getId(), recommendations);
                        recommendationMap.putIfAbsent(model, recommendations);
                    }
                    log.info("Semi-Auto-Mode project:{} successfully generate optimized recommendations.", project);
                }
                return null;
            }
        }, project);

        return recommendationMap;
    }

    private void saveRecommendation(NDataModel model, OptimizeRecommendation recommendations) {
        log.info("Semi-Auto-Mode project:{} optimized recommendations are successfully saved to metadata.", project);
        OptimizeRecommendationManager optRecMgr = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        try {
            long indexItemId = 0;
            val oldRecommendations = optRecMgr.getOptimizeRecommendation(model.getId());
            if (oldRecommendations != null) {
                indexItemId = oldRecommendations.getNextLayoutRecommendationItemId();
            }
            long finalIndexItemId = indexItemId;
            boolean isQueryHistory = getContext().getAccelerateInfoMap().entrySet().stream().noneMatch(entry -> {
                String sql = entry.getKey();
                FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager
                        .getInstance(getContext().getKylinConfig(), project);
                return favoriteQueryManager.get(sql).getChannel().equals(CHANNEL_FROM_IMPORTED);
            });
            optRecMgr.updateOptimizeRecommendation(model.getId(), recommendation -> {
                recommendation.getLayoutRecommendations().stream().filter(item -> item.getItemId() >= finalIndexItemId)
                        .forEach(item -> item.setSource(isQueryHistory ? LayoutRecommendationItem.QUERY_HISTORY
                                : LayoutRecommendationItem.IMPORTED));
            });
            optRecMgr.logOptimizeRecommendation(model.getId(), recommendations);
        } catch (Exception e) {
            log.error("Semi-Auto-Mode project:{} model({}) failed to generate recommendations", project,
                    model.getUuid(), e);
        }
    }

    public void saveModel() {
        log.info("Start saving optimized model to metadata.");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.withoutTargetModel() || modelCtx.withoutAnyIndexes()) {
                continue;
            }
            NDataModel model = modelCtx.getTargetModel();
            if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                dataModelManager.updateDataModelDesc(model);
            } else {
                dataModelManager.createDataModelDesc(model, model.getOwner());
            }
        }
        log.info("Saving optimized model to metadata completed successfully.");
    }

    private String proposeModelAlias(NDataModel model, Set<String> usedNames) {
        String rootTableAlias = model.getRootFactTable().getAlias();
        int suffix = 0;
        String targetName;
        do {
            if (suffix++ < 0) {
                throw new IllegalStateException("Potential infinite loop in getModelName().");
            }
            targetName = MODEL_ALIAS_PREFIX + rootTableAlias + "_" + suffix;
        } while (usedNames.contains(targetName));
        log.info("The alias of the model({}) was rename to {}.", model.getId(), targetName);
        usedNames.add(targetName);
        return targetName;
    }

    private Map<AccStatusType, Integer> getAccelerationNumMap() {
        Map<AccStatusType, Integer> result = Maps.newHashMap();
        result.putIfAbsent(AccStatusType.SUCCESS, 0);
        result.putIfAbsent(AccStatusType.PENDING, 0);
        result.putIfAbsent(AccStatusType.FAILED, 0);
        val accelerateInfoMap = context.getAccelerateInfoMap();
        for (Map.Entry<String, AccelerateInfo> entry : accelerateInfoMap.entrySet()) {
            if (entry.getValue().isPending()) {
                result.computeIfPresent(AccStatusType.PENDING, (k, v) -> v + 1);
            } else if (entry.getValue().isFailed()) {
                result.computeIfPresent(AccStatusType.FAILED, (k, v) -> v + 1);
            } else {
                result.computeIfPresent(AccStatusType.SUCCESS, (k, v) -> v + 1);
            }
        }
        return result;
    }

    enum AccStatusType {
        SUCCESS, PENDING, FAILED
    }

    private void genDiagnoseInfo() {
        Map<String, AccelerateInfo> accelerationMap = context.getAccelerateInfoMap();
        Map<String, Set<String>> failureMap = Maps.newHashMap();
        int pendingNum = 0;
        for (Map.Entry<String, AccelerateInfo> entry : accelerationMap.entrySet()) {
            if (!entry.getValue().isNotSucceed()) {
                continue;
            }
            if (entry.getValue().isPending()) {
                pendingNum++;
            }

            String expr;
            if (entry.getValue().getFailedCause() != null) {
                Throwable rootCause = Throwables.getRootCause(entry.getValue().getFailedCause());
                final String stackTraces = StringUtils.join(rootCause.getStackTrace(), "\n");
                if (rootCause instanceof ParseException) {
                    expr = "\nRoot cause: " + rootCause.getMessage().split("\n")[0] + "\n" + stackTraces;
                } else {
                    expr = "\nRoot cause: " + rootCause.getMessage() + "\n" + stackTraces;
                }
            } else {
                expr = "\nPending message: " + entry.getValue().getPendingMsg();
            }
            if (failureMap.get(expr) == null) {
                failureMap.putIfAbsent(expr, Sets.newHashSet());
            }
            failureMap.get(expr).add(entry.getKey());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n================== diagnosis log for auto-modeling ====================\n");
        sb.append("This round accelerates ").append(accelerationMap.size()).append(" queries.\n");
        if (failureMap.isEmpty()) {
            sb.append("No exceptions occurred.");
            sb.append("\n=======================================================================");
            log.info(sb.toString());
        } else {
            int failedNum = failureMap.values().stream().map(Set::size).reduce(Integer::sum).orElse(-1);
            sb.append("SUCCESS: ").append(accelerationMap.size() - failedNum);

            if (pendingNum != 0) {
                sb.append(", PENDING: ").append(pendingNum);
                sb.append(", FAILED: ").append(failedNum - pendingNum);
            } else {
                sb.append(", FAILED: ").append(failedNum);
            }
            sb.append(".\nClassified details are as follows:");
            failureMap.forEach((failedTypeInfo, sqlSet) -> {
                sb.append("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                sb.append(failedTypeInfo).append("\n----------------\n");
                sb.append(String.join("\n----------------\n", sqlSet));
            });
            sb.append("\n=======================================================================");
            log.error(sb.toString());
        }
    }
}
