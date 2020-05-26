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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelReuseContextOfSemiMode extends AbstractSemiAutoContext {

    @Getter
    private boolean canCreateNewModel;

    public ModelReuseContextOfSemiMode(KylinConfig kylinConfig, String project, String[] sqlArray) {
        super(kylinConfig, project, sqlArray);
        this.partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
    }

    public ModelReuseContextOfSemiMode(KylinConfig kylinConfig, String project, String[] sqlArray,
            boolean canCreateNewModel) {
        this(kylinConfig, project, sqlArray);
        this.canCreateNewModel = canCreateNewModel;
    }

    @Override
    public List<NDataModel> getOriginModels() {
        List<NDataModel> onlineModels = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .listDataModelsByStatus(RealizationStatusEnum.ONLINE);
        return genRecommendationEnhancedModels(onlineModels);
    }

    @Override
    public ChainedProposer createTransactionProposers() {
        ImmutableList<NAbstractProposer> proposers = ImmutableList.of(//
                new NModelSelectProposer(this), //
                new NModelOptProposer(this), //
                new NModelInfoAdjustProposer(this), //
                new NModelRenameProposer(this), //
                new NIndexPlanSelectProposer(this), //
                new NIndexPlanOptProposer(this), //
                new NIndexPlanShrinkProposer(this) //
        );
        return new ChainedProposer(this, proposers);
    }

    private List<NDataModel> genRecommendationEnhancedModels(List<NDataModel> models) {
        List<NDataModel> enhancedDataModel = Lists.newArrayListWithCapacity(models.size());
        OptimizeRecommendationManager recommendMgr = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        for (NDataModel model : models) {
            enhancedDataModel.add(recommendMgr.applyModel(model.getId()));
        }
        return enhancedDataModel;
    }

    @Override
    public void handleExceptionAfterModelSelect() {
        if (isCanCreateNewModel()) {
            return;
        }

        getModelContexts().forEach(modelCtx -> {
            if (modelCtx.isTargetModelMissing()) {
                modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                    AccelerateInfo accelerateInfo = getAccelerateInfoMap().get(olapContext.sql);
                    accelerateInfo.setPendingMsg(NModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG);
                });
            }
        });
    }

    @Override
    public void saveMetadata() {
        genOptRecommendations();
    }

    // it must be wrapped by a transaction for OptManager.getOrCrate will write metadata !!!
    public Map<NDataModel, Pair<OptimizeRecommendation, Long>> genOptRecommendations() {
        log.info("Semi-Auto-Mode project:{} start to generate optimized recommendations.", getProject());

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            Map<NDataModel, Pair<OptimizeRecommendation, Long>> recommendationMap = Maps.newHashMap();
            OptimizeRecommendationManager optRecMgr = OptimizeRecommendationManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            for (AbstractContext.NModelContext modelCtx : getModelContexts()) {
                if (modelCtx.isTargetModelMissing() || modelCtx.isProposedIndexesEmpty()) {
                    log.info(
                            "Semi-Auto-Mode project:{} skip model optimize, TargetModelMissing: {}, ProposedIndexesEmpty: {}",
                            getProject(), modelCtx.isTargetModelMissing(), modelCtx.isProposedIndexesEmpty());
                    continue;
                }

                NDataModel model = modelCtx.getTargetModel();
                IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
                if (modelCtx.getOriginModel() != null) {
                    long beforeLayoutItemId = 0;
                    OptimizeRecommendation before = optRecMgr.getOptimizeRecommendation(model.getId());
                    if (before != null) {
                        beforeLayoutItemId = before.getNextLayoutRecommendationItemId();
                    }
                    OptimizeRecommendation recommendations = optRecMgr.optimize(model, indexPlan);
                    optRecMgr.logOptimizeRecommendation(model.getId(), recommendations);
                    recommendationMap.putIfAbsent(model, new Pair<>(recommendations, beforeLayoutItemId));
                    saveRecommendation(model, recommendationMap.get(model));
                }
                log.info("Semi-Auto-Mode project:{} successfully generate optimized recommendations.", getProject());
            }
            return recommendationMap;
        }, getProject());
    }

    private void saveRecommendation(NDataModel model, Pair<OptimizeRecommendation, Long> pair) {
        log.info("Semi-Auto-Mode project:{} optimized recommendations are successfully saved to metadata.",
                getProject());
        OptimizeRecommendationManager optRecMgr = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        try {
            long layoutItemId = pair.getSecond();
            boolean isQueryHistory = getAccelerateInfoMap().entrySet().stream().noneMatch(entry -> {
                String sql = entry.getKey();
                FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                return favoriteQueryManager.get(sql) == null
                        || favoriteQueryManager.get(sql).getChannel().equals(FavoriteQuery.CHANNEL_FROM_IMPORTED);
            });
            optRecMgr.updateOptimizeRecommendation(model.getId(), recommendation -> {
                recommendation.getLayoutRecommendations().stream().filter(item -> item.getItemId() >= layoutItemId)
                        .forEach(item -> item.setSource(isQueryHistory ? LayoutRecommendationItem.QUERY_HISTORY
                                : LayoutRecommendationItem.IMPORTED));
            });
            optRecMgr.logOptimizeRecommendation(model.getId(), pair.getFirst());
        } catch (Exception e) {
            log.error("Semi-Auto-Mode project:{} model({}) failed to generate recommendations", getProject(),
                    model.getUuid(), e);
        }
    }
}
