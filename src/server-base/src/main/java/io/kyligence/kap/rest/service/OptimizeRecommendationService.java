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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.IndexRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeContext;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationVerifier;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
import io.kyligence.kap.rest.response.AggIndexRecomDetailResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.RecommendationStatsResponse;
import io.kyligence.kap.rest.response.TableIndexRecommendationResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("optimizeRecommendationService")
public class OptimizeRecommendationService extends BasicService {

    public OptRecommendationResponse getRecommendationByModel(String project, String modelId) {
        val optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null)
            return null;

        return new OptRecommendationResponse(optRecommendation);
    }

    @Transaction(project = 1)
    public void applyRecommendations(ApplyRecommendationsRequest request, String project) {
        updateOptimizeRecom(request);

        val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                request.getModelId());
        val passCCItems = request.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val passDimensionItems = request.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val passMeasureItems = request.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());

        val passIndexItems = Sets.<Long> newHashSet();

        request.getAggIndexRecommendations().forEach(item -> passIndexItems.addAll(item.getItemIds()));
        request.getTableIndexRecommendations().forEach(item -> passIndexItems.add(item.getItemId()));

        verifier.setPassCCItems(passCCItems);
        verifier.setPassDimensionItems(passDimensionItems);
        verifier.setPassMeasureItems(passMeasureItems);
        verifier.setPassIndexItems(passIndexItems);

        verifier.verify();
    }

    @Transaction(project = 1)
    public void removeRecommendations(RemoveRecommendationsRequest request, String project) {
        val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                request.getModelId());
        val failIndexItemIds = Stream
                .concat(request.getAggIndexItemIds().stream(), request.getTableIndexItemIds().stream())
                .collect(Collectors.toSet());
        verifier.setFailCCItems(Sets.newHashSet(request.getCcItemIds()));
        verifier.setFailDimensionItems(Sets.newHashSet(request.getDimensionItemIds()));
        verifier.setFailMeasureItems(Sets.newHashSet(request.getMeasureItemIds()));
        verifier.setFailIndexItems(failIndexItemIds);

        verifier.verify();
    }

    public AggIndexRecomDetailResponse getAggIndexRecomContent(String project, String modelId, String content,
            long indexId, int offset, int size) {
        val optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null)
            return null;

        val optimizedModel = getOptimizedModel(project, modelId);
        for (IndexRecommendationItem item : optRecommendation.getIndexRecommendations()) {
            if (item.getEntity().getId() == indexId) {
                return new AggIndexRecomDetailResponse(item.getEntity(), optimizedModel, content, offset, size);
            }
        }

        throw new IllegalArgumentException(String.format("agg index [%s] does not exist", indexId));
    }

    private NDataModel getOptimizedModel(String project, String modelId) {
        return getOptRecommendationManager(project).applyModel(modelId);
    }

    public TableIndexRecommendationResponse getTableIndexRecomContent(String project, String modelId, String content,
            long layoutId, int offset, int size) {
        val optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null)
            return null;

        val optimizedModel = getOptimizedModel(project, modelId);
        for (IndexRecommendationItem item : optRecommendation.getIndexRecommendations()) {
            for (LayoutEntity layoutEntity : item.getEntity().getLayouts()) {
                if (layoutEntity.getId() == layoutId) {
                    return new TableIndexRecommendationResponse(layoutEntity, optimizedModel, content, offset, size);
                }
            }
        }

        throw new IllegalArgumentException(String.format("table index [%s] does not exist", layoutId));
    }

    private void updateOptimizeRecom(ApplyRecommendationsRequest request) {
        val originalModel = getDataModelManager(request.getProject()).getDataModelDesc(request.getModelId());

        if (originalModel.isBroken())
            throw new IllegalArgumentException(String.format(
                    "model [%s] is broken, cannot apply any optimize recommendations", originalModel.getAlias()));

        val copiedOrinalModel = getDataModelManager(request.getProject()).copyForWrite(originalModel);

        val nameTranslations = Lists.<Pair<String, String>> newArrayList();

        getOptRecommendationManager(request.getProject()).updateOptimizeRecommendation(request.getModelId(),
                optRecomm -> {
                    // cc recommendation
                    val ccMapOrigin = optRecomm.getCcRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));

                    request.getCcRecommendations().forEach(updatedCCItem -> {
                        if (ccMapOrigin.get(updatedCCItem.getItemId()) == null)
                            throw new IllegalArgumentException(
                                    String.format("cc item with id [%s] does not exist", updatedCCItem.getItemId()));

                        val originalCCName = ccMapOrigin.get(updatedCCItem.getItemId()).getCc().getColumnName();
                        val updatedCCName = updatedCCItem.getCc().getColumnName();
                        if (!originalCCName.equals(updatedCCName)) {
                            nameTranslations.add(new Pair<>(originalCCName, updatedCCName));
                            updatedCCItem.setAutoChangeName(false);
                        }
                    });

                    val ccMapUpdated = request.getCcRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));

                    ccMapOrigin.putAll(ccMapUpdated);
                    optRecomm.setCcRecommendations(Lists.newArrayList(ccMapOrigin.values()));

                    // dimension recommendation
                    val dimMapOrigin = optRecomm.getDimensionRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));
                    val dimMapUpdated = request.getDimensionRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));

                    dimMapOrigin.putAll(dimMapUpdated);
                    optRecomm.setDimensionRecommendations(Lists.newArrayList(dimMapOrigin.values()));

                    // measure recommendation
                    val measureMapOrigin = optRecomm.getMeasureRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));
                    val measureMapUpdated = request.getMeasureRecommendations().stream()
                            .collect(Collectors.toMap(item -> item.getItemId(), item -> item));

                    measureMapOrigin.putAll(measureMapUpdated);
                    optRecomm.setMeasureRecommendations(Lists.newArrayList(measureMapOrigin.values()));
                });

        val updatedRecom = getOptRecommendationManager(request.getProject())
                .getOptimizeRecommendation(request.getModelId());
        val optContext = new OptimizeContext(copiedOrinalModel, updatedRecom);
        optContext.setNameTranslations(nameTranslations);
        updatedRecom.getCcRecommendations().forEach(ccItem -> ccItem.translate(optContext));
        updatedRecom.getDimensionRecommendations().forEach(dimensionItem -> dimensionItem.translate(optContext));
        updatedRecom.getMeasureRecommendations().forEach(measureItem -> measureItem.translate(optContext));

        getOptRecommendationManager(request.getProject()).update(optContext);
    }

    public RecommendationStatsResponse getRecommendationsStatsByProject(String project) {
        val projectInstance = getProjectManager().getProject(project);
        if (projectInstance == null)
            throw new IllegalArgumentException(String.format("project [%s] does not exist", project));

        val recommendationManager = getOptRecommendationManager(project);
        val modelStatsList = Lists.<RecommendationStatsResponse.RecommendationStatsByModel> newArrayList();
        int recommendationCount = 0;
        long lastAcceptedTime = 0;

        for (NDataModel model : getDataModelManager(project).listAllModels()) {
            if (model.isBroken())
                continue;

            val recommendation = recommendationManager.getOptimizeRecommendation(model.getId());

            val modelStats = new RecommendationStatsResponse.RecommendationStatsByModel();
            modelStats.setAlias(model.getAlias());
            modelStats.setOwner(model.getOwner());
            if (recommendation != null) {
                recommendationCount += recommendation.getRecommendationsCount();
                val modelLastAcceptedTime = recommendation.getLastVerifiedTime();
                if (modelLastAcceptedTime > lastAcceptedTime)
                    lastAcceptedTime = modelLastAcceptedTime;
                modelStats.setLastAcceptedTime(modelLastAcceptedTime);
                modelStats.setRecommendationsSize(recommendation.getRecommendationsCount());
            }
            modelStatsList.add(modelStats);
        }

        val response = new RecommendationStatsResponse();
        response.setTotalRecommendations(recommendationCount);
        response.setModels(modelStatsList);
        response.setOwner(projectInstance.getOwner());
        response.setLastAcceptedTime(lastAcceptedTime);
        return response;
    }

    @Transaction(project = 0)
    public void batchApplyRecommendations(String project, List<String> modelAlias) {
        val models = getDataflowManager(project).listAllDataflows().stream()
                .filter(df -> df.getStatus() == RealizationStatusEnum.ONLINE && !df.getModel().isBroken())
                .map(NDataflow::getModel);
        models.forEach(model -> {
            if (CollectionUtils.isEmpty(modelAlias) || modelAlias.contains(model.getAlias())) {
                val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                        model.getId());
                verifier.verifyAll();
            }
        });
    }
}
