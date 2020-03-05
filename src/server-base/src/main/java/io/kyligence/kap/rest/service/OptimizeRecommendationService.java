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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.cube.optimization.IndexOptimizerFactory;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeContext;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationVerifier;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
import io.kyligence.kap.rest.response.LayoutRecommendationDetailResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.RecommendationStatsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.var;

@Component("optimizeRecommendationService")
public class OptimizeRecommendationService extends BasicService {

    public OptRecommendationResponse getRecommendationByModel(String project, String modelId, List<String> sources) {
        var optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null) {
            optRecommendation = new OptimizeRecommendation();
            optRecommendation.setUuid(modelId);
            optRecommendation.setProject(project);
        }

        return new OptRecommendationResponse(optRecommendation, sources);
    }

    public OptRecommendationResponse getRecommendationByModel(String project, String modelId) {
        var optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null) {
            optRecommendation = new OptimizeRecommendation();
            optRecommendation.setUuid(modelId);
            optRecommendation.setProject(project);
        }
        return new OptRecommendationResponse(optRecommendation, null);
    }

    @Transaction(project = 1)
    public void applyRecommendations(ApplyRecommendationsRequest request, String project) {
        updateOptimizeRecom(request);
        shiftLayoutHitCount(project, request.getModelId());
        val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                request.getModelId());
        val passCCItems = request.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val passDimensionItems = request.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val passMeasureItems = request.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());

        val passIndexItems = Sets.<Long> newHashSet(request.getIndexRecommendationItemIds());

        verifier.setPassCCItems(passCCItems);
        verifier.setPassDimensionItems(passDimensionItems);
        verifier.setPassMeasureItems(passMeasureItems);
        verifier.setPassLayoutItems(passIndexItems);

        verifier.verify();
    }

    @Transaction(project = 1)
    public void removeRecommendations(RemoveRecommendationsRequest request, String project) {
        val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                request.getModelId());

        verifier.setFailCCItems(Sets.newHashSet(request.getCcItemIds()));
        verifier.setFailDimensionItems(Sets.newHashSet(request.getDimensionItemIds()));
        verifier.setFailMeasureItems(Sets.newHashSet(request.getMeasureItemIds()));
        verifier.setFailLayoutItems(Sets.newHashSet(request.getIndexItemIds()));

        verifier.verify();
    }

    public LayoutRecommendationDetailResponse getLayoutRecommendationContent(String project, String modelId,
            String content, long itemId, int offset, int size) {
        val optRecommendation = getOptRecommendationManager(project).getOptimizeRecommendation(modelId);
        if (optRecommendation == null)
            return null;

        val optimizedModel = getOptimizedModel(project, modelId);
        for (val item : optRecommendation.getLayoutRecommendations()) {
            if (item.getItemId() == itemId) {
                return new LayoutRecommendationDetailResponse(item, optimizedModel, content, offset, size);
            }
        }

        throw new IllegalArgumentException(String.format("index [%s] does not exist", itemId));
    }

    private NDataModel getOptimizedModel(String project, String modelId) {
        return getOptRecommendationManager(project).applyModel(modelId);
    }

    private void updateOptimizeRecom(ApplyRecommendationsRequest request) {
        val originalModel = getDataModelManager(request.getProject()).getDataModelDesc(request.getModelId());

        if (originalModel.isBroken())
            throw new IllegalArgumentException(String.format(
                    "model [%s] is broken, cannot apply any optimize recommendations", originalModel.getAlias()));

        val copiedOriginalModel = getDataModelManager(request.getProject()).copyForWrite(originalModel);

        val nameTranslations = Lists.<Pair<String, String>> newArrayList();

        getOptRecommendationManager(request.getProject()).updateOptimizeRecommendation(request.getModelId(),
                optRecomm -> {
                    // cc recommendation
                    val ccMapOrigin = optRecomm.getCcRecommendations().stream()
                            .collect(Collectors.toMap(CCRecommendationItem::getItemId, item -> item));

                    request.getCcRecommendations().forEach(updatedCCItem -> {
                        if (ccMapOrigin.get(updatedCCItem.getItemId()) == null)
                            throw new IllegalArgumentException(
                                    String.format("cc item with id [%s] does not exist", updatedCCItem.getItemId()));

                        val originalCCName = ccMapOrigin.get(updatedCCItem.getItemId()).getCc().getColumnName();
                        val updatedCCName = updatedCCItem.getCc().getColumnName();
                        if (!originalCCName.equalsIgnoreCase(updatedCCName)) {
                            nameTranslations.add(new Pair<>(originalCCName, updatedCCName));
                            updatedCCItem.setAutoChangeName(false);
                        }
                    });

                    val ccMapUpdated = request.getCcRecommendations().stream()
                            .collect(Collectors.toMap(CCRecommendationItem::getItemId, item -> item));

                    ccMapOrigin.putAll(ccMapUpdated);
                    optRecomm.setCcRecommendations(Lists.newArrayList(ccMapOrigin.values()));

                    // dimension recommendation
                    val dimMapOrigin = optRecomm.getDimensionRecommendations().stream()
                            .collect(Collectors.toMap(DimensionRecommendationItem::getItemId, item -> item));
                    request.getDimensionRecommendations().forEach(item -> {
                        val originItem = dimMapOrigin.get(item.getItemId());
                        if (originItem == null) {
                            throw new IllegalArgumentException(
                                    String.format("dimension item with id [%s] does not exist", item.getItemId()));
                        }
                        if (!item.getColumn().getName().equalsIgnoreCase(originItem.getColumn().getName())) {
                            item.setAutoChangeName(false);
                        }
                    });
                    val dimMapUpdated = request.getDimensionRecommendations().stream()
                            .collect(Collectors.toMap(DimensionRecommendationItem::getItemId, item -> item));

                    dimMapOrigin.putAll(dimMapUpdated);
                    optRecomm.setDimensionRecommendations(Lists.newArrayList(dimMapOrigin.values()));

                    // measure recommendation
                    val measureMapOrigin = optRecomm.getMeasureRecommendations().stream()
                            .collect(Collectors.toMap(MeasureRecommendationItem::getItemId, item -> item));
                    request.getMeasureRecommendations().forEach(item -> {
                        val originItem = measureMapOrigin.get(item.getItemId());
                        if (originItem == null) {
                            throw new IllegalArgumentException(
                                    String.format("measure item with id [%s] does not exist", item.getItemId()));
                        }
                        if (!item.getMeasure().getName().equalsIgnoreCase(originItem.getMeasure().getName())) {
                            item.setAutoChangeName(false);
                        }
                    });
                    val measureMapUpdated = request.getMeasureRecommendations().stream()
                            .collect(Collectors.toMap(MeasureRecommendationItem::getItemId, item -> item));

                    measureMapOrigin.putAll(measureMapUpdated);
                    optRecomm.setMeasureRecommendations(Lists.newArrayList(measureMapOrigin.values()));
                });

        val updatedRecom = getOptRecommendationManager(request.getProject())
                .getOptimizeRecommendation(request.getModelId());
        val optContext = new OptimizeContext(copiedOriginalModel, updatedRecom);
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
            if (CollectionUtils.isEmpty(modelAlias) || modelAlias.stream().map(String::toLowerCase)
                    .collect(Collectors.toList()).contains(model.getAlias().toLowerCase())) {
                shiftLayoutHitCount(project, model.getUuid());
                val verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(), project,
                        model.getId());
                verifier.verifyAll();
            }
        });
    }

    private void shiftLayoutHitCount(String project, String modelUuid) {
        // removal recommendations
        OptimizeRecommendation optimizeRecommendation = getOptRecommendationManager(project)
                .getOptimizeRecommendation(modelUuid);
        if (optimizeRecommendation == null) {
            return;
        }
        Set<Long> toBeDeletedLayouts = Sets.newHashSet();
        for (LayoutRecommendationItem layoutRecommendation : optimizeRecommendation.getLayoutRecommendations()) {
            if (!layoutRecommendation.isAdd()) {
                toBeDeletedLayouts.add(layoutRecommendation.getLayout().getId());
            }
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfManager = NDataflowManager.getInstance(config, project);
        NDataflow originDf = dfManager.getDataflow(modelUuid);
        NDataflow copiedDf = originDf.copy();
        Set<Long> ignored = IndexOptimizerFactory.getOptimizer(copiedDf, true).getGarbageLayoutMap(copiedDf).keySet();
        Map<Long, FrequencyMap> layoutHitCount = copiedDf.getLayoutHitCount();
        layoutHitCount.forEach((id, freqMap) -> {
            if (!toBeDeletedLayouts.contains(id)) {
                val oriMap = originDf.getLayoutHitCount().get(id);
                if (oriMap != null) {
                    layoutHitCount.put(id, oriMap);
                }
            }
        });

        dfManager.updateDataflow(copiedDf.getUuid(),
                copyForWrite -> copyForWrite.setLayoutHitCount(copiedDf.getLayoutHitCount()));
    }
}
