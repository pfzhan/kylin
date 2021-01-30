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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawRecManager {

    private static final int LAG_SEMANTIC_VERSION = 1;

    private final String project;
    private final JdbcRawRecStore jdbcRawRecStore;

    // CONSTRUCTOR
    public static RawRecManager getInstance(String project) {
        return Singletons.getInstance(project, RawRecManager.class);
    }

    private RawRecManager(String project) throws Exception {
        this.project = project;
        this.jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    /**
     * Load CC, Dimension, measure RawRecItems of given models. 
     * If models not given, load these RawRecItems of the whole project.
     */
    public Map<String, RawRecItem> queryNonLayoutRecItems(Set<String> modelIdSet) {
        Set<String> allModelList = modelIdSet;
        if (modelIdSet == null || modelIdSet.isEmpty()) {
            allModelList = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listAllModelIds();
        }
        Map<String, RawRecItem> allRecItems = Maps.newHashMap();
        for (String model : allModelList) {
            allRecItems.putAll(queryNonLayoutRecItems(model));
        }
        return allRecItems;
    }

    private Map<String, RawRecItem> queryNonLayoutRecItems(String model) {
        Map<String, RawRecItem> recItemMap = Maps.newHashMap();
        List<RawRecItem> recItems = jdbcRawRecStore.queryNonLayoutRecItems(project, model);
        if (CollectionUtils.isEmpty(recItems)) {
            log.info("There is no raw recommendations of model({}/{}})", project, model);
            return recItemMap;
        }
        recItems.forEach(recItem -> recItemMap.putIfAbsent(recItem.getUniqueFlag(), recItem));
        return recItemMap;
    }

    public Map<String, RawRecItem> queryNonAppliedLayoutRawRecItems(String model, boolean isAdditionalRec) {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryNonAppliedLayoutRecItems(project, model, isAdditionalRec);
        Map<String, RawRecItem> map = Maps.newHashMap();
        rawRecItems.forEach(recItem -> map.put(recItem.getUniqueFlag(), recItem));
        return map;
    }

    public void clearExistingCandidates(String project, String model) {
        long start = System.currentTimeMillis();
        List<RawRecItem> existingCandidates = jdbcRawRecStore.queryAdditionalLayoutRecItems(project, model);
        long updateTime = System.currentTimeMillis();
        existingCandidates.forEach(rawRecItem -> {
            rawRecItem.setUpdateTime(updateTime);
            if (!RawRecItem.IMPORTED.equalsIgnoreCase(rawRecItem.getRecSource())) {
                rawRecItem.setState(RawRecItem.RawRecState.INITIAL);
            }
        });
        jdbcRawRecStore.update(existingCandidates);
        log.info("clear all existing candidate recommendations of model({}/{}) takes {} ms.", //
                project, model, System.currentTimeMillis() - start);
    }

    public List<RawRecItem> displayTopNRecItems(String project, String model, int limit) {
        return jdbcRawRecStore.chooseTopNCandidates(project, model, limit, RawRecItem.RawRecState.RECOMMENDED);
    }

    public List<RawRecItem> queryImportedRawRecItems(String project, String model) {
        return jdbcRawRecStore.queryImportedRawRecItems(project, model, RawRecItem.RawRecState.RECOMMENDED);
    }

    public void updateRecommendedTopN(String project, String model, int topN) {
        long current = System.currentTimeMillis();
        RawRecManager rawRecManager = RawRecManager.getInstance(project);
        rawRecManager.clearExistingCandidates(project, model);
        List<RawRecItem> topNCandidates = jdbcRawRecStore.chooseTopNCandidates(project, model, topN,
                RawRecItem.RawRecState.INITIAL);
        topNCandidates.forEach(rawRecItem -> {
            rawRecItem.setUpdateTime(current);
            rawRecItem.setRecSource(RawRecItem.QUERY_HISTORY);
            rawRecItem.setState(RawRecItem.RawRecState.RECOMMENDED);
        });
        rawRecManager.saveOrUpdate(topNCandidates);
    }

    public Map<RawRecItem.RawRecType, Integer> getCandidatesByProject(String project) {
        RawRecItem.RawRecType[] rawRecTypes = { RawRecItem.RawRecType.ADDITIONAL_LAYOUT,
                RawRecItem.RawRecType.REMOVAL_LAYOUT };
        int additionalCandidateCount = jdbcRawRecStore.getRecItemCountByProject(project, rawRecTypes[0]);
        int removalCandidatesCount = jdbcRawRecStore.getRecItemCountByProject(project, rawRecTypes[1]);
        Map<RawRecItem.RawRecType, Integer> map = Maps.newHashMap();
        map.put(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, additionalCandidateCount);
        map.put(RawRecItem.RawRecType.REMOVAL_LAYOUT, removalCandidatesCount);
        return map;
    }

    public List<RawRecItem> getCandidatesByProjectAndBenefit(String project, int limit) {
        throw new NotImplementedException("get candidate raw recommendations by project not implement!");
    }

    public void saveOrUpdate(List<RawRecItem> recItems) {
        jdbcRawRecStore.batchAddOrUpdate(recItems);
    }

    public void deleteAllOutDated(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ImmutableList<String> models = projectManager.getProject(project).getModels();
        models.forEach(model -> deleteOutDated(model, LAG_SEMANTIC_VERSION));
    }

    public void deleteOutDated(String model, int lagVersion) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(model);
        if (dataModel.isBroken()) {
            log.info("Skip delete outdated RawRecItems of model({}/{})", project, model);
        } else {
            int semanticVersion = dataModel.getSemanticVersion();
            jdbcRawRecStore.deleteBySemanticVersion(semanticVersion - lagVersion, model);
        }
    }

    public void discardRecItemsOfBrokenModel(String model) {
        jdbcRawRecStore.discardRecItemsOfBrokenModel(model);
    }

    public void deleteRecItemsOfNonExistModels(String project, Set<String> existingModels) {
        jdbcRawRecStore.deleteRecItemsOfNonExistModels(project, existingModels);
    }

    public void deleteByProject(String project) {
        jdbcRawRecStore.deleteByProject(project);
    }

    public void cleanForDeletedProject(List<String> projectList) {
        jdbcRawRecStore.cleanForDeletedProject(projectList);
    }

    public void removeByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.BROKEN);
    }

    public void applyByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.APPLIED);
    }

    public void discardByIds(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.DISCARD);
    }

    public void updateAllCost(String project) {
        jdbcRawRecStore.updateAllCost(project);
    }

    public int getMaxId() {
        return jdbcRawRecStore.getMaxId();
    }

    public RawRecItem getRawRecItemByUniqueFlag(String project, String modelId, String uniqueFlag, Integer semanticVersion) {
        return jdbcRawRecStore.queryByUniqueFlag(project, modelId, uniqueFlag, semanticVersion);
    }

    public void importRecommendations(String project, String targetModelId, List<RawRecItem> recItems) {
        jdbcRawRecStore.importRecommendations(project, targetModelId, recItems);
    }
}
