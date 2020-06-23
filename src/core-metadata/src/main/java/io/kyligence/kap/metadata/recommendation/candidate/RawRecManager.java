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
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawRecManager {

    private static final int MAX_CACHED_NUM = 20_000;
    private static final int LAG_SEMANTIC_VERSION = 1;
    private static final int EXPIRED_TIME = 60;

    private final String project;
    private final KylinConfig kylinConfig;
    private final JdbcRawRecStore jdbcRawRecStore;
    private Cache<String, RawRecItem> cachedRecItems;
    private BiMap<String, Integer> uniqueFlagToId = HashBiMap.create();

    // CONSTRUCTOR
    public static RawRecManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, RawRecManager.class);
    }

    // called by reflection
    static RawRecManager newInstance(KylinConfig config, String project) throws Exception {
        return new RawRecManager(config, project);
    }

    private RawRecManager(KylinConfig config, String project) throws Exception {
        this.project = project;
        this.kylinConfig = config;
        this.jdbcRawRecStore = new JdbcRawRecStore(config);
        this.cachedRecItems = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_NUM)
                .expireAfterAccess(EXPIRED_TIME, TimeUnit.MINUTES).build();
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) {
        ImmutableList<String> models = NProjectManager.getInstance(cfg).getProject(project).getModels();
        models.forEach(model -> {
            queryByRawRecType(model, RawRecItem.RawRecType.COMPUTED_COLUMN, RawRecItem.RawRecState.INITIAL);
            queryByRawRecType(model, RawRecItem.RawRecType.DIMENSION, RawRecItem.RawRecState.INITIAL);
            queryByRawRecType(model, RawRecItem.RawRecType.MEASURE, RawRecItem.RawRecState.INITIAL);
        });
    }

    // CURD
    public Map<String, RawRecItem> listAll() {
        return cachedRecItems.asMap();
    }

    public Map<String, RawRecItem> listByModel(String model) {
        Map<String, RawRecItem> map = Maps.newHashMap();
        cachedRecItems.asMap().forEach((k, v) -> {
            if (v.getModelID().equalsIgnoreCase(model)) {
                map.put(k, v);
            }
        });
        return map;
    }

    private List<RawRecItem> queryByRawRecType(String model, RawRecItem.RawRecType type, RawRecItem.RawRecState state) {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryByRecTypeAndState(project, model, type, state);
        rawRecItems.forEach(this::updateCache);
        return rawRecItems;
    }

    public Map<String, RawRecItem> queryLayoutRawRecItems(String model) {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryLayoutRawRecItems(project, model);
        Map<String, RawRecItem> map = Maps.newHashMap();
        rawRecItems.forEach(recItem -> map.put(recItem.getUniqueFlag(), recItem));
        return map;
    }

    public List<RawRecItem> getCandidatesByModelAndBenefit(String project, String model, int limit) {
        List<RawRecItem> topNCandidate = jdbcRawRecStore.getTopNCandidate(project, model, limit);
        topNCandidate.forEach(this::updateCache);
        return topNCandidate;
    }

    public List<RawRecItem> getCandidatesByProjectAndBenefit(String project, int limit) {
        return null;
    }

    public void save(RawRecItem rawRecItem) {
        jdbcRawRecStore.save(rawRecItem);
        updateCache(rawRecItem);
    }

    public void batchSave(List<RawRecItem> recItems) {
        jdbcRawRecStore.save(recItems);
        updateCache(recItems);
    }

    public void saveOrUpdate(RawRecItem recItem) {
        jdbcRawRecStore.addOrUpdate(recItem);
        updateCache(recItem);
    }

    public void saveOrUpdate(List<RawRecItem> recItems) {
        jdbcRawRecStore.batchAddOrUpdate(recItems);
        updateCache(recItems);
    }

    public RawRecItem queryById(int id) {
        if (uniqueFlagToId.inverse().containsKey(id)) {
            String key = uniqueFlagToId.inverse().get(id);
            return cachedRecItems.getIfPresent(key);
        }
        RawRecItem recItem = jdbcRawRecStore.queryById(id);
        if (recItem != null) {
            updateCache(recItem);
        }
        return recItem;
    }

    public void deleteAllOutDatedRecommendations(String project) {
        ImmutableList<String> models = NProjectManager.getInstance(kylinConfig).getProject(project).getModels();
        models.forEach(model -> deleteOutDatedRecommendations(model, LAG_SEMANTIC_VERSION));
        cachedRecItems.invalidateAll();
        init(kylinConfig, project);
    }

    public void deleteOutDatedRecommendations(String model, int lagVersion) {
        int semanticVersion = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(model).getSemanticVersion();
        jdbcRawRecStore.deleteBySemanticVersion(semanticVersion - lagVersion, model);
    }

    public void removeRecommendations(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.BROKEN);
        removeFromCache(idList);
    }

    public void suggestRecommendations(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.RECOMMENDED);
        removeFromCache(idList);
    }

    public void applyRecommendations(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.APPLIED);
        removeFromCache(idList);
    }

    public void discardRawRecommendations(List<Integer> idList) {
        jdbcRawRecStore.updateState(idList, RawRecItem.RawRecState.DISCARD);
        removeFromCache(idList);
    }

    public void updateAllCost(String project) {
        jdbcRawRecStore.updateAllCost(project);
    }

    private void updateCache(List<RawRecItem> recItems) {
        recItems.forEach(this::updateCache);
    }

    private void updateCache(RawRecItem recItem) {
        if (recItem.needCache()) {
            log.debug("Add `{}` to raw recommendation cache", recItem.getUniqueFlag());
            uniqueFlagToId.put(recItem.getUniqueFlag(), recItem.getId());
            cachedRecItems.put(recItem.getUniqueFlag(), recItem);
        } else {
            removeFromCache(recItem.getId());
        }
    }

    private void removeFromCache(List<Integer> idList) {
        idList.forEach(this::removeFromCache);
    }

    private void removeFromCache(int id) {
        String uniqueFlag = uniqueFlagToId.inverse().get(id);
        if (uniqueFlag != null) {
            log.debug("remove `{}` from raw recommendation cache", uniqueFlag);
            uniqueFlagToId.remove(uniqueFlag);
            cachedRecItems.invalidate(uniqueFlag);
        }
    }
}
