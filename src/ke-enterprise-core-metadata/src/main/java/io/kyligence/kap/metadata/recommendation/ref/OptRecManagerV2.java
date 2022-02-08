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

package io.kyligence.kap.metadata.recommendation.ref;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.RandomUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.cube.optimization.GarbageLayoutType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.candidate.LayoutMetric;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecManagerV2 {

    public static OptRecManagerV2 getInstance(String project) {
        return Singletons.getInstance(project, OptRecManagerV2.class);
    }

    private final String project;

    OptRecManagerV2(String project) {
        this.project = project;
    }

    public OptRecV2 loadOptRecV2(String uuid) {
        Preconditions.checkState(StringUtils.isNotEmpty(uuid));
        OptRecV2 optRecV2 = new OptRecV2(project, uuid, true);
        optRecV2.initRecommendation();
        List<Integer> brokenLayoutIds = Lists.newArrayList(optRecV2.getBrokenRefIds());
        if (!brokenLayoutIds.isEmpty()) {
            log.debug("recognized broken index ids: {}", brokenLayoutIds);
            RawRecManager.getInstance(project).removeByIds(brokenLayoutIds);
        }
        return optRecV2;
    }

    public void discardAll(String uuid) {
        OptRecV2 optRecV2 = loadOptRecV2(uuid);
        List<Integer> rawIds = optRecV2.getRawIds();
        Map<Integer, RawRecItem> rawRecItemMap = optRecV2.getRawRecItemMap();
        List<Integer> layoutRawIds = Lists.newArrayList();
        rawIds.forEach(recId -> {
            if (rawRecItemMap.get(recId).isLayoutRec()) {
                layoutRawIds.add(recId);
            }
        });
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.discardByIds(layoutRawIds);
    }

    public boolean genRecItemsFromIndexOptimizer(String project, String modelId,
            Map<Long, GarbageLayoutType> garbageLayouts) {
        if (garbageLayouts.isEmpty()) {
            return false;
        }
        log.info("Generating raw recommendations from index optimizer for model({}/{})", project, modelId);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        NDataModel model = modelManager.getDataModelDesc(modelId);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Map<Long, LayoutEntity> allLayoutsMap = indexPlan.getAllLayoutsMap();

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();

        RawRecManager recManager = RawRecManager.getInstance(project);
        Map<String, RawRecItem> layoutRecommendations = recManager.queryNonAppliedLayoutRawRecItems(modelId, false);
        Map<String, String> uniqueFlagToUuid = Maps.newHashMap();
        layoutRecommendations.forEach((k, v) -> {
            LayoutRecItemV2 recEntity = (LayoutRecItemV2) v.getRecEntity();
            uniqueFlagToUuid.put(recEntity.getLayout().genUniqueContent(), k);
        });
        AtomicInteger newRecCount = new AtomicInteger(0);
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        garbageLayouts.forEach((layoutId, type) -> {
            LayoutEntity layout = allLayoutsMap.get(layoutId);
            String uniqueString = layout.genUniqueContent();
            String uuid = uniqueFlagToUuid.get(uniqueString);
            FrequencyMap frequencyMap = hitFrequencyMap.getOrDefault(layoutId, new FrequencyMap());
            RawRecItem recItem;
            if (uniqueFlagToUuid.containsKey(uniqueString)) {
                recItem = layoutRecommendations.get(uuid);
                recItem.setUpdateTime(System.currentTimeMillis());
                recItem.setRecSource(type.name());
                if (recItem.getState() == RawRecItem.RawRecState.DISCARD) {
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    LayoutMetric layoutMetric = recItem.getLayoutMetric();
                    if (layoutMetric == null) {
                        recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                    } else {
                        layoutMetric.setFrequencyMap(frequencyMap);
                    }
                }
            } else {
                LayoutRecItemV2 item = new LayoutRecItemV2();
                item.setLayout(layout);
                item.setCreateTime(System.currentTimeMillis());
                item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
                item.setUuid(RandomUtil.randomUUIDStr());

                recItem = new RawRecItem(project, modelId, model.getSemanticVersion(),
                        RawRecItem.RawRecType.REMOVAL_LAYOUT);
                recItem.setRecEntity(item);
                recItem.setCreateTime(item.getCreateTime());
                recItem.setUpdateTime(item.getCreateTime());
                recItem.setState(RawRecItem.RawRecState.INITIAL);
                recItem.setUniqueFlag(item.getUuid());
                recItem.setDependIDs(item.genDependIds());
                recItem.setLayoutMetric(new LayoutMetric(frequencyMap, new LayoutMetric.LatencyMap()));
                recItem.setRecSource(type.name());
                newRecCount.getAndIncrement();
            }

            if (recItem.getLayoutMetric() != null) {
                rawRecItems.add(recItem);
            }
        });
        RawRecManager.getInstance(project).saveOrUpdate(rawRecItems);
        log.info("Raw recommendations from index optimizer for model({}/{}) successfully generated.", project, modelId);

        return newRecCount.get() > 0;
    }
}
