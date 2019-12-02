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

package io.kyligence.kap.tool.metadata.migration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import lombok.val;

public class V2__IndexesAndRecommendation extends BaseJavaMigration {

    private final ResourceStore resourceStore;

    private final String metadataTable;

    public V2__IndexesAndRecommendation(ResourceStore resourceStore, String metadataTable) {
        this.resourceStore = resourceStore;
        this.metadataTable = metadataTable;
    }

    @Override
    public void migrate(Context context) throws Exception {
        val projects = resourceStore.getAllResources(ResourceStore.PROJECT_ROOT,
                new JsonSerializer<>(ProjectInstance.class));
        for (ProjectInstance project : projects) {
            val indexPlanResources = resourceStore
                    .getAllResources("/" + project.getName() + ResourceStore.INDEX_PLAN_RESOURCE_ROOT);
            for (RawResource resource : indexPlanResources) {
                val newIndexPlan = convertIndexPlan(resource);
            }

            val recommendationResources = resourceStore
                    .getAllResources("/" + project.getName() + ResourceStore.MODEL_OPTIMIZE_RECOMMENDATION);
            for (RawResource resource : recommendationResources) {
                val newRecommendation = convertRecommendation(resource);
                try (val statement = context.getConnection().prepareStatement(String
                        .format("update %s set META_TABLE_CONTENT = ? where META_TABLE_KEY = ?", metadataTable))) {
                    statement.setBytes(1, newRecommendation.read());
                    statement.setString(2, resource.getResPath());
                    statement.execute();
                }
            }
        }
    }

    private ByteSource convertIndexPlan(RawResource outdated) throws IOException {
        //        val rootNode = JsonUtil.readValueAsTree(outdated.getByteSource().openStream());
        //        rootNode.at("/indexes").iterator().
        return outdated.getByteSource();
    }

    private static final String INDEX_RECOMMENDATIONS_KEY = "index_recommendations";
    private static final String LAYOUT_RECOMMENDATIONS_KEY = "layout_recommendations";

    private ByteSource convertRecommendation(RawResource outdated) throws IOException {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        val rootNode = JsonUtil.readValue(outdated.getByteSource().openStream(), typeRef);
        val copy = JsonUtil.readValue(outdated.getByteSource().openStream(), typeRef);
        val indexRecomms = (List<Object>) rootNode.getOrDefault(INDEX_RECOMMENDATIONS_KEY, Lists.newArrayList());
        val layoutRecomms = (List<Object>) rootNode.getOrDefault(LAYOUT_RECOMMENDATIONS_KEY, Lists.newArrayList());
        if (CollectionUtils.isEmpty(indexRecomms)) {
            return outdated.getByteSource();
        }
        if (CollectionUtils.isNotEmpty(indexRecomms) && CollectionUtils.isNotEmpty(layoutRecomms)) {
            copy.remove(INDEX_RECOMMENDATIONS_KEY);
        } else {
            List<Object> newRecomms = Lists.newArrayList();
            long nextLayoutId = 1L;
            for (Object indexItem : indexRecomms) {
                Map<String, Object> itemMap = (Map<String, Object>) indexItem;
                List<Object> layouts = Lists.newArrayList();
                for (Object layout : (List<Object>) ((Map<String, Object>) itemMap.get("index_entity"))
                        .get("layouts")) {
                    Map<String, Object> layoutItem = Maps.newHashMap();
                    layoutItem.put("layout_entity", layout);
                    layoutItem.put("is_agg_index", itemMap.get("is_agg_index"));
                    layoutItem.put("is_add", itemMap.get("is_add"));
                    layoutItem.put("create_time", System.currentTimeMillis());
                    layoutItem.put("item_id", nextLayoutId);
                    layouts.add(layoutItem);
                    nextLayoutId++;
                }
                newRecomms.addAll(layouts);
            }
            copy.put(LAYOUT_RECOMMENDATIONS_KEY, newRecomms);
            copy.put("next_layout_recommendation_item_id", nextLayoutId);
            copy.remove("next_index_recommendation_item_id");
            copy.remove(INDEX_RECOMMENDATIONS_KEY);
        }
        return ByteStreams.asByteSource(JsonUtil.writeValueAsIndentBytes(copy));
    }

}
