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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.TableIndexResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("indexPlanService")
public class indexPlanService extends BasicService {

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpater;

    @Transaction(project = 0)
    public IndexPlan updateRuleBasedCuboid(String project, final UpdateRuleBasedCuboidRequest request) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = getIndexPlanManager(project);
        val modelManager = NDataModelManager.getInstance(kylinConfig, request.getProject());
        IndexPlan originIndexPlan = getIndexPlan(request.getProject(), request.getModelId());
        val model = modelManager.getDataModelDesc(request.getModelId());

        Preconditions.checkNotNull(model);

        val indexPlan = indexPlanManager.updateIndexPlan(originIndexPlan.getUuid(), copyForWrite -> {
            val newRuleBasedCuboid = new NRuleBasedIndex();
            BeanUtils.copyProperties(request, newRuleBasedCuboid);
            copyForWrite.setRuleBasedIndex(newRuleBasedCuboid);
        });
        if (request.isLoadData()) {
            semanticUpater.handleCubeUpdateRule(request.getProject(), model.getUuid(),
                    originIndexPlan.getRuleBasedIndex(), indexPlan.getRuleBasedIndex());
        }
        return indexPlanManager.getIndexPlan(originIndexPlan.getUuid());
    }

    @Transaction(project = 0)
    public void updateTableIndex(String project, CreateTableIndexRequest request) {
        removeTableIndex(project, request.getModelId(), request.getId());
        createTableIndex(project, request);
    }

    @Transaction(project = 0)
    public void createTableIndex(String project, CreateTableIndexRequest request) {
        val indexPlanManager = getIndexPlanManager(project);
        val eventManager = getEventManager(project);

        val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
        NDataModel model = indexPlan.getModel();

        val newLayout = new LayoutEntity();
        newLayout.setId(indexPlan.getNextTableIndexId() + 1);

        // handle remove the latest table index
        if (Objects.equals(newLayout.getId(), request.getId())) {
            newLayout.setId(newLayout.getId() + IndexEntity.INDEX_ID_STEP);
        }
        newLayout.setName(request.getName());
        newLayout.setColOrder(convertColumn(request.getColOrder(), model));
        newLayout.setStorageType(request.getStorageType());
        newLayout.setShardByColumns(convertColumn(request.getShardByColumns(), model));
        newLayout.setSortByColumns(convertColumn(request.getSortByColumns(), model));
        newLayout.setUpdateTime(System.currentTimeMillis());
        newLayout.setOwner(getUsername());
        newLayout.setManual(true);

        Map<Integer, String> layoutOverride = Maps.newHashMap();
        if (request.getLayoutOverrideIndexes() != null) {
            for (Map.Entry<String, String> entry : request.getLayoutOverrideIndexes().entrySet()) {
                layoutOverride.put(model.getColumnIdByColumnName(entry.getKey()), entry.getValue());
            }
        }
        newLayout.setLayoutOverrideIndexes(layoutOverride);
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (cuboidLayout.equals(newLayout) && cuboidLayout.isManual()) {
                throw new IllegalStateException("Already exists same layout");

            }
        }

        int layoutIndex = indexPlan.getWhitelistLayouts().indexOf(newLayout);
        if (layoutIndex != -1) {
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val oldLayout = copyForWrite.getWhitelistLayouts().get(layoutIndex);
                oldLayout.setManual(true);
                oldLayout.setName(request.getName());
                oldLayout.setOwner(getUsername());
                oldLayout.setUpdateTime(System.currentTimeMillis());
            });
        } else {
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val newCuboid = new IndexEntity();
                newCuboid.setId(newLayout.getId() - 1);
                newCuboid.setDimensions(Lists.newArrayList(newLayout.getColOrder()));
                newCuboid.setLayouts(Arrays.asList(newLayout));
                newCuboid.setIndexPlan(copyForWrite);
                copyForWrite.getIndexes().add(newCuboid);
            });
            if (request.isLoadData()) {
                val addEvent = new AddCuboidEvent();
                addEvent.setModelId(indexPlan.getUuid());
                addEvent.setOwner(getUsername());
                addEvent.setJobId(UUID.randomUUID().toString());
                eventManager.post(addEvent);

                val postAddEvent = new PostAddCuboidEvent();
                postAddEvent.setModelId(indexPlan.getUuid());
                postAddEvent.setJobId(addEvent.getJobId());
                postAddEvent.setOwner(getUsername());
                eventManager.post(postAddEvent);
            }
        }
    }

    @Transaction(project = 0)
    public void removeTableIndex(String project, String model, final long id) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        if (id < IndexEntity.TABLE_INDEX_START_ID) {
            throw new IllegalStateException("Table Index Id should large than " + IndexEntity.TABLE_INDEX_START_ID);
        }
        val layout = indexPlan.getCuboidLayout(id);
        Preconditions.checkNotNull(layout);
        Preconditions.checkState(layout.isManual());

        val savedIndexPlan = indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(id), LayoutEntity::equals, false, true);
        });
        if (savedIndexPlan.getCuboidLayout(id) != null) {
            return;
        }
        handleRemoveLayout(project, indexPlan.getUuid(), Sets.newHashSet(id), true, false);
    }

    public void handleRemoveLayout(String project, String modelId, Set<Long> layoutIds, boolean includeAuto,
            boolean includeManual) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        val df = dfMgr.getDataflow(modelId);
        val cpMgr = NIndexPlanManager.getInstance(kylinConfig, project);
        cpMgr.updateIndexPlan(modelId, copyForWrite -> copyForWrite.removeLayouts(layoutIds, LayoutEntity::equals,
                includeAuto, includeManual));
        dfMgr.removeLayouts(df, layoutIds);
    }

    public List<TableIndexResponse> getTableIndexs(String project, String model) {
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        List<TableIndexResponse> result = Lists.newArrayList();
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (cuboidLayout.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
                result.add(convertToResponse(cuboidLayout, indexPlan.getModel()));
            }
        }
        return result;
    }

    public NRuleBasedIndex getRule(String project, String model) {
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        return indexPlan.getRuleBasedIndex();
    }

    private TableIndexResponse convertToResponse(LayoutEntity cuboidLayout, NDataModel model) {
        val response = new TableIndexResponse();
        BeanUtils.copyProperties(cuboidLayout, response);
        response.setColOrder(convertColumnIdName(cuboidLayout.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(cuboidLayout.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(cuboidLayout.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getUuid());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(cuboidLayout.getIndex().getIndexPlan().getUuid());
        TableIndexResponse.Status status = TableIndexResponse.Status.AVAILABLE;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getLayout(cuboidLayout.getId());
            if (dataCuboid == null) {
                continue;
            }
            readyCount++;
        }
        if (readyCount != dataflow.getSegments().size() || CollectionUtils.isEmpty(dataflow.getSegments())) {
            status = TableIndexResponse.Status.EMPTY;
        }
        response.setStatus(status);
        response.setUpdateTime(cuboidLayout.getUpdateTime());
        return response;
    }

    private IndexPlan getIndexPlan(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
        return indexPlanManager.getIndexPlan(model);
    }

    private List<String> convertColumnIdName(List<Integer> ids, NDataModel model) {
        if (CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }
        val result = Lists.<String> newArrayList();
        for (Integer column : ids) {
            val name = model.getColumnNameByColumnId(column);
            result.add(name);
        }
        return result;

    }

    private List<Integer> convertColumn(List<String> columns, NDataModel model) {
        if (CollectionUtils.isEmpty(columns)) {
            return Lists.newArrayList();
        }
        val result = Lists.<Integer> newArrayList();
        for (String column : columns) {
            val id = model.getColumnIdByColumnName(column);
            result.add(id);
        }
        return result;
    }
}
