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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.TableIndexResponse;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.CubePlanUpdateEvent;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import lombok.val;

@Service("cubePlanService")
public class CubePlanService extends BasicService {

    public NCubePlan updateRuleBasedCuboid(final UpdateRuleBasedCuboidRequest request)
            throws IOException, PersistentException {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, request.getProject());
        val eventManager = EventManager.getInstance(kylinConfig, request.getProject());

        NCubePlan originCubePlan = getCubePlan(request.getProject(), request.getModel());

        if (originCubePlan == null) {
            originCubePlan = new NCubePlan();
            originCubePlan.setProject(request.getProject());
            originCubePlan.setUuid(UUID.randomUUID().toString());
            originCubePlan.setModelName(request.getModel());
            originCubePlan.setName(request.getModel() + "_cube");
            originCubePlan.setModelName(request.getModel());
            originCubePlan = cubePlanManager.createCubePlan(originCubePlan);
        }

        val cubePlan = cubePlanManager.updateCubePlan(originCubePlan.getName(),
                new NCubePlanManager.NCubePlanUpdater() {
                    @Override
                    public void modify(NCubePlan copyForWrite) {
                        val newRuleBasedCuboid = new NRuleBasedCuboidsDesc();
                        BeanUtils.copyProperties(request, newRuleBasedCuboid);
                        newRuleBasedCuboid.setCubePlan(copyForWrite);
                        newRuleBasedCuboid.init();
                        if (copyForWrite.getRuleBasedCuboidsDesc() == null) {
                            copyForWrite.setRuleBasedCuboidsDesc(new NRuleBasedCuboidsDesc());
                        }
                        copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRuleBasedCuboid);
                    }
                });
        val event = new CubePlanUpdateEvent();
        event.setApproved(true);
        event.setProject(request.getProject());
        event.setCubePlanName(cubePlan.getName());
        event.setModelName(cubePlan.getModelName());
        eventManager.post(event);
        return cubePlan;
    }

    public void updateTableIndex(CreateTableIndexRequest request) throws PersistentException, IOException {
        removeTableIndex(request.getProject(), request.getModel(), request.getId());
        createTableIndex(request);
    }

    public void createTableIndex(CreateTableIndexRequest request) throws PersistentException, IOException {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, request.getProject());
        val modelManager = NDataModelManager.getInstance(kylinConfig, request.getProject());
        val eventManager = EventManager.getInstance(kylinConfig, request.getProject());

        val cubePlan = getCubePlan(request.getProject(), request.getModel());
        NDataModel model = cubePlan.getModel();

        int maxId = -1;
        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            maxId = Math.max(maxId, column.id);
        }
        val newColumns = Lists.<NDataModel.NamedColumn> newArrayList();
        for (String col : request.getColOrder()) {
            if (model.getColumnIdByColumnName(col) == -1) {
                val newCol = new NDataModel.NamedColumn();
                newCol.id = maxId + 1;
                newCol.name = col.split("\\.")[1];
                newCol.aliasDotColumn = col;
                newColumns.add(newCol);
                maxId++;
            }
        }
        if (CollectionUtils.isNotEmpty(newColumns)) {
            model.getAllNamedColumns().addAll(newColumns);
            model = modelManager.updateDataModelDesc(model);
        }

        val newLayout = new NCuboidLayout();
        long maxCuboidId = NCuboidDesc.MANUAL_TABLE_INDEX_START_ID - NCubePlanManager.CUBOID_DESC_ID_STEP;
        for (NCuboidDesc cuboid : cubePlan.getAllCuboids()) {
            if (cuboid.isTableIndex()) {
                maxCuboidId = Math.max(maxCuboidId, cuboid.getId());
            }
        }
        newLayout.setId(maxCuboidId + NCubePlanManager.CUBOID_DESC_ID_STEP + 1);

        // handle remove the latest table index
        if (Objects.equals(newLayout.getId(), request.getId())) {
            newLayout.setId(newLayout.getId() + NCubePlanManager.CUBOID_DESC_ID_STEP);
        }
        newLayout.setName(request.getName());
        newLayout.setColOrder(convertColumn(request.getColOrder(), model));
        newLayout.setStorageType(request.getStorageType());
        newLayout.setShardByColumns(convertColumn(request.getShardByColumns(), model));
        newLayout.setSortByColumns(convertColumn(request.getSortByColumns(), model));
        newLayout.setUpdateTime(System.currentTimeMillis());
        newLayout.setOwner("ADMIN");
        Map<Integer, String> layoutOverride = Maps.newHashMap();
        if (request.getLayoutOverrideIndices() != null) {
            for (Map.Entry<String, String> entry : request.getLayoutOverrideIndices().entrySet()) {
                layoutOverride.put(model.getColumnIdByColumnName(entry.getKey()), entry.getValue());
            }
        }
        newLayout.setLayoutOverrideIndices(layoutOverride);
        for (NCuboidLayout cuboidLayout : cubePlan.getAllCuboidLayouts()) {
            if (cuboidLayout.equals(newLayout)) {
                throw new IllegalStateException("Already exists same layout");

            }
        }
        cubePlanManager.updateCubePlan(cubePlan.getName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val newCuboid = new NCuboidDesc();
                newCuboid.setId(newLayout.getId() - 1);
                newCuboid.setDimensions(Lists.newArrayList(newLayout.getColOrder()));
                newCuboid.setLayouts(Arrays.asList(newLayout));
                newCuboid.setCubePlan(copyForWrite);
                copyForWrite.getCuboids().add(newCuboid);
            }
        });
        val addEvent = new AddCuboidEvent();
        addEvent.setProject(request.getProject());
        addEvent.setModelName(cubePlan.getModelName());
        addEvent.setCubePlanName(cubePlan.getName());
        addEvent.setLayoutIds(Arrays.asList(newLayout.getId()));
        eventManager.post(addEvent);
    }

    public void removeTableIndex(String project, String model, final long id) throws PersistentException, IOException {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, project);
        val eventManager = EventManager.getInstance(kylinConfig, project);

        val cubePlan = getCubePlan(project, model);
        Preconditions.checkNotNull(cubePlan);
        if (id < NCuboidDesc.MANUAL_TABLE_INDEX_START_ID) {
            throw new IllegalStateException(
                    "Table Index Id should large than " + NCuboidDesc.MANUAL_TABLE_INDEX_START_ID);
        }
        val layout = cubePlan.getCuboidLayout(id);
        Preconditions.checkNotNull(layout);

        cubePlanManager.updateCubePlan(cubePlan.getName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                cubePlanManager.removeLayouts(copyForWrite, Sets.newHashSet(id),
                        new Predicate2<NCuboidLayout, NCuboidLayout>() {
                            @Override
                            public boolean apply(NCuboidLayout o1, NCuboidLayout o2) {
                                return o1.equals(o2);
                            }
                        });
            }
        });
        val removeEvent = new RemoveCuboidEvent();
        removeEvent.setLayoutIds(Arrays.asList(id));
        removeEvent.setProject(project);
        removeEvent.setModelName(cubePlan.getModelName());
        removeEvent.setCubePlanName(cubePlan.getName());
        eventManager.post(removeEvent);
    }

    public List<TableIndexResponse> getTableIndexs(String project, String model) {
        val cubePlan = getCubePlan(project, model);
        Preconditions.checkNotNull(cubePlan);
        List<TableIndexResponse> result = Lists.newArrayList();
        for (NCuboidLayout cuboidLayout : cubePlan.getAllCuboidLayouts()) {
            if (cuboidLayout.getId() >= NCuboidDesc.MANUAL_TABLE_INDEX_START_ID) {
                result.add(convertToResponse(cuboidLayout, cubePlan.getModel()));
            }
        }
        return result;
    }

    private TableIndexResponse convertToResponse(NCuboidLayout cuboidLayout, NDataModel model) {
        val response = new TableIndexResponse();
        response.setColOrder(convertColumnIdName(cuboidLayout.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(cuboidLayout.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(cuboidLayout.getSortByColumns(), model));
        response.setName(cuboidLayout.getName());
        response.setId(cuboidLayout.getId());
        response.setOwner(cuboidLayout.getOwner());
        response.setProject(model.getProject());
        response.setModel(model.getName());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(cuboidLayout.getCuboidDesc().getCubePlan().getName());
        TableIndexResponse.Status status = TableIndexResponse.Status.Available;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getCuboid(cuboidLayout.getId());
            if (dataCuboid == null) {
                continue;
            }
            val segmentStatus = dataCuboid.getStatus();
            if (segmentStatus == SegmentStatusEnum.NEW || segmentStatus == SegmentStatusEnum.READY_PENDING) {
                status = TableIndexResponse.Status.Empty;
                break;
            }
            readyCount++;
        }
        if (readyCount != dataflow.getSegments().size() || CollectionUtils.isEmpty(dataflow.getSegments())) {
            status = TableIndexResponse.Status.Empty;
        }
        response.setStatus(status);
        return response;
    }

    private NCubePlan getCubePlan(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, project);
        val result = cubePlanManager.findMatchingCubePlan(model, project, kylinConfig);
        return CollectionUtils.isEmpty(result) ? null : result.get(0);
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
