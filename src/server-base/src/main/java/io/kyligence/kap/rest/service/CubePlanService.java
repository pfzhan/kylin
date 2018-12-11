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

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
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
@Service("cubePlanService")
public class CubePlanService extends BasicService {

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpater;

    @Transaction(project = 0)
    public NCubePlan updateRuleBasedCuboid(String project, final UpdateRuleBasedCuboidRequest request) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = getCubePlanManager(project);
        val modelManager = NDataModelManager.getInstance(kylinConfig, request.getProject());
        NCubePlan originCubePlan = getCubePlan(request.getProject(), request.getModel());
        val model = modelManager.getDataModelDesc(request.getModel());

        Preconditions.checkNotNull(model);

        val cubePlan = cubePlanManager.updateCubePlan(originCubePlan.getName(), copyForWrite -> {
            val newRuleBasedCuboid = new NRuleBasedCuboidsDesc();
            BeanUtils.copyProperties(request, newRuleBasedCuboid);
            copyForWrite.setNewRuleBasedCuboid(newRuleBasedCuboid);
        });
        if (request.isLoadData()) {
            semanticUpater.handleCubeUpdateRule(request.getProject(), model.getName(), cubePlan.getName());
        }
        return cubePlan;
    }

    @Transaction(project = 0)
    public void updateTableIndex(String project, CreateTableIndexRequest request) {
        removeTableIndex(project, request.getModel(), request.getId());
        createTableIndex(project, request);
    }

    @Transaction(project = 0)
    public void createTableIndex(String project, CreateTableIndexRequest request) {
        val cubePlanManager = getCubePlanManager(project);
        val eventManager = getEventManager(project);

        val cubePlan = getCubePlan(request.getProject(), request.getModel());
        NDataModel model = cubePlan.getModel();

        val newLayout = new NCuboidLayout();
        long maxCuboidId = NCuboidDesc.TABLE_INDEX_START_ID - NCuboidDesc.CUBOID_DESC_ID_STEP;
        for (NCuboidDesc cuboid : cubePlan.getAllCuboids()) {
            if (cuboid.isTableIndex()) {
                maxCuboidId = Math.max(maxCuboidId, cuboid.getId());
            }
        }
        newLayout.setId(maxCuboidId + NCuboidDesc.CUBOID_DESC_ID_STEP + 1);

        // handle remove the latest table index
        if (Objects.equals(newLayout.getId(), request.getId())) {
            newLayout.setId(newLayout.getId() + NCuboidDesc.CUBOID_DESC_ID_STEP);
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
        if (request.getLayoutOverrideIndices() != null) {
            for (Map.Entry<String, String> entry : request.getLayoutOverrideIndices().entrySet()) {
                layoutOverride.put(model.getColumnIdByColumnName(entry.getKey()), entry.getValue());
            }
        }
        newLayout.setLayoutOverrideIndices(layoutOverride);
        for (NCuboidLayout cuboidLayout : cubePlan.getAllCuboidLayouts()) {
            if (cuboidLayout.equals(newLayout) && cuboidLayout.isManual()) {
                throw new IllegalStateException("Already exists same layout");

            }
        }

        int layoutIndex = cubePlan.getWhitelistCuboidLayouts().indexOf(newLayout);
        if (layoutIndex != -1) {
            cubePlanManager.updateCubePlan(cubePlan.getName(), copyForWrite -> {
                val oldLayout = copyForWrite.getWhitelistCuboidLayouts().get(layoutIndex);
                oldLayout.setManual(true);
                oldLayout.setName(request.getName());
                oldLayout.setOwner(getUsername());
                oldLayout.setUpdateTime(System.currentTimeMillis());
            });
        } else {
            cubePlanManager.updateCubePlan(cubePlan.getName(), copyForWrite -> {
                val newCuboid = new NCuboidDesc();
                newCuboid.setId(newLayout.getId() - 1);
                newCuboid.setDimensions(Lists.newArrayList(newLayout.getColOrder()));
                newCuboid.setLayouts(Arrays.asList(newLayout));
                newCuboid.setCubePlan(copyForWrite);
                copyForWrite.getCuboids().add(newCuboid);
            });
            if (request.isLoadData()) {
                val addEvent = new AddCuboidEvent();
                addEvent.setProject(request.getProject());
                addEvent.setModelName(cubePlan.getModelName());
                addEvent.setCubePlanName(cubePlan.getName());
                addEvent.setOwner(getUsername());
                addEvent.setJobId(UUID.randomUUID().toString());
                eventManager.post(addEvent);

                val postAddEvent = new PostAddCuboidEvent();
                postAddEvent.setProject(request.getProject());
                postAddEvent.setModelName(cubePlan.getModelName());
                postAddEvent.setCubePlanName(cubePlan.getName());
                postAddEvent.setJobId(addEvent.getJobId());
                postAddEvent.setOwner(getUsername());
                eventManager.post(postAddEvent);
            }
        }
    }

    @Transaction(project = 0)
    public void removeTableIndex(String project, String model, final long id) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, project);

        val cubePlan = getCubePlan(project, model);
        Preconditions.checkState(cubePlan != null);
        if (id < NCuboidDesc.TABLE_INDEX_START_ID) {
            throw new IllegalStateException("Table Index Id should large than " + NCuboidDesc.TABLE_INDEX_START_ID);
        }
        val layout = cubePlan.getCuboidLayout(id);
        Preconditions.checkNotNull(layout);
        Preconditions.checkState(layout.isManual());

        val savedCubePlan = cubePlanManager.updateCubePlan(cubePlan.getName(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(id), NCuboidLayout::equals, false, true);
        });
        if (savedCubePlan.getCuboidLayout(id) != null) {
            return;
        }
        handleRemoveLayout(project, cubePlan.getName(), Sets.newHashSet(id), true, false);
    }

    public void handleRemoveLayout(String project, String cubePlanName, Set<Long> layoutIds, boolean includeAuto,
            boolean includeManual) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        val df = dfMgr.getDataflow(cubePlanName);
        val cpMgr = NCubePlanManager.getInstance(kylinConfig, project);
        cpMgr.updateCubePlan(cubePlanName, copyForWrite -> copyForWrite.removeLayouts(layoutIds, NCuboidLayout::equals,
                includeAuto, includeManual));
        dfMgr.removeLayouts(df, layoutIds);
    }

    public List<TableIndexResponse> getTableIndexs(String project, String model) {
        val cubePlan = getCubePlan(project, model);
        Preconditions.checkState(cubePlan != null);
        List<TableIndexResponse> result = Lists.newArrayList();
        for (NCuboidLayout cuboidLayout : cubePlan.getAllCuboidLayouts()) {
            if (cuboidLayout.getId() >= NCuboidDesc.TABLE_INDEX_START_ID) {
                result.add(convertToResponse(cuboidLayout, cubePlan.getModel()));
            }
        }
        return result;
    }

    public NRuleBasedCuboidsDesc getRule(String project, String model) {
        val cubePlan = getCubePlan(project, model);
        Preconditions.checkState(cubePlan != null);
        if (cubePlan.getRuleBasedCuboidsDesc() != null
                && cubePlan.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid() != null) {
            return cubePlan.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid();
        }
        return cubePlan.getRuleBasedCuboidsDesc();
    }

    private TableIndexResponse convertToResponse(NCuboidLayout cuboidLayout, NDataModel model) {
        val response = new TableIndexResponse();
        BeanUtils.copyProperties(cuboidLayout, response);
        response.setColOrder(convertColumnIdName(cuboidLayout.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(cuboidLayout.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(cuboidLayout.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getName());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(cuboidLayout.getCuboidDesc().getCubePlan().getName());
        TableIndexResponse.Status status = TableIndexResponse.Status.AVAILABLE;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getCuboid(cuboidLayout.getId());
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

    private NCubePlan getCubePlan(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, project);
        return cubePlanManager.findMatchingCubePlan(model, project, kylinConfig);
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
