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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.ModifiedOrder;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final Message msg = MsgPicker.getMsg();

    private static final String LAST_MODIFY = "last_modify";

    public static final char[] VALID_MODELNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    public List<NDataModelResponse> getModels(final String modelName, final String projectName, boolean exactMatch,
            String owner, String status, String sortBy, boolean reverse) {

        List<NDataModel> models = getDataModelManager(projectName).getDataModels();
        List<NDataModelResponse> filterModels = new ArrayList<NDataModelResponse>();
        for (NDataModel modelDesc : models) {
            boolean isModelNameMatch = StringUtils.isEmpty(modelName)
                    || (exactMatch && modelDesc.getAlias().toLowerCase().equals(modelName.toLowerCase()))
                    || (!exactMatch && modelDesc.getAlias().toLowerCase().contains(modelName.toLowerCase()));
            boolean isModelOwnerMatch = StringUtils.isEmpty(owner)
                    || (exactMatch && modelDesc.getOwner().toLowerCase().equals(owner.toLowerCase()))
                    || (!exactMatch && modelDesc.getOwner().toLowerCase().contains(owner.toLowerCase()));
            if (isModelNameMatch && isModelOwnerMatch) {
                RealizationStatusEnum modelStatus = getModelStatus(modelDesc.getName(), projectName);
                boolean isModelStatusMatch = StringUtils.isEmpty(status)
                        || (modelStatus.name().toLowerCase().equals(status.toLowerCase()));

                if (isModelStatusMatch) {
                    NDataModelResponse nDataModelResponse = new NDataModelResponse(modelDesc);
                    nDataModelResponse.setStatus(modelStatus);
                    filterModels.add(nDataModelResponse);
                }
            }
        }
        if (sortBy.equals(LAST_MODIFY) && reverse) {
            Collections.sort(filterModels, new ModifiedOrder());
        } else if (sortBy.equals(LAST_MODIFY) && !reverse) {
            Collections.sort(filterModels, new ModifiedOrder());
            Collections.reverse(filterModels);

        }
        return filterModels;
    }

    private RealizationStatusEnum getModelStatus(String modelName, String projectName) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, projectName);
        if (CollectionUtils.isNotEmpty(cubePlans)) {
            return getDataflowManager(projectName).getDataflow(cubePlans.get(0).getName()).getStatus();
        } else {
            throw new IllegalStateException("No cubePlans exists in " + modelName);
        }
    }

    public Segments<NDataSegment> getSegments(String modelName, String project, String start, String end) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        SegmentRange filterRange = getSegmentRangeByModel(project, modelName, start, end);
        Segments<NDataSegment> segments = new Segments<NDataSegment>();
        for (NCubePlan cubeplan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubeplan.getName());
            for (NDataSegment segment : dataflow.getSegments()) {
                if (segment.getSegRange().overlaps(filterRange)) {
                    long segmentSize = dataflowManager.getSegmentSize(segment);
                    NDataSegmentResponse nDataSegmentResponse = new NDataSegmentResponse(segment);
                    nDataSegmentResponse.setSizeKB(segmentSize);
                    segments.add(nDataSegmentResponse);
                }

            }
        }

        return segments;
    }

    public List<CuboidDescResponse> getAggIndices(String modelName, String project) {
        List<NCuboidDesc> cuboidDescs = getCuboidDescs(modelName, project);
        List<CuboidDescResponse> result = new ArrayList<CuboidDescResponse>();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            if (cuboidDesc.getId() < NCuboidDesc.TABLE_INDEX_START_ID) {
                CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
                result.add(cuboidDescResponse);
            }
        }
        return result;
    }

    public List<CuboidDescResponse> getTableIndices(String modelName, String project) {
        List<NCuboidDesc> cuboidDescs = getCuboidDescs(modelName, project);
        List<CuboidDescResponse> result = new ArrayList<CuboidDescResponse>();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            if (cuboidDesc.getId() >= NCuboidDesc.TABLE_INDEX_START_ID) {
                CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
                result.add(cuboidDescResponse);
            }
        }
        return result;
    }

    public List<NCuboidDesc> getCuboidDescs(String modelName, String project) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        List<NCuboidDesc> cuboidDescs = new ArrayList<NCuboidDesc>();
        for (NCubePlan cubeplan : cubePlans) {
            cuboidDescs.addAll(cubeplan.getCuboids());
        }
        return cuboidDescs;
    }

    public CuboidDescResponse getCuboidById(String modelName, String project, Long cuboidId) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        NCuboidDesc cuboidDesc = null;
        for (NCubePlan cubeplan : cubePlans) {
            cuboidDesc = cubeplan.getCuboidDesc(cuboidId);
            break;
        }
        NDataflow dataflow = getDataflowManager(project).getDataflow(cuboidDesc.getCubePlan().getName());
        Segments<NDataSegment> segments = dataflow.getSegments();
        List<NCuboidLayout> layouts = cuboidDesc.getLayouts();
        long storage = 0L;
        long startTime = Long.MAX_VALUE;
        long endTime = 0L;
        for (NDataSegment segment : segments) {
            for (NCuboidLayout layout : layouts) {
                NDataCuboid cuboid = segment.getCuboid(layout.getId());
                if (cuboid != null) {
                    storage += cuboid.getSizeKB();
                }
            }
            long start = Long.parseLong(segment.getSegRange().getStart().toString());
            long end = Long.parseLong(segment.getSegRange().getEnd().toString());
            startTime = startTime < start ? startTime : start;
            endTime = endTime > end ? endTime : end;
        }
        CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
        cuboidDescResponse.setStartTime(startTime);
        cuboidDescResponse.setEndTime(endTime);
        cuboidDescResponse.setStorageSize(storage);
        return cuboidDescResponse;
    }

    public String getModelJson(String modelName, String project) throws JsonProcessingException {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelName);
        return JsonUtil.writeValueAsIndentString(modelDesc);
    }

    public List<NForestSpanningTree> getModelRelations(String modelName, String project) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        List<NForestSpanningTree> result = new ArrayList<NForestSpanningTree>();
        if (cubePlans == null) {
            return result;
        }
        for (NCubePlan cubeplan : cubePlans) {
            NSpanningTree spanningTree = cubeplan.getSpanningTree();
            NForestSpanningTree nForestSpanningTree = new NForestSpanningTree(spanningTree.getCuboids(),
                    spanningTree.getCuboidCacheKey());
            result.add(nForestSpanningTree);
        }
        return result;
    }

    public List<NDataModelResponse> getRelateModels(String project, String table) throws IOException {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<String> models = dataModelManager.getModelsUsingTable(tableDesc);
        List<NDataModelResponse> dataModels = new ArrayList<NDataModelResponse>();
        for (String model : models) {
            Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
            NDataModel dataModelDesc = dataModelManager.getDataModelDesc(model);
            NDataModelResponse nDataSegmentResponse = new NDataModelResponse(dataModelDesc);
            Segments<NDataSegment> segments = getSegments(model, project, "", "");
            for (NDataSegment segment : segments) {
                segmentRanges.put(segment.getSegRange(), segment.getStatus());
            }
            nDataSegmentResponse.setSegmentRanges(segmentRanges);
            dataModels.add(nDataSegmentResponse);
        }
        return dataModels;
    }

    private List<NCubePlan> getCubePlans(String modelName, String project) {
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        List<NCubePlan> cubePlans = cubePlanManager.findMatchingCubePlan(modelName, project,
                KylinConfig.getInstanceFromEnv());
        return cubePlans;
    }

    private void checkAliasExist(String modelName, String newAlias, String project) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<NDataModel> models = dataModelManager.listModels();
        for (NDataModel model : models) {
            if (!StringUtils.isNotEmpty(modelName) && model.getName().equals(modelName)) {
                continue;
            } else if (model.getAlias().equals(newAlias)) {
                throw new BadRequestException("model alias " + newAlias + " already exists");
            }
        }
    }

    public void dropModel(String model, String project) throws IOException {
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel dataModelDesc = dataModelManager.getDataModelDesc(model);
        if (null == dataModelDesc) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), model));
        }
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        List<NCubePlan> cubePlans = getCubePlans(model, project);
        for (NCubePlan cubePlan : cubePlans) {
            Segments<NDataSegment> segments = dataflowManager.getDataflow(cubePlan.getName()).getSegments();
            if (CollectionUtils.isNotEmpty(segments)) {
                throw new IllegalStateException("You should purge your model first before you delete it");
            }
        }
        for (NCubePlan cubePlan : cubePlans) {
            cubePlanManager.removeCubePlan(cubePlan);
            dataflowManager.dropDataflow(cubePlan.getName());
        }

        getDataModelManager(project).dropModel(dataModelDesc);
    }

    public void purgeModel(String model, String project) throws IOException {
        NDataModel dataModelDesc = getDataModelManager(project).getDataModelDesc(model);
        if (null == dataModelDesc) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), model));
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        List<NCubePlan> cubePlans = getCubePlans(model, project);
        List<NDataSegment> segments = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(cubePlans)) {
            for (NCubePlan cubePlan : cubePlans) {
                NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
                NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
                nDataflowUpdate.setStatus(RealizationStatusEnum.DISABLED);
                segments.addAll(dataflow.getSegments());
                NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
                NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
                nDataflowUpdate.setToRemoveSegs(nDataSegments);
                dataflowManager.updateDataflow(nDataflowUpdate);
            }
        }

    }

    public void cloneModel(String modelName, String newModelName, String project) throws IOException {
        checkAliasExist("", newModelName, project);
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel dataModelDesc = dataModelManager.getDataModelDesc(modelName);
        if (null == dataModelDesc) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        //copyForWrite nDataModel do init,but can not set new modelname
        NDataModel nDataModel = JsonUtil.readValue(JsonUtil.writeValueAsIndentString(dataModelDesc), NDataModel.class);
        nDataModel.setName(UUID.randomUUID().toString());
        nDataModel.setAlias(newModelName);
        nDataModel.setLastModified(0L);
        dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
        cloneCubePlan(modelName, nDataModel.getName(), project, nDataModel.getOwner());
    }

    private void cloneCubePlan(String modelName, String newModelName, String project, String owner) throws IOException {
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        List<NCubePlan> cubePlans = cubePlanManager.findMatchingCubePlan(modelName, project, getConfig());
        NDataflowManager dataflowManager = getDataflowManager(project);
        for (NCubePlan cubePlan : cubePlans) {
            NCubePlan copy = cubePlanManager.copy(cubePlan);
            copy.setModelName(newModelName);
            copy.setSegmentRangeStart(0L);
            copy.setSegmentRangeEnd(0L);
            copy.updateRandomUuid();
            copy.setName(copy.getUuid());
            copy.setLastModified(0L);
            cubePlanManager.createCubePlan(copy);
            NDataflow nDataflow = new NDataflow();
            nDataflow.setStatus(RealizationStatusEnum.DISABLED);
            nDataflow.setProject(project);
            nDataflow.setCubePlanName(cubePlan.getName());
            dataflowManager.createDataflow(copy.getName(), project, copy, owner);
        }
    }

    public void renameDataModel(String project, String modelName, String newAlias) throws IOException {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelName);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        //rename
        checkAliasExist(modelName, newAlias, project);
        nDataModel.setAlias(newAlias);
        NDataModel modelUpdate = modelManager.copyForWrite(nDataModel);
        modelManager.updateDataModelDesc(modelUpdate);
    }

    public void updateDataModelStatus(String modelName, String project, String status) throws IOException {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelName);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        List<NCubePlan> cubePlans = getCubePlans(nDataModel.getName(), project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        for (NCubePlan cubePlan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            boolean needChangeStatus = (status.equals(RealizationStatusEnum.DISABLED.name())
                    && dataflow.getStatus().equals(RealizationStatusEnum.READY))
                    || (status.equals(RealizationStatusEnum.READY.name())
                    && dataflow.getStatus().equals(RealizationStatusEnum.DISABLED));
            if (dataflow.getStatus().equals(RealizationStatusEnum.DESCBROKEN)
                    && !status.equals(RealizationStatusEnum.DESCBROKEN.name())) {
                throw new BadRequestException(
                        "DescBroken model " + nDataModel.getName() + "cannot to set disable or enable");
            }
            if (needChangeStatus) {
                NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
                nDataflowUpdate.setStatus(RealizationStatusEnum.valueOf(status));
                dataflowManager.updateDataflow(nDataflowUpdate);
            }
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelName, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelName).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    public boolean isModelsUsingTable(String table, String project) throws IOException {
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table)).size() > 0;
    }

    public List<String> getModelsUsingTable(String table, String project) throws IOException {
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table));
    }
}
