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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.LoadingRangeRefreshEvent;
import io.kyligence.kap.event.model.ModelSemanticUpdateEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.event.model.RemoveSegmentEvent;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.ModelStatus;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelFlatTableDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.request.ModelCanvasUpdateRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelSemanticUpdateRequest;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SimplifiedColumnResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasureResponse;
import io.kyligence.kap.rest.response.SimplifiedTableResponse;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import lombok.var;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.ModifiedOrder;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.KeywordDefaultDirtyHack;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.adhocquery.PushDownConverterKeyWords;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.CubePlanStatus;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;

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
                    NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
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

    private NDataModelResponse enrichModelResponse(NDataModel modelDesc, String projectName) {
        NDataModelResponse nDataModelResponse = new NDataModelResponse(modelDesc);
        List<SimplifiedTableResponse> simpleTables = new ArrayList<>();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            SimplifiedTableResponse simpleTable = new SimplifiedTableResponse();
            simpleTable.setTable(tableRef.getTableIdentity());
            List<SimplifiedColumnResponse> columns = getSimplifiedColumns(projectName, tableRef);
            simpleTable.setColumns(columns);
            simpleTables.add(simpleTable);
        }
        nDataModelResponse.setSimpleTables(simpleTables);
        List<SimplifiedMeasureResponse> simplifiedMeasures = getSimplifiedMeasures(modelDesc);
        nDataModelResponse.setSimplifiedMeasures(simplifiedMeasures);
        if (modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            Segments<NDataSegment> segments = getSegments(modelDesc.getName(), projectName, "0", "" + Long.MAX_VALUE);
            if (CollectionUtils.isNotEmpty(segments)) {
                NDataSegment lastSegment = segments.get(segments.size() - 1);
                nDataModelResponse.setLastBuildEnd(lastSegment.getSegRange().getEnd().toString());
            } else {
                nDataModelResponse.setLastBuildEnd("");

            }
        }
        return nDataModelResponse;
    }

    private List<SimplifiedMeasureResponse> getSimplifiedMeasures(NDataModel model) {
        List<NDataModel.Measure> measures = model.getAllMeasures();
        List<SimplifiedMeasureResponse> measureResponses = new ArrayList<>();
        for (NDataModel.Measure measure : measures) {
            SimplifiedMeasureResponse measureResponse = new SimplifiedMeasureResponse();
            measureResponse.setId(measure.id);
            measureResponse.setName(measure.getName());
            measureResponse.setExpression(measure.getFunction().getExpression());
            measureResponse.setReturnType(measure.getFunction().getReturnType());
            List<ParameterResponse> parameters = new ArrayList<>();
            Set<ParameterDesc.PlainParameter> plainParameters = measure.getFunction().getParameter().getPlainParameters();
            for (ParameterDesc.PlainParameter plainParameter : plainParameters) {
                ParameterResponse parameterResponse = new ParameterResponse();
                parameterResponse.setType(plainParameter.getType());
                parameterResponse.setValue(plainParameter.getValue());
                parameters.add(parameterResponse);
            }
            measureResponse.setParameterValue(parameters);
            measureResponses.add(measureResponse);
        }
        return measureResponses;
    }

    private List<SimplifiedColumnResponse> getSimplifiedColumns(String project, TableRef tableRef) {
        List<SimplifiedColumnResponse> columns = new ArrayList<>();
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        for (ColumnDesc columnDesc : tableRef.getTableDesc().getColumns()) {
            TableExtDesc tableExtDesc = tableMetadataManager.getTableExt(tableRef.getTableDesc());
            SimplifiedColumnResponse simplifiedColumnResponse = new SimplifiedColumnResponse();
            simplifiedColumnResponse.setName(columnDesc.getName());
            simplifiedColumnResponse.setComment(columnDesc.getComment());
            simplifiedColumnResponse.setDataType(columnDesc.getDatatype());
            //get column cardinality
            List<TableExtDesc.ColumnStats> columnStats = tableExtDesc.getColumnStats();
            for (TableExtDesc.ColumnStats columnStat : columnStats) {
                if (columnStat.getColumnName().equals(columnDesc.getName())) {
                    simplifiedColumnResponse.setCardinality(columnStat.getCardinality());
                }
            }
            columns.add(simplifiedColumnResponse);
        }
        return columns;
    }

    private RealizationStatusEnum getModelStatus(String modelName, String projectName) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, projectName);
        if (CollectionUtils.isNotEmpty(cubePlans)) {
            return getDataflowManager(projectName).getDataflow(cubePlans.get(0).getName()).getStatus();
        } else {
            return RealizationStatusEnum.OFFLINE;
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
                    nDataSegmentResponse.setBytesSize(segmentSize);
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
            cuboidDescs.addAll(cubeplan.getAllCuboids());
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
                    storage += cuboid.getByteSize();
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

    public List<RelatedModelResponse> getRelateModels(String project, String table, String modelName) throws IOException {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<String> models = dataModelManager.getModelsUsingRootTable(tableDesc);
        List<RelatedModelResponse> relatedModel = new ArrayList<>();
        for (String model : models) {
            Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
            NDataModel dataModelDesc = dataModelManager.getDataModelDesc(model);
            if (StringUtils.isEmpty(modelName) || dataModelDesc.getAlias().toLowerCase().contains(modelName.toLowerCase())) {
                RelatedModelResponse relatedModelResponse = new RelatedModelResponse(dataModelDesc);
                Segments<NDataSegment> segments = getSegments(model, project, "", "");
                for (NDataSegment segment : segments) {
                    segmentRanges.put(segment.getSegRange(), segment.getStatus());
                }
                relatedModelResponse.setStatus(getModelStatus(model, project));
                relatedModelResponse.setSegmentRanges(segmentRanges);
                relatedModel.add(relatedModelResponse);
            }
        }
        return relatedModel;
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
                throw new BadRequestException("Model alias " + newAlias + " already exists!");
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
                throw new IllegalStateException("You should purge your model first before you drop it!");
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
                nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
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
            nDataflow.setStatus(RealizationStatusEnum.OFFLINE);
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

    public void unlinkModel(String modelName, String project) throws IOException {
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataModelManager dataModelManager = getDataModelManager(project);

        NDataModel nDataModel = dataModelManager.getDataModelDesc(modelName);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        if (nDataModel.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new IllegalStateException("Model " + modelName + " is model based, can not unlink it!");
        } else {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(nDataModel.getRootFactTable().getTableIdentity());
            NDataModel modelUpdate = dataModelManager.copyForWrite(nDataModel);
            if (dataLoadingRange != null) {
                modelUpdate.setAutoMergeTimeRanges(dataLoadingRange.getAutoMergeTimeRanges());
                modelUpdate.setAutoMergeEnabled(dataLoadingRange.isAutoMergeEnabled());
                modelUpdate.setVolatileRange(dataLoadingRange.getVolatileRange());
            }
            modelUpdate.setManagementType(ManagementType.MODEL_BASED);
            dataModelManager.updateDataModelDesc(modelUpdate);
            return;
        }
    }

    public void updateDataModelStatus(String modelName, String project, String status) throws Exception {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelName);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        List<NCubePlan> cubePlans = getCubePlans(nDataModel.getName(), project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        for (NCubePlan cubePlan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            if (dataflow.getStatus().equals(RealizationStatusEnum.NEW)) {
                throw new IllegalStateException(
                        "No ready segment in model '" + modelName + "', can not online the model!");
            }
            boolean needChangeStatus = (status.equals(RealizationStatusEnum.OFFLINE.name())
                    && dataflow.getStatus().equals(RealizationStatusEnum.ONLINE))
                    || (status.equals(RealizationStatusEnum.ONLINE.name())
                    && dataflow.getStatus().equals(RealizationStatusEnum.OFFLINE));
            if (dataflow.getStatus().equals(RealizationStatusEnum.DESCBROKEN)
                    && !status.equals(RealizationStatusEnum.DESCBROKEN.name())) {
                throw new BadRequestException(
                        "DescBroken model " + nDataModel.getName() + " can not online or offline!");
            }
            if (needChangeStatus) {
                NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
                if (status.equals(RealizationStatusEnum.OFFLINE.name())) {
                    nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
                } else if (status.equals(RealizationStatusEnum.ONLINE.name())) {
                    if (!dataflow.checkAllowedOnline()) {
                        throw new IllegalStateException(
                                "Some segments in model '" + modelName + "' are not ready, can not online the model!");
                    } else {
                        nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
                    }
                }
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
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table))
                .size() > 0;
    }

    public List<String> getModelsUsingTable(String table, String project) throws IOException {
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table));
    }

    public RefreshAffectedSegmentsResponse getAffectedSegmentsResponse(String project, String table, String start,
            String end, ManagementType managementType) throws IOException {
        Segments<NDataSegment> segments = new Segments<>();
        RefreshAffectedSegmentsResponse response = new RefreshAffectedSegmentsResponse();
        long byteSize = 0L;
        List<RelatedModelResponse> models = getRelateModels(project, table, "");
        for (NDataModel model : models) {
            if (model.getManagementType().equals(managementType)) {
                segments.addAll(getSegments(model.getName(), project, start, end));
            }
        }

        if (CollectionUtils.isEmpty(segments)) {
            throw new BadRequestException("No segments to refresh, please select new range and try again!");
        } else {
            String affectedStart = segments.getFirstSegment().getSegRange().getStart().toString();
            String affectedEnd = segments.getLatestReadySegment().getSegRange().getEnd().toString();
            for (NDataSegment segment : segments) {
                byteSize += ((NDataSegmentResponse) segment).getBytesSize();
            }
            response.setAffectedStart(affectedStart);
            response.setAffectedEnd(affectedEnd);
            response.setByteSize(byteSize);
        }
        return response;
    }

    public void refreshSegments(String project, String table, String refreshStart, String refreshEnd,
            String affectedStart, String affectedEnd) throws IOException, PersistentException {
        RefreshAffectedSegmentsResponse response = getAffectedSegmentsResponse(project, table, refreshStart, refreshEnd,
                ManagementType.TABLE_ORIENTED);
        if (!response.getAffectedStart().equals(affectedStart) || !response.getAffectedEnd().equals(affectedEnd)) {
            throw new BadRequestException("Can not refersh, please try again and confirm affected storage!");
        }
        List<RelatedModelResponse> models = getRelateModels(project, table, "");
        for (NDataModel model : models) {
            Segments<NDataSegment> segments = getSegments(model.getName(), project, refreshStart, refreshEnd);
            if (segments.getBuildingSegments().size() > 0) {
                throw new BadRequestException(
                        "Can not refresh, some segments is building during the range you want to refresh!");

            }
        }
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        EventManager eventManager = getEventManager(project);
        SegmentRange segmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(refreshStart, refreshEnd);
        LoadingRangeRefreshEvent event = new LoadingRangeRefreshEvent();
        event.setSegmentRange(segmentRange);
        event.setApproved(true);
        event.setProject(project);
        event.setTableName(table);
        eventManager.post(event);
    }


    public void createModel(ModelRequest modelRequest) throws IOException {
        String project = modelRequest.getProject();
        checkAliasExist(modelRequest.getName(), modelRequest.getAlias(), project);
        //remove some attributes in modelResponse to fit NDataModel
        NDataModel dataModel = convertToDataModel(modelRequest);
        getDataModelManager(project).createDataModelDesc(dataModel, dataModel.getOwner());
    }

    public static NDataModel convertToDataModel(ModelRequest modelRequest) throws IOException {
        //remove some attributes in modelResponse to fit NDataModel
        NDataModel dataModel = JsonUtil.readValue(JsonUtil.writeValueAsString(modelRequest), NDataModel.class);
        dataModel.setUuid(UUID.randomUUID().toString());

        List<SimplifiedMeasureResponse> simplifiedMeasures = modelRequest.getSimplifiedMeasures();
        List<NDataModel.Measure> measures = new ArrayList<>();
        boolean hasCount = false;
        int id = NDataModel.MEASURE_ID_BASE;
        for (SimplifiedMeasureResponse simplifiedMeasure : simplifiedMeasures) {
            NDataModel.Measure measure = new NDataModel.Measure();
            measure.id = simplifiedMeasure.getId();
            measure.setName(simplifiedMeasure.getName());
            FunctionDesc functionDesc = new FunctionDesc();
            functionDesc.setReturnType(simplifiedMeasure.getReturnType());
            functionDesc.setExpression(simplifiedMeasure.getExpression());
            List<ParameterResponse> parameterResponseList = simplifiedMeasure.getParameterValue();
            functionDesc.setParameter(enrichParameterDesc(parameterResponseList, null));
            // TODO just check count(*), update this logic when count(col) is implemented
            if (functionDesc.isCount()) {
                hasCount = true;
            }
            measure.setFunction(functionDesc);
            measure = CubeUtils.newMeasure(functionDesc, simplifiedMeasure.getName(), id++);
            measures.add(measure);

        }
        if (!hasCount) {
            FunctionDesc functionDesc = new FunctionDesc();
            ParameterDesc parameterDesc = new ParameterDesc();
            parameterDesc.setType("constant");
            parameterDesc.setValue("1");
            functionDesc.setParameter(parameterDesc);
            functionDesc.setExpression("COUNT");
            functionDesc.setReturnType("bigint");
            NDataModel.Measure measure = CubeUtils.newMeasure(functionDesc, "COUNT_ALL", id);
            measures.add(measure);
        }
        dataModel.setAllMeasures(measures);

        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        for (int i = 0; i < allNamedColumns.size(); i++) {
            allNamedColumns.get(i).id = i;
        }

        return dataModel;
    }

    private static ParameterDesc enrichParameterDesc(List<ParameterResponse> parameterResponseList, ParameterDesc nextParameterDesc) {
        if (CollectionUtils.isEmpty(parameterResponseList)) {
            return nextParameterDesc;
        }
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType(parameterResponseList.get(0).getType());
        parameterDesc.setValue(parameterResponseList.get(0).getValue());
        parameterDesc.setNextParameter(nextParameterDesc);
        if (!parameterResponseList.isEmpty()) {
            return enrichParameterDesc(parameterResponseList.subList(1, parameterResponseList.size()), parameterDesc);
        } else {
            return parameterDesc;
        }

    }

    public void buildSegmentsManually(String project, String model, String start, String end)
            throws IOException, PersistentException {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(model);
        if (!modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new BadRequestException("Table oriented Model '" + model + "' can not build segments manually!");
        }
        List<NCubePlan> cubePlans = getCubePlans(model, project);
        if (CollectionUtils.isEmpty(cubePlans)) {
            throw new BadRequestException(
                    "Can not build segments, please define table index or aggregate index first!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        TableDesc table = getTableManager(project).getTableDesc(modelDesc.getRootFactTableName());
        SegmentRange segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);

        checkSegmentToBuildOverlapsBuilt(project, model, segmentRangeToBuild);

        for (NCubePlan cubePlan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            NDataSegment newSegment = dataflowManager.appendSegment(dataflow, segmentRangeToBuild);
            List<Integer> segmentIds = new ArrayList<>();
            segmentIds.add(newSegment.getId());
            AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
            addSegmentEvent.setApproved(true);
            addSegmentEvent.setSegmentIds(segmentIds);
            addSegmentEvent.setCubePlanName(cubePlan.getName());
            addSegmentEvent.setModelName(model);
            addSegmentEvent.setSegmentRange(segmentRangeToBuild);
            addSegmentEvent.setProject(project);
            eventManager.post(addSegmentEvent);
        }
    }

    private void checkSegmentToBuildOverlapsBuilt(String project, String model, SegmentRange segmentRangeToBuild) {
        Segments<NDataSegment> segments = getSegments(model, project, "0", "" + Long.MAX_VALUE);
        if (CollectionUtils.isEmpty(segments)) {
            return;
        } else {
            for (NDataSegment existedSegment : segments) {
                if (existedSegment.getSegRange().overlaps(segmentRangeToBuild)) {
                    throw new BadRequestException("Segments to build overlaps built or building segment(from " + existedSegment.getSegRange().getStart().toString() + " to " + existedSegment.getSegRange().getEnd().toString() + "), please select new data range and try again!");
                }
            }
        }
    }


    public void primaryCheck(NDataModel modelDesc) {
        Message msg = MsgPicker.getMsg();

        if (modelDesc == null) {
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        }

        String modelName = modelDesc.getName();

        if (StringUtils.isEmpty(modelName)) {
            logger.info("Model name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelName, VALID_MODELNAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", modelDesc.getName());
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), modelName));
        }
    }

    public ComputedColumnUsageResponse getComputedColumnUsages(String project) {
        ComputedColumnUsageResponse ret = new ComputedColumnUsageResponse();
        List<NDataModel> models = getDataModelManager(project).getDataModels();
        for (NDataModel model : models) {
            for (ComputedColumnDesc computedColumnDesc : model.getComputedColumnDescs()) {
                ret.addUsage(computedColumnDesc, model.getName());
            }
        }
        return ret;
    }

    /**
     * check if the computed column expressions are valid ( in hive)
     * <p>
     * ccInCheck is optional, if provided, other cc in the model will skip hive check
     */
    public void checkComputedColumn(final NDataModel dataModelDesc, String project, String ccInCheck) {

        dataModelDesc.setDraft(false);
        if (dataModelDesc.getUuid() == null)
            dataModelDesc.updateRandomUuid();

        dataModelDesc.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataModelManager(project).getDataModels(), false);

        if (dataModelDesc.isSeekingCCAdvice()) {
            // if it's seeking for advise, it should have thrown exceptions by far
            throw new IllegalStateException("No advice could be provided");
        }

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {
            checkCCName(cc.getColumnName());

            if (!StringUtils.isEmpty(ccInCheck)
                    && !StringUtils.equalsIgnoreCase(cc.getFullName(), ccInCheck))
                continue;

            //replace computed columns with basic columns
            String ccExpression = massageComputedColumn(dataModelDesc, project, cc);
            cc.simpleParserCheck(ccExpression, dataModelDesc.getAliasMap().keySet());

            //check by data source, this could be slow
            long ts = System.currentTimeMillis();
            try {
                NDataModelFlatTableDesc flatTableDesc = new NDataModelFlatTableDesc(dataModelDesc, true);
                SparkSession ss = SparkSession.builder().enableHiveSupport().getOrCreate();
                Dataset<Row> ds = NJoinedFlatTable.generateDataset(flatTableDesc, ss);
                ds.selectExpr(NSparkCubingUtil.convertFromDot(ccExpression));
            } catch (Exception e) {
                throw new IllegalArgumentException("The expression " + cc.getExpression() + " failed syntax check", e);
            }

            logger.debug("Spent {} ms to visit data source to validate computed column expression: {}",
                    (System.currentTimeMillis() - ts), cc.getExpression());
        }    }

    static void checkCCName(String name) {
        if (PushDownConverterKeyWords.CALCITE.contains(name.toUpperCase())
                || PushDownConverterKeyWords.HIVE.contains(name.toUpperCase())) {
            throw new IllegalStateException(
                    "The computed column's name:" + name + " is a sql keyword, please choose another name.");
        }
    }

    private static String massageComputedColumn(NDataModel modelDesc, String project, ComputedColumnDesc cc) {

        NDataModelFlatTableDesc flatTableDesc = new NDataModelFlatTableDesc(modelDesc);

        String tempConst = "'" + UUID.randomUUID().toString() + "'";

        StringBuilder forCC = new StringBuilder();
        forCC.append("select ");
        forCC.append(cc.getExpression());
        forCC.append(" ,").append(tempConst);
        forCC.append(" ");
        NJoinedFlatTable.appendJoinStatement(flatTableDesc, forCC, false);


        String ccSql = KeywordDefaultDirtyHack.transform(forCC.toString());
        ccSql = KapQueryUtil.massageComputedColumn(ccSql, project, "DEFAULT", modelDesc);

        return ccSql.substring("select ".length(), ccSql.indexOf(tempConst) - 1);
    }

    public void preProcessBeforeModelSave(NDataModel model, String project) {

        model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataModelManager(project).getDataModels(), false);

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = massageComputedColumn(model, project, ccDesc);
            ccDesc.setInnerExpression(ccExpression);
        }
    }

    public void updateModelDataCheckDesc(String project, String modelName, long checkOptions, long faultThreshold,
            long faultActions) throws IOException {

        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelName);
        if (dataModel == null) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }

        dataModel.setDataCheckDesc(DataCheckDesc.valueOf(checkOptions, faultThreshold, faultActions));
        getDataModelManager(project).updateDataModelDesc(dataModel);
    }

    public void deleteSegmentById(String model, String project, int[] ids) throws PersistentException {
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(
                    "Model '" + model + "' is table table oriented, can not remove segments manually!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        checkSegmentsOverlapWithBuilding(model, project, ids);
        checkDeleteSegmentLegally(model, project, ids);
        for (NCubePlan cubePlan : getCubePlans(model, project)) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            List<Integer> idsToDelete = new ArrayList<>();
            for (int id : ids) {
                if (dataflow.getSegment(id) != null) {
                    idsToDelete.add(id);
                }
            }
            if (CollectionUtils.isEmpty(idsToDelete)) {
                continue;
            }
            RemoveSegmentEvent event = new RemoveSegmentEvent();
            event.setSegmentIds(idsToDelete);
            event.setCubePlanName(cubePlan.getName());
            event.setModelName(model);
            event.setApproved(true);
            event.setProject(project);
            eventManager.post(event);
        }
    }

    private void checkSegmentsOverlapWithBuilding(String model, String project, int[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        for (NCubePlan cubePlan : getCubePlans(model, project)) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            Segments<NDataSegment> buildingSegments = dataflow.getSegments().getBuildingSegments();
            if (buildingSegments.size() > 0) {
                for (NDataSegment segment : buildingSegments) {
                    for (int id : ids) {
                        if (segment.getSegRange().overlaps(dataflow.getSegment(id).getSegRange())) {
                            throw new BadRequestException("Can not remove segment (ID:" + id
                                    + "), because this segment overlaps building segments!");
                        }
                    }
                }

            }
        }
    }

    private void checkDeleteSegmentLegally(String model, String project, int[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        List<NCubePlan> cubePlans = getCubePlans(model, project);
        List<Integer> idsToDelete = Ints.asList(ids);
        for (NCubePlan cubePlan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            Segments<NDataSegment> allSegments = dataflow.getSegments();
            if (allSegments.size() <= 2) {
                continue;
            } else {
                for (int i = 0; i < allSegments.size(); i++) {
                    for (int id : idsToDelete) {
                        if (id == allSegments.get(i).getId() && i >= 1 && i < allSegments.size() - 1) {
                            if (!idsToDelete.contains(allSegments.get(i - 1).getId())
                                    || !idsToDelete.contains(allSegments.get(i + 1).getId())) {
                                throw new BadRequestException(
                                        "Only consecutive segments in head or tail can be removed!");
                            }
                        }
                    }
                }

            }
        }
    }


    public void refreshSegmentById(String modelName, String project, int[] ids) throws PersistentException {
        NDataflowManager dataflowManager = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        checkSegmentsOverlapWithBuilding(modelName, project, ids);
        for (NCubePlan cubePlan : getCubePlans(modelName, project)) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            for (int id : ids) {
                NDataSegment segment = dataflow.getSegment(id);
                if (dataflow.getSegment(id) != null) {
                    Event event = new RefreshSegmentEvent();
                    event.setSegmentRange(segment.getSegRange());
                    event.setProject(project);
                    event.setApproved(true);
                    event.setModelName(modelName);
                    event.setCubePlanName(segment.getCubePlan().getName());
                    eventManager.post(event);

                }
            }
        }
    }
    public void updateDataModelSemantic(ModelSemanticUpdateRequest request) throws PersistentException, IOException {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val cubeManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val originModel = modelManager.getDataModelDesc(request.getModel());

        Preconditions.checkState(originModel.getModelStatus() == ModelStatus.READY,
                "model " + request.getModel() + " is building");
        val copyModel = modelManager.copyForWrite(originModel);
        BeanUtils.copyProperties(request, copyModel, "allNamedColumns", "allMeasures");
        updateModelColumns(copyModel, request);
        val targetModel = modelManager.updateDataModelDesc(copyModel);

        for (NCubePlan cubePlan : cubeManager.findMatchingCubePlan(request.getModel(), request.getProject(),
                KylinConfig.getInstanceFromEnv())) {
            Preconditions.checkState(cubePlan.getStatus() == CubePlanStatus.READY,
                    "cube " + cubePlan.getName() + " is building");
            // check agg group contains removed dimensions
            val rule = cubePlan.getRuleBasedCuboidsDesc();
            if (rule != null && !targetModel.getEffectiveDimenionsMap().keySet().containsAll(rule.getDimensions())) {
                val allDimensions = rule.getDimensions();
                val dimensionNames = allDimensions.stream()
                        .filter(id -> !targetModel.getEffectiveDimenionsMap().containsKey(id))
                        .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
                throw new IllegalStateException("cube " + cubePlan.getName() + " still contains dimensions "
                        + StringUtils.join(dimensionNames, ","));
            }
        }

        val eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val event = new ModelSemanticUpdateEvent();
        event.setProject(request.getProject());
        event.setModelName(request.getModel());
        event.setOriginModel(originModel);
        eventManager.post(event);
    }

    public void updateDataModelCanvas(ModelCanvasUpdateRequest request) throws IOException {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val originModel = modelManager.getDataModelDesc(request.getModel());
        val targetModel = modelManager.copyForWrite(originModel);

        targetModel.setCanvas(request.getCanvas());
        modelManager.updateDataModelDesc(targetModel);
    }

    private void updateModelColumns(NDataModel targetModel, ModelSemanticUpdateRequest request) throws IOException {
        val newMeasures = Lists.<NDataModel.Measure> newArrayList();
        var maxMeasureId = targetModel.getEffectiveMeasureMap().keySet().stream().mapToInt(i -> i).max()
                .orElse(NDataModel.MEASURE_ID_BASE - 1) + 1;
        for (NDataModel.Measure measure : request.getAllMeasures()) {
            measure.getFunction().init(targetModel);
            val matched = targetModel.getEffectiveMeasureMap().values().stream()
                    .filter(m -> m.getName().equals(measure.getName()) && m.getFunction().equals(measure.getFunction()))
                    .count();
            if (matched == 0) {
                val measureCopy = JsonUtil.deepCopy(measure, NDataModel.Measure.class);
                measureCopy.id = maxMeasureId;
                newMeasures.add(measureCopy);
                maxMeasureId++;
            }
        }
        targetModel.getAllMeasures().addAll(newMeasures);
        for (NDataModel.Measure measure : targetModel.getAllMeasures()) {
            val matched = request.getAllMeasures().stream()
                    .filter(m -> m.getName().equals(measure.getName()) && m.getFunction().equals(measure.getFunction()))
                    .count();
            measure.tomb = matched == 0 || measure.tomb;
        }

        for (NDataModel.NamedColumn newColumn : request.getAllDimensions()) {
            // change name does not matter
            val matched = targetModel.getAllNamedColumns().stream()
                    .filter(col -> col.isDimension() && col.aliasDotColumn.equals(newColumn.aliasDotColumn))
                    .collect(Collectors.toList());
            for (NDataModel.NamedColumn column : matched) {
                column.name = newColumn.name;
            }
            if (matched.size() == 0) {
                targetModel.getAllNamedColumns().stream()
                        .filter(col -> col.isExist() && col.aliasDotColumn.equals(newColumn.aliasDotColumn))
                        .forEach(col -> {
                            col.status = NDataModel.ColumnStatus.DIMENSION;
                            col.name = newColumn.name;
                        });
            }
        }
        for (NDataModel.NamedColumn column : targetModel.getAllNamedColumns()) {
            if (!column.isDimension()) {
                continue;
            }
            val matched = request.getAllDimensions().stream()
                    .filter(col -> col.aliasDotColumn.equals(column.aliasDotColumn)).count();
            if (matched == 0) {
                column.status = NDataModel.ColumnStatus.EXIST;
            }
        }
    }

}
