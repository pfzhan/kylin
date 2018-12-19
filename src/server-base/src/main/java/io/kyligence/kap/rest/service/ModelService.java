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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.adhocquery.PushDownConverterKeyWords;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelFlatTableDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.ModelInfoResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NSpanningTreeResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final Message msg = MsgPicker.getMsg();

    private static final String LAST_MODIFY = "last_modify";

    private static final String MODEL = "Model '";

    public static final char[] VALID_MODELNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    private QueryHistoryService queryHistoryService;

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpdater;

    @Setter
    @Autowired
    private SegmentHelper segmentHelper;

    private NDataModel getNDataModelByModelName(String modelName, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelName);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        return nDataModel;
    }

    public List<NDataModelResponse> getModels(final String modelName, final String projectName, boolean exactMatch,
            String owner, String status, String sortBy, boolean reverse) {

        List<NDataModel> models = getDataModelManager(projectName).getDataModels();
        List<NDataModelResponse> filterModels = new ArrayList<>();
        for (NDataModel modelDesc : models) {
            boolean isModelNameMatch = StringUtils.isEmpty(modelName)
                    || (exactMatch && modelDesc.getAlias().equalsIgnoreCase(modelName))
                    || (!exactMatch && modelDesc.getAlias().toLowerCase().contains(modelName.toLowerCase()));
            boolean isModelOwnerMatch = StringUtils.isEmpty(owner)
                    || (exactMatch && modelDesc.getOwner().equalsIgnoreCase(owner))
                    || (!exactMatch && modelDesc.getOwner().toLowerCase().contains(owner.toLowerCase()));
            if (isModelNameMatch && isModelOwnerMatch) {
                RealizationStatusEnum modelStatus = getModelStatus(modelDesc.getName(), projectName);
                boolean isModelStatusMatch = StringUtils.isEmpty(status)
                        || (modelStatus != null && modelStatus.name().equalsIgnoreCase(status));

                if (isModelStatusMatch) {
                    NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
                    nDataModelResponse.setStatus(modelStatus);
                    filterModels.add(nDataModelResponse);
                }
            }
        }
        return sortModelResponses(sortBy, reverse, filterModels);
    }

    private List<NDataModelResponse> sortModelResponses(String sortBy, boolean reverse,
            List<NDataModelResponse> filterModels) {
        var comp = Comparator.<NDataModelResponse> comparingLong(r -> -r.getLastModified());
        if (sortBy.equals(LAST_MODIFY) && reverse) {
            Collections.sort(filterModels, comp);
        } else if (sortBy.equals(LAST_MODIFY) && !reverse) {
            Collections.sort(filterModels, comp);
            Collections.reverse(filterModels);

        }
        return filterModels;
    }

    private NDataModelResponse enrichModelResponse(NDataModel modelDesc, String projectName) {
        NDataModelResponse nDataModelResponse = new NDataModelResponse(modelDesc);
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

    private RealizationStatusEnum getModelStatus(String modelName, String projectName) {
        val cubePlan = getCubePlan(modelName, projectName);
        if (cubePlan != null) {
            return getDataflowManager(projectName).getDataflow(cubePlan.getName()).getStatus();
        } else {
            return null;
        }
    }

    public Segments<NDataSegment> getSegments(String modelName, String project, String start, String end) {
        val cubePlan = getCubePlan(modelName, project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        SegmentRange filterRange = getSegmentRangeByModel(project, modelName, start, end);
        Segments<NDataSegment> segments = new Segments<NDataSegment>();
        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        for (NDataSegment segment : dataflow.getSegments()) {
            if (segment.getSegRange().overlaps(filterRange)) {
                long segmentSize = dataflowManager.getSegmentSize(segment);
                NDataSegmentResponse nDataSegmentResponse = new NDataSegmentResponse(segment);
                nDataSegmentResponse.setBytesSize(segmentSize);
                segments.add(nDataSegmentResponse);
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
        val cubePlan = getCubePlan(modelName, project);
        List<NCuboidDesc> cuboidDescs = new ArrayList<NCuboidDesc>();
        cuboidDescs.addAll(cubePlan.getAllCuboids());
        return cuboidDescs;
    }

    public CuboidDescResponse getCuboidById(String modelName, String project, Long cuboidId) {
        NCubePlan cubePlan = getCubePlan(modelName, project);
        NCuboidDesc cuboidDesc = cubePlan.getCuboidDesc(cuboidId);
        return new CuboidDescResponse(cuboidDesc);
    }

    public String getModelJson(String modelName, String project) throws JsonProcessingException {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelName);
        return JsonUtil.writeValueAsIndentString(modelDesc);
    }

    public List<NSpanningTree> getModelRelations(String modelName, String project) {
        val cubeplan = getCubePlan(modelName, project);
        List<NSpanningTree> result = new ArrayList<>();
        val allLayouts = Lists.<NCuboidLayout> newArrayList();
        if (cubeplan.getRuleBasedCuboidsDesc() != null) {
            val rule = cubeplan.getRuleBasedCuboidsDesc();
            allLayouts.addAll(rule.genCuboidLayouts());
        }
        val autoLayouts = cubeplan.getWhitelistCuboidLayouts().stream()
                .filter(layout -> layout.getId() < NCuboidDesc.TABLE_INDEX_START_ID).collect(Collectors.toList());
        allLayouts.addAll(autoLayouts);
        val tree = NSpanningTreeFactory.fromCuboidLayouts(allLayouts, cubeplan.getName());
        result.add(tree);
        return result;
    }

    public List<NSpanningTreeResponse> getSimplifiedModelRelations(String modelName, String project) {
        val model = getDataModelManager(project).getDataModelDesc(modelName);
        List<NSpanningTreeResponse> result = Lists.newArrayList();
        for (NSpanningTree spanningTree : getModelRelations(modelName, project)) {
            result.add(new NSpanningTreeResponse((NForestSpanningTree) spanningTree, model));
        }
        return result;
    }

    public List<RelatedModelResponse> getRelateModels(String project, String table, String modelName) {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<String> models = dataModelManager.getTableOrientedModelsUsingRootTable(tableDesc);
        List<RelatedModelResponse> relatedModel = new ArrayList<>();
        val errorExecutables = getExecutableManager(project).getExecutablesByStatus(ExecutableState.ERROR);
        for (String model : models) {
            Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
            NDataModel dataModelDesc = dataModelManager.getDataModelDesc(model);
            if (StringUtils.isEmpty(modelName)
                    || dataModelDesc.getAlias().toLowerCase().contains(modelName.toLowerCase())) {
                RelatedModelResponse relatedModelResponse = new RelatedModelResponse(dataModelDesc);
                Segments<NDataSegment> segments = getSegments(model, project, "", "");
                for (NDataSegment segment : segments) {
                    segmentRanges.put(segment.getSegRange(), segment.getStatus());
                }
                relatedModelResponse.setStatus(getModelStatus(model, project));
                relatedModelResponse.setSegmentRanges(segmentRanges);
                val filteredErrorExecutables = errorExecutables.stream()
                        .filter(abstractExecutable -> StringUtils
                                .equalsIgnoreCase(abstractExecutable.getTargetModelAlias(), dataModelDesc.getAlias()))
                        .collect(Collectors.toList());
                relatedModelResponse.setHasErrorJobs(CollectionUtils.isNotEmpty(filteredErrorExecutables));
                relatedModel.add(relatedModelResponse);
            }
        }
        return relatedModel;
    }

    private NCubePlan getCubePlan(String modelName, String project) {
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        return cubePlanManager.findMatchingCubePlan(modelName, project, KylinConfig.getInstanceFromEnv());
    }

    private void checkAliasExist(String modelName, String newAlias, String project) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<NDataModel> models = dataModelManager.getDataModels();
        for (NDataModel model : models) {
            if (!StringUtils.isNotEmpty(modelName) && model.getName().equals(modelName)) {
                continue;
            } else if (model.getAlias().equals(newAlias)) {
                throw new BadRequestException("Model alias " + newAlias + " already exists!");
            }
        }
    }

    @Transaction(project = 1)
    public void dropModel(String model, String project) {
        NDataModel dataModelDesc = getNDataModelByModelName(model, project);
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        NCubePlan cubePlan = getCubePlan(model, project);
        Segments<NDataSegment> segments = dataflowManager.getDataflow(cubePlan.getName()).getSegments();
        if (CollectionUtils.isNotEmpty(segments)) {
            throw new IllegalStateException("You should purge your model first before you drop it!");
        }
        cubePlanManager.removeCubePlan(cubePlan);
        dataflowManager.dropDataflow(cubePlan.getName());
        getDataModelManager(project).dropModel(dataModelDesc);
    }

    @Transaction(project = 1)
    public void purgeModel(String model, String project) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        val cubePlan = getCubePlan(model, project);
        List<NDataSegment> segments = new ArrayList<>();
        if (cubePlan != null) {
            NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
            segments.addAll(dataflow.getSegments());
            NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
            NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
            nDataflowUpdate.setToRemoveSegs(nDataSegments);
            dataflowManager.updateDataflow(nDataflowUpdate);
        }

    }

    @Transaction(project = 1)
    public void purgeModelManually(String model, String project) {
        NDataModel dataModelDesc = getNDataModelByModelName(model, project);
        if (dataModelDesc.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(MODEL + model + "' is table oriented, can not pruge the model!!");
        }
        purgeModel(model, project);
    }

    @Transaction(project = 2)
    public void cloneModel(String modelName, String newModelName, String project) {
        checkAliasExist("", newModelName, project);
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel dataModelDesc = getNDataModelByModelName(modelName, project);
        //copyForWrite nDataModel do init,but can not set new modelname
        NDataModel nDataModel;
        try {
            nDataModel = JsonUtil.readValue(JsonUtil.writeValueAsIndentString(dataModelDesc), NDataModel.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        nDataModel.setName(UUID.randomUUID().toString());
        nDataModel.setAlias(newModelName);
        nDataModel.setLastModified(0L);
        nDataModel.setMvcc(-1);
        dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
        cloneCubePlan(modelName, nDataModel.getName(), project, nDataModel.getOwner());
    }

    private void cloneCubePlan(String modelName, String newModelName, String project, String owner) {
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        NCubePlan cubePlan = cubePlanManager.findMatchingCubePlan(modelName, project, getConfig());
        NDataflowManager dataflowManager = getDataflowManager(project);
        NCubePlan copy = cubePlanManager.copy(cubePlan);
        copy.setModelName(newModelName);
        copy.setSegmentRangeStart(0L);
        copy.setSegmentRangeEnd(0L);
        copy.updateRandomUuid();
        copy.setName(copy.getUuid());
        copy.setLastModified(0L);
        copy.setMvcc(-1);
        cubePlanManager.createCubePlan(copy);
        NDataflow nDataflow = new NDataflow();
        nDataflow.setStatus(RealizationStatusEnum.OFFLINE);
        nDataflow.setCubePlanName(cubePlan.getName());
        dataflowManager.createDataflow(copy.getName(), copy, owner);
    }

    @Transaction(project = 0)
    public void renameDataModel(String project, String modelName, String newAlias) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = getNDataModelByModelName(modelName, project);
        //rename
        checkAliasExist(modelName, newAlias, project);
        nDataModel.setAlias(newAlias);
        NDataModel modelUpdate = modelManager.copyForWrite(nDataModel);
        modelManager.updateDataModelDesc(modelUpdate);
    }

    @Transaction(project = 1)
    public void unlinkModel(String modelName, String project) {
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataModelManager dataModelManager = getDataModelManager(project);

        NDataModel nDataModel = getNDataModelByModelName(modelName, project);
        if (nDataModel.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new IllegalStateException("Model " + modelName + " is model based, can not unlink it!");
        } else {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager
                    .getDataLoadingRange(nDataModel.getRootFactTable().getTableIdentity());
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

    @Transaction(project = 1)
    public void updateDataModelStatus(String modelName, String project, String status) {
        NDataModel nDataModel = getNDataModelByModelName(modelName, project);
        NCubePlan cubePlan = getCubePlan(nDataModel.getName(), project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        checkDataflowStatus(dataflow, modelName);
        boolean needChangeStatus = (status.equals(RealizationStatusEnum.OFFLINE.name())
                && dataflow.getStatus().equals(RealizationStatusEnum.ONLINE))
                || (status.equals(RealizationStatusEnum.ONLINE.name())
                        && dataflow.getStatus().equals(RealizationStatusEnum.OFFLINE));
        if (needChangeStatus) {
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
            if (status.equals(RealizationStatusEnum.OFFLINE.name())) {
                nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            } else if (status.equals(RealizationStatusEnum.ONLINE.name())) {
                nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            }
            dataflowManager.updateDataflow(nDataflowUpdate);
        }
    }

    private void checkDataflowStatus(NDataflow dataflow, String modelName) {
        if (dataflow.getStatus().equals(RealizationStatusEnum.DESCBROKEN)) {
            throw new BadRequestException("DescBroken model " + modelName + " can not online or offline!");
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelName, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelName).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    public boolean isModelsUsingTable(String table, String project) {
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table))
                .size() > 0;
    }

    public List<String> getModelsUsingTable(String table, String project) {
        return getDataModelManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table));
    }

    public RefreshAffectedSegmentsResponse getAffectedSegmentsResponse(String project, String table, String start,
            String end, ManagementType managementType) {
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

    @Transaction(project = 0)
    public void refreshSegments(String project, String table, String refreshStart, String refreshEnd,
            String affectedStart, String affectedEnd) {

        RefreshAffectedSegmentsResponse response = getAffectedSegmentsResponse(project, table, refreshStart, refreshEnd,
                ManagementType.TABLE_ORIENTED);
        if (!response.getAffectedStart().equals(affectedStart) || !response.getAffectedEnd().equals(affectedEnd)) {
            throw new BadRequestException("Can not refresh, please try again and confirm affected storage!");
        }

        List<RelatedModelResponse> models = getRelateModels(project, table, "");
        for (NDataModel model : models) {
            Segments<NDataSegment> segments = getSegments(model.getName(), project, refreshStart, refreshEnd);
            if (segments.getBuildingSegments().size() > 0) {
                throw new BadRequestException(
                        "Can not refresh, some segments is building within the range you want to refresh!");
            }
        }

        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        SegmentRange segmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(refreshStart, refreshEnd);
        segmentHelper.refreshLoadingRange(project, table, segmentRange);
    }

    @Transaction(project = 0)
    public void createModel(String project, ModelRequest modelRequest) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof UserDetails) {
            modelRequest.setOwner(((UserDetails) authentication.getPrincipal()).getUsername());
        }
        checkAliasExist(modelRequest.getName(), modelRequest.getAlias(), project);
        //remove some attributes in modelResponse to fit NDataModel
        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        preProcessBeforeModelSave(dataModel, project);
        val model = getDataModelManager(project).createDataModelDesc(dataModel, dataModel.getOwner());
        syncPartitionDesc(model.getName(), project);

        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val nCubePlan = new NCubePlan();
        nCubePlan.setUuid(UUID.randomUUID().toString());
        nCubePlan.setModelName(modelRequest.getName());
        nCubePlan.setName(modelRequest.getName() + "_cube");
        nCubePlan.setModelName(modelRequest.getName());
        cubePlanManager.createCubePlan(nCubePlan);
        dataflowManager.createDataflow(nCubePlan.getName(), nCubePlan, model.getOwner());

    }

    @Transaction(project = 0)
    public void buildSegmentsManually(String project, String model, String start, String end) {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(model);
        if (!modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new BadRequestException("Table oriented model '" + model + "' can not build segments manually!");
        }
        val cubePlan = getCubePlan(model, project);
        if (cubePlan == null) {
            throw new BadRequestException(
                    "Can not build segments, please define table index or aggregate index first!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        TableDesc table = getTableManager(project).getTableDesc(modelDesc.getRootFactTableName());
        SegmentRange segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);

        checkSegmentToBuildOverlapsBuilt(project, model, segmentRangeToBuild);

        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        NDataSegment newSegment = dataflowManager.appendSegment(dataflow, segmentRangeToBuild);

        AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setSegmentId(newSegment.getId());
        addSegmentEvent.setCubePlanName(cubePlan.getName());
        addSegmentEvent.setModelName(model);
        addSegmentEvent.setJobId(UUID.randomUUID().toString());
        addSegmentEvent.setOwner(getUsername());
        eventManager.post(addSegmentEvent);

        PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setCubePlanName(cubePlan.getName());
        postAddSegmentEvent.setSegmentId(newSegment.getId());
        postAddSegmentEvent.setModelName(model);
        postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
        postAddSegmentEvent.setOwner(getUsername());
        eventManager.post(postAddSegmentEvent);

    }

    void syncPartitionDesc(String model, String project) {
        val dataloadingManager = getDataLoadingRangeManager(project);
        val datamodelManager = getDataModelManager(project);
        val modelDesc = datamodelManager.getDataModelDesc(model);
        val dataloadingRange = dataloadingManager.getDataLoadingRange(modelDesc.getRootFactTableName());
        val modelUpdate = datamodelManager.copyForWrite(modelDesc);
        //full load
        if (dataloadingRange == null) {
            modelUpdate.setPartitionDesc(null);
        } else {
            var partition = modelUpdate.getPartitionDesc();
            if (partition == null) {
                partition = new PartitionDesc();
            }
            partition.setPartitionDateColumn(dataloadingRange.getColumnName());
            partition.setPartitionDateFormat(dataloadingRange.getPartitionDateFormat());
            modelUpdate.setPartitionDesc(partition);
        }
        datamodelManager.updateDataModelDesc(modelUpdate);
    }

    private void checkSegmentToBuildOverlapsBuilt(String project, String model, SegmentRange segmentRangeToBuild) {
        Segments<NDataSegment> segments = getSegments(model, project, "0", "" + Long.MAX_VALUE);
        if (CollectionUtils.isEmpty(segments)) {
            return;
        } else {
            for (NDataSegment existedSegment : segments) {
                if (existedSegment.getSegRange().overlaps(segmentRangeToBuild)) {
                    throw new BadRequestException("Segments to build overlaps built or building segment(from "
                            + existedSegment.getSegRange().getStart().toString() + " to "
                            + existedSegment.getSegRange().getEnd().toString()
                            + "), please select new data range and try again!");
                }
            }
        }
    }

    public void primaryCheck(NDataModel modelDesc) {
        Message msg = MsgPicker.getMsg();

        if (modelDesc == null) {
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        }

        String modelName = modelDesc.getAlias();

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
                getDataModelManager(project).getDataModels(), false, project);

        if (dataModelDesc.isSeekingCCAdvice()) {
            // if it's seeking for advise, it should have thrown exceptions by far
            throw new IllegalStateException("No advice could be provided");
        }

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {
            checkCCName(cc.getColumnName());

            if (!StringUtils.isEmpty(ccInCheck) && !StringUtils.equalsIgnoreCase(cc.getFullName(), ccInCheck))
                continue;

            //replace computed columns with basic columns
            String ccExpression = KapQueryUtil.massageComputedColumn(dataModelDesc, project, cc);
            cc.simpleParserCheck(ccExpression, dataModelDesc.getAliasMap().keySet());

            //check by data source, this could be slow
            long ts = System.currentTimeMillis();
            try {
                NDataModelFlatTableDesc flatTableDesc = new NDataModelFlatTableDesc(dataModelDesc, true);
                SparkSession ss = SparderEnv.getSparkSession();
                Dataset<Row> ds = NJoinedFlatTable.generateDataset(flatTableDesc, ss);
                ds.selectExpr(NSparkCubingUtil.convertFromDot(ccExpression));
            } catch (Exception e) {
                throw new IllegalArgumentException("The expression " + cc.getExpression() + " failed syntax check", e);
            }

            logger.debug("Spent {} ms to visit data source to validate computed column expression: {}",
                    (System.currentTimeMillis() - ts), cc.getExpression());
        }
    }

    static void checkCCName(String name) {
        if (PushDownConverterKeyWords.CALCITE.contains(name.toUpperCase())
                || PushDownConverterKeyWords.HIVE.contains(name.toUpperCase())) {
            throw new IllegalStateException(
                    "The computed column's name:" + name + " is a sql keyword, please choose another name.");
        }
    }

    public void preProcessBeforeModelSave(NDataModel model, String project) {

        model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataModelManager(project).getDataModels(), false, project);

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = KapQueryUtil.massageComputedColumn(model, project, ccDesc);
            ccDesc.setInnerExpression(ccExpression);
        }
    }

    @Transaction(project = 0)
    public void updateModelDataCheckDesc(String project, String modelName, long checkOptions, long faultThreshold,
            long faultActions) {

        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelName);
        if (dataModel == null) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }

        dataModel.setDataCheckDesc(DataCheckDesc.valueOf(checkOptions, faultThreshold, faultActions));
        getDataModelManager(project).updateDataModelDesc(dataModel);
    }

    @Transaction(project = 1)
    public void deleteSegmentById(String model, String project, String[] ids) {
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(MODEL + model + "' is table oriented, can not remove segments manually!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsOverlapWithBuilding(model, project, ids);
        checkDeleteSegmentLegally(model, project, ids);
        val cubePlan = getCubePlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        Set<String> idsToDelete = Sets.newHashSet();
        for (String id : ids) {
            if (dataflow.getSegment(id) != null) {
                idsToDelete.add(id);
            } else {
                throw new IllegalArgumentException(
                        String.format("segment %s not found on model %s", id, dataflow.getModelAlias()));
            }
        }
        segmentHelper.removeSegment(project, dataflow.getName(), idsToDelete);
    }

    private void checkSegmentsOverlapWithBuilding(String model, String project, String[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        NCubePlan cubePlan = getCubePlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        Segments<NDataSegment> buildingSegments = dataflow.getSegments().getBuildingSegments();
        if (buildingSegments.size() > 0) {
            for (NDataSegment segment : buildingSegments) {
                for (String id : ids) {
                    if (segment.getSegRange().overlaps(dataflow.getSegment(id).getSegRange())) {
                        throw new BadRequestException("Can not remove segment (ID:" + id
                                + "), because this segment overlaps building segments!");
                    }
                }
            }

        }
    }

    private void checkDeleteSegmentLegally(String model, String project, String[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        val cubePlan = getCubePlan(model, project);
        List<String> idsToDelete = Lists.newArrayList(ids);
        NDataflow dataflow = dataflowManager.getDataflow(cubePlan.getName());
        Segments<NDataSegment> allSegments = dataflow.getSegments();
        if (allSegments.size() <= 2) {
            return;
        } else {
            for (int i = 1; i < allSegments.size() - 1; i++) {
                for (String id : idsToDelete) {
                    if (id.equals(allSegments.get(i).getId())) {
                        checkNeighbouringSegmentsDeleted(idsToDelete, i, allSegments);
                    }
                }
            }

        }
    }

    private void checkNeighbouringSegmentsDeleted(List<String> idsToDelete, int i, Segments<NDataSegment> allSegments) {
        if (!idsToDelete.contains(allSegments.get(i - 1).getId())
                || !idsToDelete.contains(allSegments.get(i + 1).getId())) {
            throw new BadRequestException("Only consecutive segments in head or tail can be removed!");
        }
    }

    @Transaction(project = 1)
    public void refreshSegmentById(String modelName, String project, String[] ids) {
        NDataflowManager dfMgr = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        NCubePlan cubePlan = getCubePlan(modelName, project);
        NDataflow df = dfMgr.getDataflow(cubePlan.getName());

        checkSegmentsOverlapWithBuilding(modelName, project, ids);

        for (String id : ids) {
            NDataSegment segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format("segment %s not found on model %s", id, df.getModelAlias()));
            }

            NDataSegment newSeg = dfMgr.refreshSegment(df, segment.getSegRange());

            val event = new RefreshSegmentEvent();
            event.setSegmentId(newSeg.getId());
            event.setModelName(modelName);
            event.setCubePlanName(segment.getCubePlan().getName());
            event.setOwner(getUsername());
            event.setJobId(UUID.randomUUID().toString());
            eventManager.post(event);

            val event2 = new PostMergeOrRefreshSegmentEvent();
            event2.setSegmentId(newSeg.getId());
            event2.setModelName(modelName);
            event2.setCubePlanName(segment.getCubePlan().getName());
            event2.setOwner(getUsername());
            event2.setJobId(event.getJobId());
            eventManager.post(event2);

        }
    }

    @Transaction(project = 0)
    public void updateDataModelSemantic(String project, ModelRequest request) {
        val modelManager = getDataModelManager(project);
        val cubeManager = getCubePlanManager(project);
        val dataflowManager = getDataflowManager(project);
        val originModel = modelManager.getDataModelDesc(request.getName());

        val df = dataflowManager.getDataflowByModelName(request.getName());
        val copyModel = modelManager.copyForWrite(originModel);
        semanticUpdater.updateModelColumns(copyModel, request);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), request.getProject())
                .getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, modelManager.getDataModels(), false, project);

        val cubePlan = cubeManager.findMatchingCubePlan(request.getName(), request.getProject(),
                KylinConfig.getInstanceFromEnv());
        // check agg group contains removed dimensions
        val rule = cubePlan.getRuleBasedCuboidsDesc();
        if (rule != null && !copyModel.getEffectiveDimenionsMap().keySet().containsAll(rule.getDimensions())) {
            val allDimensions = rule.getDimensions();
            val dimensionNames = allDimensions.stream()
                    .filter(id -> !copyModel.getEffectiveDimenionsMap().containsKey(id))
                    .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());

            throw new IllegalStateException("model " + cubePlan.getModel().getName()
                    + "'s agg group still contains dimensions " + StringUtils.join(dimensionNames, ","));
        }

        preProcessBeforeModelSave(copyModel, project);
        modelManager.updateDataModelDesc(copyModel);
        semanticUpdater.handleSemanticUpdate(project, request.getName(), originModel);
    }

    public NDataModel convertToDataModel(ModelRequest modelDesc) {
        return semanticUpdater.convertToDataModel(modelDesc);
    }

    public AffectedModelsResponse getAffectedModelsByToggleTableType(String tableName, String project, boolean fact) {
        val dataflowManager = getDataflowManager(project);
        val modelManager = getDataModelManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        if (fact) {
            checkSingleIncrementingLoadingTable(project, tableName);
        }
        val response = new AffectedModelsResponse();
        val models = modelManager.getTableOrientedModelsUsingRootTable(table);
        var size = 0;
        response.setModels(models);
        for (val model : models) {
            size += dataflowManager.getDataflowByteSize(model);
        }
        response.setByteSize(size);
        return response;
    }

    public void checkSingleIncrementingLoadingTable(String project, String tableName) {
        val modelManager = getDataModelManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        val modelsUsingTable = modelManager.getModelsUsingTable(table);
        for (val modelUsingTable : modelsUsingTable) {
            val modelDesc = modelManager.getDataModelDesc(modelUsingTable);
            if (!modelDesc.getRootFactTable().getTableDesc().getIdentity().equals(tableName)
                    || modelDesc.isJoinTable(tableName)) {
                throw new BadRequestException("Can not set table '" + tableName
                        + "' incrementing loading, due to another incrementing loading table existed in model '"
                        + modelUsingTable + "'!");
            }
        }
    }

    private void checkProjectWhenModelSelected(String model, String project) {
        if (!isSelectAll(model) && isSelectAll(project)) {
            throw new BadRequestException("Project name can not be empty when model is selected!");
        }
    }

    public List<ModelInfoResponse> getModelInfo(String suite, String model, String project, long start, long end) {
        List<ModelInfoResponse> modelInfoList = Lists.newArrayList();
        checkProjectWhenModelSelected(model, project);
        if (isSelectAll(project)) {
            modelInfoList.addAll(getAllModelInfo());
        } else if (isSelectAll(model)) {
            modelInfoList.addAll(getModelInfoByProject(project));
        } else {
            modelInfoList.add(getModelInfoByModel(model, project));
        }

        List<QueryTimesResponse> result = getQueryHistoryDao(project).getQueryTimesResponseBySql(suite, project, model,
                start, end, QueryTimesResponse.class);
        Map<String, QueryTimesResponse> resultMap = result.stream()
                .collect(Collectors.toMap(QueryTimesResponse::getModel, queryTimesResponse -> queryTimesResponse));
        for (val modelInfoResponse : modelInfoList) {
            if (resultMap.containsKey(modelInfoResponse.getModel())) {
                modelInfoResponse.setQueryTimes(resultMap.get(modelInfoResponse.getModel()).getQueryTimes());
            }
        }

        return modelInfoList;
    }

    private List<ModelInfoResponse> getAllModelInfo() {
        List<ModelInfoResponse> modelInfoLists = Lists.newArrayList();
        val projectManager = getProjectManager();
        List<ProjectInstance> projects = Lists.newArrayList();
        projects.addAll(projectManager.listAllProjects());
        for (val projectInstance : projects) {
            modelInfoLists.addAll(getModelInfoByProject(projectInstance.getName()));
        }
        return modelInfoLists;
    }

    private List<ModelInfoResponse> getModelInfoByProject(String project) {
        List<ModelInfoResponse> modelInfoLists = Lists.newArrayList();
        val projectManager = getProjectManager();
        List<ProjectInstance> projects = Lists.newArrayList();
        if (isSelectAll(project)) {
            projects.addAll(projectManager.listAllProjects());
        } else {
            projects.add(projectManager.getProject(project));
        }
        for (val projectInstance : projects) {
            val models = projectInstance.getModels();
            for (val model : models) {
                modelInfoLists.add(getModelInfoByModel(model, projectInstance.getName()));
            }
        }
        return modelInfoLists;
    }

    private ModelInfoResponse getModelInfoByModel(String model, String project) {
        val dataflowManager = getDataflowManager(project);
        val modelManager = getDataModelManager(project);
        val modelDesc = modelManager.getDataModelDesc(model);
        if (modelDesc == null) {
            throw new BadRequestException(MODEL + model + "' does not exist!");
        }
        val modelInfoResponse = new ModelInfoResponse();
        modelInfoResponse.setProject(project);
        modelInfoResponse.setAlias(modelDesc.getAlias());
        modelInfoResponse.setModel(model);
        val modelSize = dataflowManager.getDataflowByteSize(model);
        modelInfoResponse.setModelStorageSize(modelSize);
        return modelInfoResponse;
    }

    private boolean isSelectAll(String field) {
        return "*".equals(field);
    }
}
