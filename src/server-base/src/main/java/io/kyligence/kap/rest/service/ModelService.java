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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.rest.storage.ModelCleaner;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
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

import io.kyligence.kap.metadata.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
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
import io.kyligence.kap.rest.response.IndexEntityResponse;
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

    private static final String SEGMENT_PATH = "segment_path";

    private static final String FILE_COUNT = "file_count";

    private static final String CREATE_TIME = "create_time";

    private static final String START_TIME = "start_time";

    private static final String END_TIME = "end_time";

    private static final String STORAGE = "storage";

    public static final char[] VALID_NAME_FOR_MODEL_DIMENSION_MEASURE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpdater;

    @Setter
    @Autowired
    private SegmentHelper segmentHelper;

    private NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelId));
        }
        return nDataModel;
    }

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, String status, String sortBy, boolean reverse) {

        List<NDataflow> dataflowList = getDataflowManager(projectName).listAllDataflows(true);
        val dfManager = getDataflowManager(projectName);

        val resultMap = getModelQueryTimesMap("*", projectName, "*", 0, 0);

        List<NDataModelResponse> filterModels = new ArrayList<>();
        List<NDataModelResponse> brokenModels = new ArrayList<>();
        for (NDataflow dataflow : dataflowList) {
            val modelDesc = dataflow.getModel();
            if (dataflow.checkBrokenWithRelatedInfo()) {
                val nDataModelResponse = new NDataModelResponse();
                nDataModelResponse.setUuid(dataflow.getId());
                nDataModelResponse.setStatus(RealizationStatusEnum.BROKEN);
                nDataModelResponse.setAlias(modelDesc.getAlias());
                brokenModels.add(nDataModelResponse);
                continue;
            }
            boolean isModelNameMatch = isArgMatch(modelAlias, exactMatch, modelDesc.getAlias());
            boolean isModelOwnerMatch = isArgMatch(owner, exactMatch, modelDesc.getOwner());
            if (isModelNameMatch && isModelOwnerMatch) {
                RealizationStatusEnum modelStatus = getModelStatus(modelDesc.getUuid(), projectName);
                boolean isModelStatusMatch = StringUtils.isEmpty(status)
                        || (modelStatus != null && modelStatus.name().equalsIgnoreCase(status));

                if (isModelStatusMatch) {
                    NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
                    nDataModelResponse.setStatus(modelStatus);
                    nDataModelResponse.setStorage(dfManager.getDataflowByteSize(modelDesc.getUuid()));
                    if (resultMap.containsKey(modelDesc.getUuid())) {
                        nDataModelResponse.setUsage(resultMap.get(modelDesc.getUuid()).getQueryTimes());
                    }
                    filterModels.add(nDataModelResponse);
                }
            }
        }
        val sortModelResponses = sortModelResponses(sortBy, reverse, filterModels);
        if ((StringUtils.isBlank(status) || RealizationStatusEnum.BROKEN.toString().equalsIgnoreCase(status))
                && StringUtils.isBlank(modelAlias) && StringUtils.isBlank(owner)) {
            sortModelResponses.addAll(brokenModels);
        }
        return sortModelResponses;
    }

    private boolean isArgMatch(String valueToMatch, boolean exactMatch, String originValue) {
        return StringUtils.isEmpty(valueToMatch) || (exactMatch && originValue.equalsIgnoreCase(valueToMatch))
                || (!exactMatch && originValue.toLowerCase().contains(valueToMatch.toLowerCase()));
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
            Segments<NDataSegment> segments = getSegmentsByRange(modelDesc.getUuid(), projectName, "0",
                    "" + Long.MAX_VALUE);
            if (CollectionUtils.isNotEmpty(segments)) {
                NDataSegment lastSegment = segments.get(segments.size() - 1);
                nDataModelResponse.setLastBuildEnd(lastSegment.getSegRange().getEnd().toString());
            } else {
                nDataModelResponse.setLastBuildEnd("");

            }
        }
        return nDataModelResponse;
    }

    private RealizationStatusEnum getModelStatus(String modelId, String projectName) {
        val indexPlan = getIndexPlan(modelId, projectName);
        if (indexPlan != null) {
            return getDataflowManager(projectName).getDataflow(indexPlan.getUuid()).getStatus();
        } else {
            return null;
        }
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String sortBy, boolean reverse, String status) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        List<NDataSegmentResponse> segmentResponse = Lists.newArrayList();
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        val segs = getSegmentsByRange(modelId, project, start, end);
        for (NDataSegment segment : segs) {
            if (StringUtils.isNotEmpty(status)
                    && status.equalsIgnoreCase(segs.getSegmentStatusToDisplay(segment).toString())) {
                continue;
            }
            long segmentSize = dataflowManager.getSegmentSize(segment);
            NDataSegmentResponse nDataSegmentResponse = new NDataSegmentResponse(segment);
            nDataSegmentResponse.setBytesSize(segmentSize);
            nDataSegmentResponse.getAdditionalInfo().put(SEGMENT_PATH, dataflow.getSegmentHdfsPath(segment.getId()));
            nDataSegmentResponse.getAdditionalInfo().put(FILE_COUNT, dataflowManager.getSegmentFileCount(segment) + "");
            nDataSegmentResponse.setStatusToDisplay(dataflow.getSegments().getSegmentStatusToDisplay(segment));
            segmentResponse.add(nDataSegmentResponse);
        }
        sortSegments(segmentResponse, sortBy);
        if (reverse) {
            segmentResponse = Lists.reverse(segmentResponse);
        }
        return segmentResponse;
    }

    private void sortSegments(List<NDataSegmentResponse> segments, String sortBy) {
        var comp = Comparator.<NDataSegmentResponse> comparingLong(r -> -r.getCreateTimeUTC());

        switch (sortBy) {
        case CREATE_TIME:
            comp = Comparator.<NDataSegmentResponse> comparingLong(r -> -r.getCreateTimeUTC());
            break;
        case START_TIME:
            comp = Comparator
                    .<NDataSegmentResponse> comparingLong(r -> -Long.parseLong(r.getSegRange().getStart().toString()));
            break;
        case END_TIME:
            comp = Comparator
                    .<NDataSegmentResponse> comparingLong(r -> -Long.parseLong(r.getSegRange().getEnd().toString()));
            break;
        case STORAGE:
            comp = Comparator.<NDataSegmentResponse> comparingLong(r -> -r.getBytesSize());
            break;
        default:
            break;
        }
        Collections.sort(segments, comp);
    }

    public Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        val df = dataflowManager.getDataflow(modelId);
        SegmentRange filterRange = getSegmentRangeByModel(project, modelId, start, end);
        return df.getSegmentsByRange(filterRange);
    }

    public List<IndexEntityResponse> getAggIndices(String modelId, String project) {
        List<IndexEntity> indexEntities = getIndexEntities(modelId, project);
        List<IndexEntityResponse> result = new ArrayList<>();
        for (IndexEntity indexEntity : indexEntities) {
            if (indexEntity.getId() < IndexEntity.TABLE_INDEX_START_ID) {
                IndexEntityResponse indexEntityResponse = new IndexEntityResponse(indexEntity);
                result.add(indexEntityResponse);
            }
        }
        return result;
    }

    public List<IndexEntityResponse> getTableIndices(String modelId, String project) {
        List<IndexEntity> indexEntities = getIndexEntities(modelId, project);
        List<IndexEntityResponse> result = new ArrayList<IndexEntityResponse>();
        for (IndexEntity indexEntity : indexEntities) {
            if (indexEntity.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
                IndexEntityResponse indexEntityResponse = new IndexEntityResponse(indexEntity);
                result.add(indexEntityResponse);
            }
        }
        return result;
    }

    public List<IndexEntity> getIndexEntities(String modelId, String project) {
        val indexPlan = getIndexPlan(modelId, project);
        List<IndexEntity> cuboidDescs = new ArrayList<IndexEntity>();
        cuboidDescs.addAll(indexPlan.getAllIndexes());
        return cuboidDescs;
    }

    public IndexEntityResponse getCuboidById(String modelId, String project, Long cuboidId) {
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        IndexEntity cuboidDesc = indexPlan.getIndexEntity(cuboidId);
        return new IndexEntityResponse(cuboidDesc);
    }

    public String getModelJson(String modelId, String project) throws JsonProcessingException {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        return JsonUtil.writeValueAsIndentString(modelDesc);
    }

    public List<NSpanningTree> getModelRelations(String modelId, String project) {
        val indexPlan = getIndexPlan(modelId, project);
        List<NSpanningTree> result = new ArrayList<>();
        val allLayouts = Lists.<LayoutEntity> newArrayList();
        if (indexPlan.getRuleBasedIndex() != null) {
            val rule = indexPlan.getRuleBasedIndex();
            allLayouts.addAll(rule.genCuboidLayouts());
        }
        val autoLayouts = indexPlan.getWhitelistLayouts().stream()
                .filter(layout -> layout.getId() < IndexEntity.TABLE_INDEX_START_ID).collect(Collectors.toList());
        allLayouts.addAll(autoLayouts);
        val tree = NSpanningTreeFactory.fromLayouts(allLayouts, indexPlan.getUuid());
        result.add(tree);
        return result;
    }

    public List<NSpanningTreeResponse> getSimplifiedModelRelations(String modelId, String project) {
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        List<NSpanningTreeResponse> result = Lists.newArrayList();
        for (NSpanningTree spanningTree : getModelRelations(modelId, project)) {
            result.add(new NSpanningTreeResponse((NForestSpanningTree) spanningTree, model));
        }
        return result;
    }

    public List<RelatedModelResponse> getRelateModels(String project, String table, String modelId) {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        val dataflowManager = getDataflowManager(project);
        val models = dataflowManager.getTableOrientedModelsUsingRootTable(tableDesc);
        List<RelatedModelResponse> relatedModel = new ArrayList<>();
        val errorExecutables = getExecutableManager(project).getExecutablesByStatus(ExecutableState.ERROR);
        for (var dataModelDesc : models) {
            Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
            val model = dataModelDesc.getUuid();
            if (StringUtils.isEmpty(modelId)
                    || dataModelDesc.getAlias().toLowerCase().contains(modelId.toLowerCase())) {
                RelatedModelResponse relatedModelResponse = new RelatedModelResponse(dataModelDesc);
                Segments<NDataSegment> segments = getSegmentsByRange(model, project, "", "");
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

    private IndexPlan getIndexPlan(String modelId, String project) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        return indexPlanManager.getIndexPlan(modelId);
    }

    private void checkAliasExist(String modelId, String newAlias, String project) {
        List<NDataModel> models = getDataflowManager(project).listUnderliningDataModels();
        for (NDataModel model : models) {
            if (!StringUtils.isNotEmpty(modelId) && model.getUuid().equals(modelId)) {
                continue;
            } else if (model.getAlias().equals(newAlias)) {
                throw new BadRequestException("Model alias " + newAlias + " already exists!");
            }
        }
    }

    @Transaction(project = 1)
    public void dropModel(String modelId, String project) {
        val projectInstance = getProjectManager().getProject(project);
        Preconditions.checkState(MaintainModelType.MANUAL_MAINTAIN.equals(projectInstance.getMaintainModelType()));

        NDataModel dataModelDesc = getModelById(modelId, project);
        val cleaner = new ModelCleaner();
        cleaner.collect(dataModelDesc);
        cleaner.cleanup();
    }

    @Transaction(project = 1)
    public void purgeModel(String modelId, String project) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        val indexPlan = getIndexPlan(modelId, project);
        List<NDataSegment> segments = new ArrayList<>();
        if (indexPlan != null) {
            NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            segments.addAll(dataflow.getSegments());
            NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
            NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
            nDataflowUpdate.setToRemoveSegs(nDataSegments);
            dataflowManager.updateDataflow(nDataflowUpdate);
        }

    }

    @Transaction(project = 1)
    public void purgeModelManually(String modelId, String project) {
        NDataModel dataModelDesc = getModelById(modelId, project);
        if (dataModelDesc.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(
                    MODEL + dataModelDesc.getAlias() + "' is table oriented, can not purge the model!!");
        }
        purgeModel(modelId, project);
    }

    @Transaction(project = 2)
    public void cloneModel(String modelId, String newModelName, String project) {
        checkAliasExist("", newModelName, project);
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel dataModelDesc = getModelById(modelId, project);
        //copyForWrite nDataModel do init,but can not set new modelname
        NDataModel nDataModel;
        try {
            nDataModel = JsonUtil.readValue(JsonUtil.writeValueAsIndentString(dataModelDesc), NDataModel.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setAlias(newModelName);
        nDataModel.setLastModified(0L);
        nDataModel.setMvcc(-1);
        val newModel = dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
        cloneIndexPlan(modelId, project, nDataModel.getOwner(), newModel.getUuid());
    }

    private void cloneIndexPlan(String modelId, String project, String owner, String newModelId) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan copy = indexPlanManager.copy(indexPlan);
        copy.setUuid(newModelId);
        copy.setSegmentRangeStart(0L);
        copy.setSegmentRangeEnd(0L);
        copy.setLastModified(0L);
        copy.setMvcc(-1);
        indexPlanManager.createIndexPlan(copy);
        NDataflow nDataflow = new NDataflow();
        nDataflow.setStatus(RealizationStatusEnum.OFFLINE);
        dataflowManager.createDataflow(copy, owner);
    }

    @Transaction(project = 0)
    public void renameDataModel(String project, String modelId, String newAlias) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = getModelById(modelId, project);
        //rename
        checkAliasExist(modelId, newAlias, project);
        nDataModel.setAlias(newAlias);
        NDataModel modelUpdate = modelManager.copyForWrite(nDataModel);
        modelManager.updateDataModelDesc(modelUpdate);
    }

    @Transaction(project = 1)
    public void unlinkModel(String modelId, String project) {
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataModelManager dataModelManager = getDataModelManager(project);

        NDataModel nDataModel = getModelById(modelId, project);
        if (nDataModel.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new IllegalStateException("Model " + nDataModel.getAlias() + " is model based, can not unlink it!");
        } else {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager
                    .getDataLoadingRange(nDataModel.getRootFactTable().getTableIdentity());
            NDataModel modelUpdate = dataModelManager.copyForWrite(nDataModel);
            if (dataLoadingRange != null) {
                val segmentConfig = dataLoadingRange.getSegmentConfig();
                if (segmentConfig != null) {
                    modelUpdate.setSegmentConfig(segmentConfig);
                }
            }
            modelUpdate.setManagementType(ManagementType.MODEL_BASED);
            dataModelManager.updateDataModelDesc(modelUpdate);
            return;
        }
    }

    @Transaction(project = 1)
    public void updateDataModelStatus(String modelId, String project, String status) {
        NDataModel nDataModel = getModelById(modelId, project);
        IndexPlan indexPlan = getIndexPlan(nDataModel.getUuid(), project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        checkDataflowStatus(dataflow, modelId);
        boolean needChangeStatus = (status.equals(RealizationStatusEnum.OFFLINE.name())
                && dataflow.getStatus().equals(RealizationStatusEnum.ONLINE))
                || (status.equals(RealizationStatusEnum.ONLINE.name())
                        && dataflow.getStatus().equals(RealizationStatusEnum.OFFLINE));
        if (needChangeStatus) {
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            if (status.equals(RealizationStatusEnum.OFFLINE.name())) {
                nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            } else if (status.equals(RealizationStatusEnum.ONLINE.name())) {
                nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            }
            dataflowManager.updateDataflow(nDataflowUpdate);
        }
    }

    private void checkDataflowStatus(NDataflow dataflow, String modelId) {
        if (dataflow.getStatus().equals(RealizationStatusEnum.BROKEN)) {
            throw new BadRequestException("DescBroken model " + modelId + " can not online or offline!");
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelId, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelId).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    public boolean isModelsUsingTable(String table, String project) {
        return getDataflowManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table))
                .size() > 0;
    }

    public List<NDataModel> getModelsUsingTable(String table, String project) {
        return getDataflowManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table));
    }

    public RefreshAffectedSegmentsResponse getAffectedSegmentsResponse(String project, String table, String start,
            String end, ManagementType managementType) {
        Segments<NDataSegment> segments = new Segments<>();
        val dfManager = getDataflowManager(project);
        RefreshAffectedSegmentsResponse response = new RefreshAffectedSegmentsResponse();
        long byteSize = 0L;
        List<RelatedModelResponse> models = getRelateModels(project, table, "");
        for (NDataModel model : models) {
            if (model.getManagementType().equals(managementType)) {
                segments.addAll(getSegmentsByRange(model.getUuid(), project, start, end));
            }
        }

        if (CollectionUtils.isEmpty(segments)) {
            throw new BadRequestException("No segments to refresh, please select new range and try again!");
        } else {
            String affectedStart = segments.getFirstSegment().getSegRange().getStart().toString();
            String affectedEnd = segments.getLatestReadySegment().getSegRange().getEnd().toString();
            for (NDataSegment segment : segments) {
                byteSize += dfManager.getSegmentSize(segment);
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
            Segments<NDataSegment> segments = getSegmentsByRange(model.getUuid(), project, refreshStart, refreshEnd);
            if (segments.getBuildingSegments().size() > 0) {
                throw new BadRequestException(
                        "Can not refresh, some segments is building within the range you want to refresh!");
            }
        }

        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        SegmentRange segmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(refreshStart, refreshEnd);
        segmentHelper.refreshRelatedModelSegments(project, table, segmentRange);
    }

    @Transaction(project = 0)
    public NDataModel createModel(String project, ModelRequest modelRequest) throws Exception {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof UserDetails) {
            modelRequest.setOwner(((UserDetails) authentication.getPrincipal()).getUsername());
        }
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);
        checkAliasExist(modelRequest.getUuid(), modelRequest.getAlias(), project);
        checkModelRequest(modelRequest);
        //remove some attributes in modelResponse to fit NDataModel
        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        if (prj.getMaintainModelType().equals(MaintainModelType.AUTO_MAINTAIN)
                || dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException("Can not create model manually in SQL acceleration project!");
        }
        preProcessBeforeModelSave(dataModel, project);
        val model = getDataModelManager(project).createDataModelDesc(dataModel, dataModel.getOwner());
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(model.getUuid());
        indexPlanManager.createIndexPlan(indexPlan);
        val df = dataflowManager.createDataflow(indexPlan, model.getOwner());
        SegmentRange range = null;
        if (model.getPartitionDesc() == null
                || StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        } else if (PushDownUtil.needPushdown(modelRequest.getStart(), modelRequest.getEnd())) {
            //load existing data
            val pushDownResponse = getMaxAndMinTimeInPartitionColumnByPushdown(project, model.getUuid());
            val start = PushDownUtil.calcStart(pushDownResponse.getFirst(), null);
            range = getSegmentRangeByModel(project, model.getUuid(), start, pushDownResponse.getSecond());
        } else {
            range = getSegmentRangeByModel(project, model.getUuid(), modelRequest.getStart(), modelRequest.getEnd());
        }
        proposeAndSaveDateFormatIfNotExist(project, model.getUuid());
        if (range != null) {
            dataflowManager.fillDfManually(df, Lists.newArrayList(range));
        }
        return getDataModelManager(project).getDataModelDesc(model.getUuid());
    }

    private void checkModelRequest(ModelRequest request) {
        checkModelDimensions(request);
        checkModelMeasures(request);
        checkModelJoinConditions(request);
    }

    private void checkModelDimensions(ModelRequest request) {
        Set<String> dimensionNames = new HashSet<>();

        for (NDataModel.NamedColumn dimension : request.getDimensions()) {
            // check if the dimension name is valid
            if (!StringUtils.containsOnly(dimension.getName(), VALID_NAME_FOR_MODEL_DIMENSION_MEASURE))
                throw new IllegalArgumentException(String.format(msg.getINVALID_DIMENSION_NAME(), dimension.getName()));

            // check duplicate dimension names
            if (dimensionNames.contains(dimension.getName()))
                throw new IllegalArgumentException(
                        String.format(msg.getDUPLICATE_DIMENSION_NAME(), dimension.getName()));

            dimensionNames.add(dimension.getName());
        }
    }

    private void checkModelMeasures(ModelRequest request) {
        Set<String> measureNames = new HashSet<>();
        Set<SimplifiedMeasure> measures = new HashSet<>();

        for (SimplifiedMeasure measure : request.getSimplifiedMeasures()) {
            // check if the measure name is valid
            if (!StringUtils.containsOnly(measure.getName(), VALID_NAME_FOR_MODEL_DIMENSION_MEASURE))
                throw new IllegalArgumentException(String.format(msg.getINVALID_MEASURE_NAME(), measure.getName()));

            // check duplicate measure names
            if (measureNames.contains(measure.getName()))
                throw new IllegalArgumentException(String.format(msg.getDUPLICATE_MEASURE_NAME(), measure.getName()));

            // check duplicate measure definitions
            if (measures.contains(measure))
                throw new IllegalArgumentException(
                        String.format(msg.getDUPLICATE_MEASURE_DEFINITION(), measure.getName()));

            measureNames.add(measure.getName());
            measures.add(measure);
        }
    }

    private void checkModelJoinConditions(ModelRequest request) {
        for (JoinTableDesc joinTableDesc : request.getJoinTables()) {
            Set<Pair<String, String>> joinKeys = new HashSet<>();
            JoinDesc joinDesc = joinTableDesc.getJoin();
            int size = joinDesc.getPrimaryKey().length;
            String[] primaryKeys = joinDesc.getPrimaryKey();
            String[] foreignKey = joinDesc.getForeignKey();

            for (int i = 0; i < size; i++) {
                if (joinKeys.contains(Pair.newPair(primaryKeys[i], foreignKey[i])))
                    throw new IllegalArgumentException(
                            String.format(msg.getDUPLICATE_JOIN_CONDITIONS(), primaryKeys[i], foreignKey[i]));

                joinKeys.add(Pair.newPair(primaryKeys[i], foreignKey[i]));
            }
        }
    }

    private void proposeAndSaveDateFormatIfNotExist(String project, String modelId) throws Exception {
        val modelManager = getDataModelManager(project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        val partitionDesc = modelDesc.getPartitionDesc();
        if (partitionDesc == null || StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
                || StringUtils.isNotEmpty(partitionDesc.getPartitionDateFormat()))
            return;
        String partitionColumn = modelDesc.getPartitionDesc().getPartitionDateColumn();

        val date = PushDownUtil.getFormatIfNotExist(modelDesc.getRootFactTableName(), partitionColumn);
        val format = DateFormat.proposeDateFormat(date);
        modelManager.updateDataModel(modelId, model -> {
            model.getPartitionDesc().setPartitionDateFormat(format);
        });
    }

    private Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String model)
            throws Exception {
        val modelManager = getDataModelManager(project);
        val modelDesc = modelManager.getDataModelDesc(model);
        val table = modelDesc.getRootFactTableName();

        val minAndMaxTime = PushDownUtil.getMaxAndMinTime(modelDesc.getPartitionDesc().getPartitionDateColumn(), table);

        String dateFormat;
        if (StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateFormat())) {
            val copy = modelManager.copyForWrite(modelDesc);
            dateFormat = DateFormat.proposeDateFormat(minAndMaxTime.getFirst());
            copy.getPartitionDesc().setPartitionDateFormat(dateFormat);
            modelManager.updateDataModelDesc(copy);
        } else {
            dateFormat = modelDesc.getPartitionDesc().getPartitionDateFormat();
        }

        return new Pair<>(DateFormat.getFormattedDate(minAndMaxTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(minAndMaxTime.getSecond(), dateFormat));
    }

    @Transaction(project = 0)
    public void buildSegmentsManually(String project, String modelId, String start, String end) throws Exception {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (!modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new BadRequestException(
                    "Table oriented model '" + modelDesc.getAlias() + "' can not build segments manually!");
        }
        val indexPlan = getIndexPlan(modelId, project);
        if (indexPlan == null) {
            throw new BadRequestException(
                    "Can not build segments, please define table index or aggregate index first!");
        }

        SegmentRange segmentRangeToBuild = null;

        TableDesc table = getTableManager(project).getTableDesc(modelDesc.getRootFactTableName());

        val df = getDataflowManager(project).getDataflow(modelId);

        if (modelDesc.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateColumn())) {
            //if full seg exists,refresh it
            val segs = df.getSegments(SegmentStatusEnum.READY);
            if (segs.size() == 1 && segs.get(0).getSegRange().isInfinite()) {
                refreshSegmentById(modelId, project,
                        Lists.newArrayList(df.getSegments().get(0).getId()).toArray(new String[0]));
                return;
            }
            //build Full seg
            segmentRangeToBuild = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        } else if (PushDownUtil.needPushdown(start, end)) {
            val response = getMaxAndMinTimeInPartitionColumnByPushdown(project, modelId);
            start = PushDownUtil.calcStart(response.getFirst(), df.getCoveredRange());
            if (Long.parseLong(start) >= Long.parseLong(response.getSecond())) {
                throw new BadRequestException("Entire existing data has already loaded!");
            }
            segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, response.getSecond());
        } else {
            segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);
        }

        proposeAndSaveDateFormatIfNotExist(project, modelId);

        checkSegmentToBuildOverlapsBuilt(project, modelId, segmentRangeToBuild);

        NDataSegment newSegment = getDataflowManager(project).appendSegment(df, segmentRangeToBuild);

        EventManager eventManager = getEventManager(project);

        AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setSegmentId(newSegment.getId());
        addSegmentEvent.setModelId(modelId);
        addSegmentEvent.setJobId(UUID.randomUUID().toString());
        addSegmentEvent.setOwner(getUsername());
        eventManager.post(addSegmentEvent);

        PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setSegmentId(newSegment.getId());
        postAddSegmentEvent.setModelId(modelId);
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
        Segments<NDataSegment> segments = getSegmentsByRange(model, project, "0", "" + Long.MAX_VALUE);
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

        String modelAlias = modelDesc.getAlias();

        if (StringUtils.isEmpty(modelAlias)) {
            logger.info("Model name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", modelDesc.getUuid());
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), modelAlias));
        }
    }

    public ComputedColumnUsageResponse getComputedColumnUsages(String project) {
        ComputedColumnUsageResponse ret = new ComputedColumnUsageResponse();
        List<NDataModel> models = getDataflowManager(project).listUnderliningDataModels();
        for (NDataModel model : models) {
            for (ComputedColumnDesc computedColumnDesc : model.getComputedColumnDescs()) {
                ret.addUsage(computedColumnDesc, model.getUuid());
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
                getDataflowManager(project).listUnderliningDataModels(), project);

        if (dataModelDesc.isSeekingCCAdvice()) {
            // if it's seeking for advice, it should have thrown exceptions by far
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
                getDataflowManager(project).listUnderliningDataModels(), project);

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = KapQueryUtil.massageComputedColumn(model, project, ccDesc);
            ccDesc.setInnerExpression(ccExpression);
        }
    }

    @Transaction(project = 0)
    public void updateModelDataCheckDesc(String project, String modelId, long checkOptions, long faultThreshold,
            long faultActions) {

        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelId);
        if (dataModel == null) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelId));
        }

        dataModel.setDataCheckDesc(DataCheckDesc.valueOf(checkOptions, faultThreshold, faultActions));
        getDataModelManager(project).updateDataModelDesc(dataModel);
    }

    @Transaction(project = 1)
    public void deleteSegmentById(String model, String project, String[] ids) {
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(
                    MODEL + dataModel.getAlias() + "' is table oriented, can not remove segments manually!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsOverlapWithBuilding(model, project, ids);
        checkDeleteSegmentLegally(model, project, ids);
        val indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Set<String> idsToDelete = Sets.newHashSet();
        for (String id : ids) {
            if (dataflow.getSegment(id) != null) {
                idsToDelete.add(id);
            } else {
                throw new IllegalArgumentException(
                        String.format("segment %s not found on model %s", id, dataflow.getModelAlias()));
            }
        }
        segmentHelper.removeSegment(project, dataflow.getUuid(), idsToDelete);
    }

    private void checkSegmentsOverlapWithBuilding(String model, String project, String[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
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
        val indexPlan = getIndexPlan(model, project);
        List<String> idsToDelete = Lists.newArrayList(ids);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
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
    public void refreshSegmentById(String modelId, String project, String[] ids) {
        NDataflowManager dfMgr = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());

        checkSegmentsOverlapWithBuilding(modelId, project, ids);

        for (String id : ids) {
            NDataSegment segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format("segment %s not found on model %s", id, df.getModelAlias()));
            }

            NDataSegment newSeg = dfMgr.refreshSegment(df, segment.getSegRange());

            val event = new RefreshSegmentEvent();
            event.setSegmentId(newSeg.getId());
            event.setModelId(modelId);
            event.setOwner(getUsername());
            event.setJobId(UUID.randomUUID().toString());
            eventManager.post(event);

            val event2 = new PostMergeOrRefreshSegmentEvent();
            event2.setSegmentId(newSeg.getId());
            event2.setModelId(modelId);
            event2.setOwner(getUsername());
            event2.setJobId(event.getJobId());
            eventManager.post(event2);

        }
    }

    @Transaction(project = 0)
    public void updateDataModelSemantic(String project, ModelRequest request) throws Exception {
        checkModelRequest(request);
        val modelManager = getDataModelManager(project);
        val cubeManager = getIndexPlanManager(project);
        val dataflowManager = getDataflowManager(project);
        val originModel = modelManager.getDataModelDesc(request.getUuid());

        val df = dataflowManager.getDataflow(request.getUuid());
        val copyModel = modelManager.copyForWrite(originModel);
        semanticUpdater.updateModelColumns(copyModel, request);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), request.getProject())
                .getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(), project);

        val indexPlan = cubeManager.getIndexPlan(request.getUuid());
        // check agg group contains removed dimensions
        val rule = indexPlan.getRuleBasedIndex();
        if (rule != null && !copyModel.getEffectiveDimenionsMap().keySet().containsAll(rule.getDimensions())) {
            val allDimensions = rule.getDimensions();
            val dimensionNames = allDimensions.stream()
                    .filter(id -> !copyModel.getEffectiveDimenionsMap().containsKey(id))
                    .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());

            throw new IllegalStateException("model " + indexPlan.getModel().getUuid()
                    + "'s agg group still contains dimensions " + StringUtils.join(dimensionNames, ","));
        }

        preProcessBeforeModelSave(copyModel, project);
        modelManager.updateDataModelDesc(copyModel);

        val newModel = modelManager.getDataModelDesc(request.getId());

        if (needChangeFormat(originModel, newModel)) {
            modelManager.updateDataModel(request.getId(), copyForWrite -> {
                copyForWrite.getPartitionDesc().setPartitionDateFormat("");
            });
        }
        proposeAndSaveDateFormatIfNotExist(project, request.getId());
        semanticUpdater.handleSemanticUpdate(project, request.getId(), originModel);
    }

    private boolean needChangeFormat(NDataModel oriModel, NDataModel newModel) {
        if (newModel.getPartitionDesc() == null) {
            return false;
        }
        if (oriModel.getPartitionDesc() == null) {
            return true;
        }
        if (!StringUtil.equals(newModel.getPartitionDesc().getPartitionDateColumn(),
                oriModel.getPartitionDesc().getPartitionDateColumn())) {
            return true;
        }
        return false;
    }

    public NDataModel convertToDataModel(ModelRequest modelDesc) {
        return semanticUpdater.convertToDataModel(modelDesc);
    }

    public AffectedModelsResponse getAffectedModelsByToggleTableType(String tableName, String project, boolean fact) {
        val dataflowManager = getDataflowManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        if (fact) {
            checkSingleIncrementingLoadingTable(project, tableName);
        }
        val response = new AffectedModelsResponse();
        val models = dataflowManager.getTableOrientedModelsUsingRootTable(table).stream()
                .map(RootPersistentEntity::getUuid).collect(Collectors.toList());
        var size = 0;
        response.setModels(models);
        for (val model : models) {
            size += dataflowManager.getDataflowByteSize(model);
        }
        response.setByteSize(size);
        return response;
    }

    public void checkSingleIncrementingLoadingTable(String project, String tableName) {
        val dataflowManager = getDataflowManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        val modelsUsingTable = dataflowManager.getModelsUsingTable(table);
        for (val modelDesc : modelsUsingTable) {
            if (!modelDesc.getRootFactTable().getTableDesc().getIdentity().equals(tableName)
                    || modelDesc.isJoinTable(tableName)) {
                throw new BadRequestException("Can not set table '" + tableName
                        + "' incremental loading, due to another incremental loading table existed in model '"
                        + modelDesc.getAlias() + "'!");
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

        val resultMap = getModelQueryTimesMap(suite, project, model, start, end);
        for (val modelInfoResponse : modelInfoList) {
            if (resultMap.containsKey(modelInfoResponse.getModel())) {
                modelInfoResponse.setQueryTimes(resultMap.get(modelInfoResponse.getModel()).getQueryTimes());
            }
        }

        return modelInfoList;
    }

    private Map<String, QueryTimesResponse> getModelQueryTimesMap(String suite, String project, String model,
            long start, long end) {
        List<QueryTimesResponse> result = getQueryHistoryDao(project).getQueryTimesByModel(suite, model, start, end,
                QueryTimesResponse.class);
        return result.stream()
                .collect(Collectors.toMap(QueryTimesResponse::getModel, queryTimesResponse -> queryTimesResponse));
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

    public List<ModelConfigResponse> getModelConfig(String project) {
        val responseList = Lists.<ModelConfigResponse> newArrayList();
        getDataflowManager(project).listUnderliningDataModels().forEach(dataModel -> {
            val response = new ModelConfigResponse();
            response.setModel(dataModel.getUuid());
            response.setAlias(dataModel.getAlias());
            val segmentConfig = dataModel.getSegmentConfig();
            response.setAutoMergeEnabled(segmentConfig.getAutoMergeEnabled());
            response.setAutoMergeTimeRanges(segmentConfig.getAutoMergeTimeRanges());
            response.setVolatileRange(segmentConfig.getVolatileRange());
            response.setRetentionRange(segmentConfig.getRetentionRange());
            response.setConfigLastModified(dataModel.getConfigLastModified());
            response.setConfigLastModifier(dataModel.getConfigLastModifier());
            val indexPlan = getIndexPlan(dataModel.getUuid(), project);
            if (indexPlan != null) {
                val overrideProps = indexPlan.getOverrideProps();
                response.getOverrideProps().putAll(overrideProps);
            }
            responseList.add(response);
        });
        return responseList;
    }

    @Transaction(project = 0)
    public void updateModelConfig(String project, String modelId, ModelConfigRequest request) {
        val dataModelManager = getDataModelManager(project);
        dataModelManager.updateDataModel(modelId, copyForWrite -> {
            val segmentConfig = copyForWrite.getSegmentConfig();
            segmentConfig.setAutoMergeEnabled(request.getAutoMergeEnabled());
            segmentConfig.setAutoMergeTimeRanges(request.getAutoMergeTimeRanges());
            segmentConfig.setVolatileRange(request.getVolatileRange());
            segmentConfig.setRetentionRange(request.getRetentionRange());

            copyForWrite.setConfigLastModified(System.currentTimeMillis());
            copyForWrite.setConfigLastModifier(getUsername());
        });
        val indexPlan = getIndexPlan(modelId, project);
        val indexPlanManager = getIndexPlanManager(project);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.setOverrideProps(request.getOverrideProps());
        });
    }

    public void reloadModels(List<String> models, String project) {
        if (CollectionUtils.isNotEmpty(models)) {
            models.forEach(modelId -> getDataModelManager(project).reload(modelId));
        }
    }
}