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

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kyligence.kap.secondstorage.metadata.TableData;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SecondStorageConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageModelCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.UpdateImpact;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.adhocquery.PushDownConverterKeyWords;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.event.ModelAddEvent;
import io.kyligence.kap.common.event.ModelDropEvent;
import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.AddTableNameSqlVisitor;
import io.kyligence.kap.metadata.acl.AclTCRDigest;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.acl.NDataModelAclParams;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.JoinedFlatTable;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.model.schema.AffectedModelContext;
import io.kyligence.kap.metadata.model.util.MultiPartitionUtil;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggGroupResponse;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.ComputedColumnCheckResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.FusionModelResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.InvalidIndexesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.MultiPartitionValueResponse;
import io.kyligence.kap.rest.response.NCubeDescResponse;
import io.kyligence.kap.rest.response.NDataModelOldParams;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.response.SegmentRangeResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.params.FullBuildSegmentParams;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.ModelQueryParams;
import io.kyligence.kap.rest.util.ModelTriple;
import io.kyligence.kap.rest.util.ModelUtils;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import io.kyligence.kap.secondstorage.util.SecondStorageJobUtil;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import io.kyligence.kap.streaming.event.StreamingJobDropEvent;
import io.kyligence.kap.streaming.event.StreamingJobKillEvent;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.BISyncTool;
import io.kyligence.kap.tool.bisync.SyncContext;
import lombok.Setter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("modelService")
public class ModelService extends BasicService implements TableModelSupporter, ProjectModelSupporter {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final String LAST_MODIFY = "last_modify";
    public static final String REC_COUNT = "recommendations_count";

    public static final String VALID_NAME_FOR_MODEL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";

    public static final String VALID_NAME_FOR_DIMENSION = "^[\\u4E00-\\u9FA5a-zA-Z0-9 _\\-()%?（）]+$";

    public static final String VALID_NAME_FOR_MEASURE = "^[\\u4E00-\\u9FA5a-zA-Z0-9 _\\-()%?（）.]+$";

    private static final List<String> MODEL_CONFIG_BLOCK_LIST = Lists.newArrayList("kylin.index.rule-scheduler-data");

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpdater;

    @Setter
    @Autowired
    @Qualifier("segmentHelper")
    private SegmentHelperSupporter segmentHelper;

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private AccessService accessService;

    @Setter
    @Autowired
    private ModelQuerySupporter modelQuerySupporter;

    @Setter
    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    @Setter
    @Autowired
    private List<ModelChangeSupporter> modelChangeSupporters = Lists.newArrayList();

    public NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }
        return nDataModel;
    }

    public NDataModel getModelByAlias(String modelAlias, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (null == nDataModel) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        return nDataModel;
    }

    /**
     * for 3x rest api
     * @param modelList
     * @return
     */
    public List<NDataModel> addOldParams(String project, List<NDataModel> modelList) {
        val executables = getAllRunningExecutable(project);
        modelList.forEach(model -> {
            NDataModelOldParams oldParams = new NDataModelOldParams();
            oldParams.setName(model.getAlias());
            oldParams.setJoinTables(model.getJoinTables());

            if (!model.isBroken()) {
                addOldSegmentParams(model, oldParams, executables);
            }

            if (model instanceof NDataModelResponse) {
                oldParams.setProjectName(model.getProject());
                oldParams.setSizeKB(((NDataModelResponse) model).getStorage() / 1024);
                ((NDataModelResponse) model).setOldParams(oldParams);
            } else if (model instanceof RelatedModelResponse) {
                ((RelatedModelResponse) model).setOldParams(oldParams);
            }
        });

        return modelList;
    }

    private void addOldSegmentParams(NDataModel model, NDataModelOldParams oldParams,
            List<AbstractExecutable> executables) {
        List<NDataSegmentResponse> segments = getSegmentsResponse(model.getId(), model.getProject(), "1",
                String.valueOf(Long.MAX_VALUE - 1), null, executables, LAST_MODIFY, true);
        calculateRecordSizeAndCount(segments, oldParams);

        if (model instanceof NDataModelResponse) {
            ((NDataModelResponse) model).setSegments(segments);
            ((NDataModelResponse) model).setHasSegments(
                    ((NDataModelResponse) model).isHasSegments() || CollectionUtils.isNotEmpty(segments));
        }

        if (model instanceof FusionModelResponse) {
            FusionModel fusionModel = getFusionModelManager(model.getProject()).getFusionModel(model.getId());
            NDataModel batchModel = fusionModel.getBatchModel();
            if (!batchModel.isBroken()) {
                List<NDataSegmentResponse> batchSegments = getSegmentsResponse(batchModel.getUuid(), model.getProject(),
                        "1", String.valueOf(Long.MAX_VALUE - 1), null, executables, LAST_MODIFY, true);
                calculateRecordSizeAndCount(batchSegments, oldParams);
                ((FusionModelResponse) model).setBatchSegments(batchSegments);
            }
        }
    }

    private void calculateRecordSizeAndCount(List<NDataSegmentResponse> segments, NDataModelOldParams oldParams) {
        long inputRecordCnt = oldParams.getInputRecordCnt();
        long inputRecordSizeBytes = oldParams.getInputRecordSizeBytes();
        for (NDataSegmentResponse segment : segments) {
            long sourceRows = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getSourceRows)
                    .max(Long::compareTo).orElse(0L);
            inputRecordCnt += sourceRows;
            inputRecordSizeBytes += segment.getSourceBytesSize();

            NDataSegmentResponse.OldParams segmentOldParams = new NDataSegmentResponse.OldParams();
            segmentOldParams.setSizeKB(segment.getBytesSize() / 1024);
            segmentOldParams.setInputRecords(sourceRows);
            segment.setOldParams(segmentOldParams);
        }
        oldParams.setInputRecordCnt(inputRecordCnt);
        oldParams.setInputRecordSizeBytes(inputRecordSizeBytes);
    }

    @VisibleForTesting
    public Set<String> getAllProjects() {
        return projectService.getReadableProjects().stream().map(ProjectInstance::getName).collect(Collectors.toSet());
    }

    @VisibleForTesting
    public boolean isProjectNotExist(String project) {
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(project, true);
        return CollectionUtils.isEmpty(projectInstances);
    }

    /**
     * for 3x rest api
     */
    public NDataModelResponse getCube(String modelAlias, String projectName) {
        if (Objects.nonNull(projectName)) {
            List<NDataModelResponse> cubes = getCubes(modelAlias, projectName);
            if (!CollectionUtils.isEmpty(cubes)) {
                return cubes.get(0);
            }
        } else {
            for (String project : getAllProjects()) {
                List<NDataModelResponse> cubes = getCubes(modelAlias, project);
                if (!CollectionUtils.isEmpty(cubes)) {
                    return cubes.get(0);
                }
            }
        }

        return null;
    }

    /**
     * for 3x rest api
     * @param modelAlias
     * @param projectName
     * @return
     */
    public List<NDataModelResponse> getCubes(String modelAlias, String projectName) {
        if (Objects.nonNull(projectName)) {
            return getCubes0(modelAlias, projectName);
        }

        List<NDataModelResponse> cubes = Lists.newArrayList();
        for (String project : getAllProjects()) {
            cubes.addAll(getCubes0(modelAlias, project));
        }

        return cubes;
    }

    private boolean isAggGroupIncludeAllJoinCol(List<Set<String>> aggIncludes, List<String> needCols) {
        for (Set<String> includes : aggIncludes) {// if one of agggroup contains all join column then the column would return
            boolean allIn = includes.containsAll(needCols);
            if (allIn) {
                return true;
            }
        }
        return false;
    }

    private List<NCubeDescResponse.Dimension3X> getDimension3XES(IndexPlan indexPlan, NDataModelResponse cube,
            List<AggGroupResponse> aggGroupResponses, NDataModel dataModel) {
        String rootFactTable = cube.getRootFactTableName();
        List<NCubeDescResponse.Dimension3X> dims = new ArrayList<>();
        HashMap<String, String> fk2Pk = Maps.newHashMap();
        cube.getJoinTables().forEach(join -> {
            String[] pks = join.getJoin().getPrimaryKey();
            String[] fks = join.getJoin().getForeignKey();
            for (int i = 0; i < pks.length; ++i)
                fk2Pk.put(fks[i], pks[i]);
        });

        HashMap<String, List<String>> tableToAllDim = Maps.newHashMap();
        cube.getNamedColumns().forEach(namedColumn -> {
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            String table = aliasDotColumn.split("\\.")[0];
            String column = aliasDotColumn.split("\\.")[1];
            if (!tableToAllDim.containsKey(table)) {
                tableToAllDim.put(table, Lists.newArrayList());
            }
            tableToAllDim.get(table).add(column);
        });

        Set<String> allAggDim = Sets.newHashSet();//table.col
        indexPlan.getRuleBasedIndex().getDimensions().stream().map(x -> dataModel.getEffectiveDimensions().get(x))
                .forEach(x -> allAggDim.add(x.getIdentity()));

        List<Set<String>> aggIncludes = new ArrayList<>();
        aggGroupResponses.forEach(agg -> aggIncludes.add(Sets.newHashSet(agg.getIncludes())));

        cube.getNamedColumns().forEach(namedColumn -> {
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            String table = aliasDotColumn.split("\\.")[0];
            boolean isRootFactTable = rootFactTable.endsWith("." + table);
            if (isRootFactTable) {
                if (allAggDim.contains(aliasDotColumn)) {
                    dims.add(new NCubeDescResponse.Dimension3X(namedColumn, false));
                }
            } else {
                List<String> needCols = new ArrayList<>();
                List<String> dimTableCol = tableToAllDim.get(table);
                dimTableCol.stream().filter(x -> fk2Pk.containsKey(table + "." + x)).forEach(x -> {
                    needCols.add(x);
                    needCols.add(fk2Pk.get(x));
                });
                if (isAggGroupIncludeAllJoinCol(aggIncludes, needCols)) {
                    boolean isDerived = !allAggDim.contains(aliasDotColumn);
                    dims.add(new NCubeDescResponse.Dimension3X(namedColumn, isDerived));
                }
            }
        });
        return dims;
    }

    public NCubeDescResponse getCubeWithExactModelName(String modelAlias, String projectName) {
        NDataModel dataModel = getDataModelManager(projectName).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        NDataModelResponse cube = new NDataModelResponse(dataModel);
        NCubeDescResponse result = new NCubeDescResponse();

        result.setUuid(cube.getUuid());
        result.setName(cube.getAlias());
        result.setMeasures(
                cube.getMeasures().stream().map(NCubeDescResponse.Measure3X::new).collect(Collectors.toList()));

        IndexPlan indexPlan = getIndexPlan(result.getUuid(), projectName);
        if (!dataModel.isBroken() && indexPlan.getRuleBasedIndex() != null) {
            List<AggGroupResponse> aggGroupResponses = indexPlan.getRuleBasedIndex().getAggregationGroups().stream()
                    .map(x -> new AggGroupResponse(dataModel, x)).collect(Collectors.toList());
            result.setAggregationGroups(aggGroupResponses);
            result.setDimensions(getDimension3XES(indexPlan, cube, aggGroupResponses, dataModel));
        } else {
            result.setAggregationGroups(new ArrayList<>());
            result.setDimensions(new ArrayList<>());
        }

        return result;
    }

    /**
     * for 3x rest api
     * @param modelAlias
     * @param project
     * @return
     */
    public List<NDataModelResponse> getCubes0(String modelAlias, String project) {
        Preconditions.checkNotNull(project);

        List<NDataModelResponse> modelResponseList = getModels(modelAlias, project, true, null, null, LAST_MODIFY,
                true);

        modelResponseList.forEach(modelResponse -> {
            NDataModelOldParams oldParams = new NDataModelOldParams();

            long inputRecordCnt = 0L;
            long inputRecordSizeBytes = 0L;
            if (!modelResponse.isModelBroken()) {
                List<NDataSegmentResponse> segments = getSegmentsResponse(modelResponse.getId(), project, "1",
                        String.valueOf(Long.MAX_VALUE - 1), null, LAST_MODIFY, true);
                for (NDataSegmentResponse segment : segments) {
                    long sourceRows = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getSourceRows)
                            .max(Long::compareTo).orElse(0L);
                    inputRecordCnt += sourceRows;
                    inputRecordSizeBytes += segment.getSourceBytesSize();

                    NDataSegmentResponse.OldParams segmentOldParams = new NDataSegmentResponse.OldParams();
                    segmentOldParams.setSizeKB(segment.getBytesSize() / 1024);
                    segmentOldParams.setInputRecords(sourceRows);
                    segment.setOldParams(segmentOldParams);
                }

                modelResponse.setSegments(segments);
                modelResponse.setHasSegments(modelResponse.isHasSegments() || CollectionUtils.isNotEmpty(segments));

            }

            oldParams.setName(modelResponse.getAlias());
            oldParams.setProjectName(project);
            oldParams.setStreaming(false);
            oldParams.setSizeKB(modelResponse.getStorage() / 1024);
            oldParams.setInputRecordSizeBytes(inputRecordSizeBytes);
            oldParams.setInputRecordCnt(inputRecordCnt);
            oldParams.setJoinTables(modelResponse.getJoinTables());
            modelResponse.setOldParams(oldParams);
        });

        return modelResponseList;
    }

    private boolean isListContains(List<String> status, ModelStatusToDisplayEnum modelStatus) {
        return status == null || status.isEmpty() || (modelStatus != null && status.contains(modelStatus.name()));
    }

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, List<String> status, String sortBy, boolean reverse) {
        return getModels(modelAlias, projectName, exactMatch, owner, status, sortBy, reverse, null, null, null);
    }

    public List<NDataModelResponse> getSCD2ModelsByStatus(final String projectName, final List<String> status) {
        return getModels(null, projectName, false, null, status, LAST_MODIFY, true, null, null, null).stream()
                .filter(SCD2CondChecker.INSTANCE::isScd2Model).collect(Collectors.toList());
    }

    public List<String> getSCD2ModelsAliasByStatus(final String projectName, final List<String> status) {
        return getSCD2ModelsByStatus(projectName, status).stream().map(NDataModel::getAlias)
                .collect(Collectors.toList());
    }

    private List<String> getSCD2Models(final String projectName) {
        return getSCD2ModelsByStatus(projectName, null).stream().map(NDataModel::getId).collect(Collectors.toList());
    }

    public List<String> getModelNonOffOnlineStatus() {
        return Arrays.asList(ModelStatusToDisplayEnum.ONLINE.name(), ModelStatusToDisplayEnum.WARNING.name(),
                ModelStatusToDisplayEnum.LAG_BEHIND.name());
    }

    public List<String> getMultiPartitionModelsAlias(final String projectName, final List<String> status) {
        return getMultiPartitionModelsByStatus(projectName, status).stream().map(NDataModel::getAlias)
                .collect(Collectors.toList());
    }

    public List<NDataModelResponse> getMultiPartitionModelsByStatus(final String projectName,
            final List<String> status) {
        return getModels(null, projectName, false, null, status, LAST_MODIFY, true, null, null, null).stream()
                .filter(NDataModel::isMultiPartitionModel).collect(Collectors.toList());
    }

    public DataResult<List<NDataModel>> getModels(String modelId, String modelAlias, boolean exactMatch, String project,
            String owner, List<String> status, String table, Integer offset, Integer limit, String sortBy,
            boolean reverse, String modelAliasOrOwner, List<ModelAttributeEnum> modelAttributes, Long lastModifyFrom,
            Long lastModifyTo, boolean onlyNormalDim) {
        List<NDataModel> models = new ArrayList<>();
        DataResult<List<NDataModel>> filterModels;
        if (StringUtils.isEmpty(table)) {
            val modelQueryParams = new ModelQueryParams(modelId, modelAlias, exactMatch, project, owner, status, offset,
                    limit, sortBy, reverse, modelAliasOrOwner, modelAttributes, lastModifyFrom, lastModifyTo,
                    onlyNormalDim);
            val tripleList = modelQuerySupporter.getModels(modelQueryParams);
            val pair = getModelsOfCurrentPage(modelQueryParams, tripleList);
            models.addAll(pair.getFirst());
            // add second storage infos
            ModelUtils.addSecondStorageInfo(project, models);
            filterModels = new DataResult<>(models, pair.getSecond(), offset, limit);
            filterModels.setValue(addOldParams(project, filterModels.getValue()));
            filterModels.setValue(updateReponseAcl(filterModels.getValue(), project));
            return filterModels;
        }
        models.addAll(getRelateModels(project, table, modelAlias));
        Set<NDataModel> filteredModels = ModelUtils.getFilteredModels(project, modelAttributes, models);

        if (CollectionUtils.isNotEmpty(modelAttributes)) {
            models = models.stream().filter(filteredModels::contains).collect(Collectors.toList());
        }
        if (StringUtils.isNotEmpty(modelId)) {
            models.removeIf(model -> !model.getUuid().equals(modelId));
        }
        filterModels = DataResult.get(models, offset, limit);

        filterModels.setValue(addOldParams(project, filterModels.getValue()));
        filterModels.setValue(updateReponseAcl(filterModels.getValue(), project));
        return filterModels;
    }

    public Pair<List<NDataModelResponse>, Integer> getModelsOfCurrentPage(ModelQueryParams queryElem,
            List<ModelTriple> modelTripleList) {
        val projectName = queryElem.getProjectName();
        val offset = queryElem.getOffset();
        val limit = queryElem.getLimit();
        val status = queryElem.getStatus();
        val dfManager = getDataflowManager(projectName);
        List<NDataModelResponse> filterModels = new ArrayList<>();
        final AtomicInteger totalSize = new AtomicInteger();
        modelTripleList.stream().map(t -> {
            if ((status == null || status.isEmpty()) && !PagingUtil.isInCurrentPage(totalSize.get(), offset, limit)) {
                totalSize.getAndIncrement();
                return null;
            }
            NDataModel dataModel = t.getDataModel();
            try {
                return convertToDataModelResponse(dataModel, projectName, dfManager, status,
                        queryElem.isOnlyNormalDim());
            } catch (Exception e) {
                String errorMsg = String.format(Locale.ROOT,
                        "convert NDataModelResponse failed, mark to broken. %s, %s", dataModel.getAlias(),
                        dataModel.getUuid());
                logger.error(errorMsg, e);
                return convertToDataModelResponseBroken(dataModel);
            }
        }).filter(Objects::nonNull).forEach(nDataModelResponse -> {
            if (PagingUtil.isInCurrentPage(totalSize.get(), offset, limit)) {
                filterModels.add(nDataModelResponse);
            }
            totalSize.getAndIncrement();
        });

        return new Pair<>(filterModels, totalSize.get());
    }

    public NDataModelResponse convertToDataModelResponseBroken(NDataModel modelDesc) {
        NDataModelResponse response = new NDataModelResponse(modelDesc);
        response.setStatus(ModelStatusToDisplayEnum.BROKEN);
        return response;
    }

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, List<String> status, String sortBy, boolean reverse, String modelAliasOrOwner,
            Long lastModifyFrom, Long lastModifyTo) {
        return getModels(modelAlias, projectName, exactMatch, owner, status, sortBy, reverse, modelAliasOrOwner,
                lastModifyFrom, lastModifyTo, true);
    }

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, List<String> status, String sortBy, boolean reverse, String modelAliasOrOwner,
            Long lastModifyFrom, Long lastModifyTo, boolean onlyNormalDim) {
        aclEvaluate.checkProjectReadPermission(projectName);
        List<Pair<NDataflow, NDataModel>> pairs = getFirstMatchModels(modelAlias, projectName, exactMatch, owner,
                modelAliasOrOwner, lastModifyFrom, lastModifyTo);
        val dfManager = getDataflowManager(projectName);
        List<NDataModelResponse> filterModels = new ArrayList<>();
        pairs.forEach(p -> {
            val modelDesc = p.getValue();
            val dataModelResponse = convertToDataModelResponse(modelDesc, projectName, dfManager, status,
                    onlyNormalDim);
            if (dataModelResponse != null) {
                filterModels.add(dataModelResponse);
            }
        });

        if ("expansionrate".equalsIgnoreCase(sortBy)) {
            return sortExpansionRate(reverse, filterModels);
        } else if (getProjectManager().getProject(projectName).isSemiAutoMode()) {
            Comparator<NDataModelResponse> comparator = BasicService
                    .propertyComparator(StringUtils.isEmpty(sortBy) ? ModelService.REC_COUNT : sortBy, !reverse);
            filterModels.sort(comparator);
            return filterModels;
        } else {
            Comparator<NDataModelResponse> comparator = BasicService
                    .propertyComparator(StringUtils.isEmpty(sortBy) ? ModelService.LAST_MODIFY : sortBy, !reverse);
            filterModels.sort(comparator);
            return filterModels;
        }
    }

    public NDataModelResponse convertToDataModelResponse(NDataModel modelDesc, String projectName,
            NDataflowManager dfManager, List<String> status, boolean onlyNormalDim) {
        long inconsistentSegmentCount = dfManager.getDataflow(modelDesc.getId()).getSegments(SegmentStatusEnum.WARNING)
                .size();
        ModelStatusToDisplayEnum modelResponseStatus = convertModelStatusToDisplay(modelDesc, projectName,
                inconsistentSegmentCount);
        if (modelDesc.isFusionModel()) {
            modelResponseStatus = convertFusionModelStatusToDisplay(modelDesc, modelResponseStatus, projectName,
                    dfManager);
        }
        boolean isModelStatusMatch = isListContains(status, modelResponseStatus);
        if (isModelStatusMatch) {
            boolean isScd2ForbiddenOnline = checkSCD2ForbiddenOnline(modelDesc, projectName);
            NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
            nDataModelResponse.computedInfo(inconsistentSegmentCount, modelResponseStatus, isScd2ForbiddenOnline,
                    modelDesc, onlyNormalDim);
            return nDataModelResponse;
        } else {
            return null;
        }
    }

    private ModelStatusToDisplayEnum convertFusionModelStatusToDisplay(NDataModel modelDesc,
            ModelStatusToDisplayEnum modelResponseStatus, String projectName, NDataflowManager dfManager) {
        FusionModel fusionModel = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName)
                .getFusionModel(modelDesc.getFusionId());
        val batchModel = fusionModel.getBatchModel();
        long inconsistentBatchSegmentCount = dfManager.getDataflow(batchModel.getId())
                .getSegments(SegmentStatusEnum.WARNING).size();
        ModelStatusToDisplayEnum batchModelResponseStatus = convertModelStatusToDisplay(batchModel, projectName,
                inconsistentBatchSegmentCount);
        if (!batchModel.isBroken() && !modelDesc.isBroken()) {
            switch (modelResponseStatus) {
            case ONLINE:
                return (batchModelResponseStatus == ModelStatusToDisplayEnum.WARNING ? ModelStatusToDisplayEnum.WARNING
                        : modelResponseStatus);
            case OFFLINE:
                return batchModelResponseStatus;
            default:
                return modelResponseStatus;
            }
        } else {
            return modelResponseStatus;
        }
    }

    private List<Pair<NDataflow, NDataModel>> getFirstMatchModels(final String modelAlias, final String projectName,
            boolean exactMatch, String owner, String modelAliasOrOwner, Long lastModifyFrom, Long lastModifyTo) {
        return getDataflowManager(projectName).listAllDataflows(true).stream()
                .map(df -> Pair.newPair(df,
                        df.checkBrokenWithRelatedInfo()
                                ? modelQuerySupporter.getBrokenModel(projectName, df.getId())
                                : df.getModel()))
                .filter(p -> !(Objects.nonNull(lastModifyFrom) && lastModifyFrom > p.getValue().getLastModified())
                        && !(Objects.nonNull(lastModifyTo) && lastModifyTo <= p.getValue().getLastModified())
                        && (ModelUtils.isArgMatch(modelAliasOrOwner, exactMatch, p.getValue().getAlias())
                                || ModelUtils.isArgMatch(modelAliasOrOwner, exactMatch, p.getValue().getOwner()))
                        && ModelUtils.isArgMatch(modelAlias, exactMatch, p.getValue().getAlias())
                        && ModelUtils.isArgMatch(owner, exactMatch, p.getValue().getOwner())
                        && !p.getValue().fusionModelBatchPart())
                .collect(Collectors.toList());
    }

    public ModelStatusToDisplayEnum convertModelStatusToDisplay(NDataModel modelDesc, final String projectName,
            long inconsistentSegmentCount) {
        RealizationStatusEnum modelStatus = modelDesc.isBroken() ? RealizationStatusEnum.BROKEN
                : getModelStatus(modelDesc.getUuid(), projectName);
        ModelStatusToDisplayEnum modelResponseStatus = ModelStatusToDisplayEnum.convert(modelStatus);
        val segmentHoles = getDataflowManager(projectName).calculateSegHoles(modelDesc.getUuid());
        if (modelResponseStatus == ModelStatusToDisplayEnum.BROKEN
                || modelResponseStatus == ModelStatusToDisplayEnum.OFFLINE) {
            return modelResponseStatus;
        }
        if (getEmptyIndexesCount(projectName, modelDesc.getId()) > 0 || CollectionUtils.isNotEmpty(segmentHoles)
                || inconsistentSegmentCount > 0) {
            modelResponseStatus = ModelStatusToDisplayEnum.WARNING;
        }
        return modelResponseStatus;
    }

    private long getEmptyIndexesCount(String project, String id) {
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(id);
        return indexPlan.getAllLayoutsReadOnly().size() - indexPlanManager.getAvailableIndexesCount(project, id);
    }

    private List<NDataModelResponse> sortExpansionRate(boolean reverse, List<NDataModelResponse> filterModels) {
        List<NDataModelResponse> sorted;
        if (!reverse) {
            sorted = filterModels.stream().sorted(Comparator.comparing(a -> new BigDecimal(a.getExpansionrate())))
                    .collect(Collectors.toList());
        } else {
            sorted = filterModels.stream().sorted(
                    (a, b) -> new BigDecimal(b.getExpansionrate()).compareTo(new BigDecimal(a.getExpansionrate())))
                    .collect(Collectors.toList());
        }
        List<NDataModelResponse> unknowModels = sorted.stream()
                .filter(modle -> "-1".equalsIgnoreCase(modle.getExpansionrate())).collect(Collectors.toList());

        List<NDataModelResponse> nDataModelResponses = sorted.stream()
                .filter(modle -> !"-1".equalsIgnoreCase(modle.getExpansionrate())).collect(Collectors.toList());
        nDataModelResponses.addAll(unknowModels);
        return nDataModelResponses;
    }

    private NDataModelResponse enrichModelResponse(NDataModel modelDesc, String projectName) {
        NDataModelResponse nDataModelResponse = modelDesc.isFusionModel() ? new FusionModelResponse(modelDesc)
                : new NDataModelResponse(modelDesc);

        if (modelDesc.isBroken()) {
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
            if (tableManager.getTableDesc(modelDesc.getRootFactTableName()) == null) {
                nDataModelResponse.setRootFactTableName(nDataModelResponse.getRootFactTableName() + " deleted");
                nDataModelResponse.setRootFactTableDeleted(true);
            }
            nDataModelResponse.setBroken(modelDesc.isBroken());
            return nDataModelResponse;
        }
        nDataModelResponse.setAllTableRefs(modelDesc.getAllTables());
        nDataModelResponse.setBroken(modelDesc.isBroken());
        if (ManagementType.MODEL_BASED == modelDesc.getManagementType()) {
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

    private boolean checkSCD2ForbiddenOnline(NDataModel modelDesc, String projectName) {
        boolean isSCD2 = SCD2CondChecker.INSTANCE.isScd2Model(modelDesc);
        return !getProjectManager().getProject(projectName).getConfig().isQueryNonEquiJoinModelEnabled() && isSCD2;
    }

    protected RealizationStatusEnum getModelStatus(String modelId, String projectName) {
        val indexPlan = getIndexPlan(modelId, projectName);
        if (indexPlan != null) {
            return getDataflowManager(projectName).getDataflow(indexPlan.getUuid()).getStatus();
        } else {
            return null;
        }
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String status, String sortBy, boolean reverse) {
        return getSegmentsResponse(modelId, project, start, end, status, null, null, false, sortBy, reverse);
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String status, List<AbstractExecutable> executables, String sortBy, boolean reverse) {
        return getSegmentsResponse(modelId, project, start, end, status, null, null, executables, false, sortBy,
                reverse);
    }

    private List<AbstractExecutable> getAllRunningExecutable(String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NExecutableManager execManager = NExecutableManager.getInstance(kylinConfig, project);
        return execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, JobTypeEnum.INDEX_BUILD,
                JobTypeEnum.SUB_PARTITION_BUILD);
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String status, Collection<Long> withAllIndexes, Collection<Long> withoutAnyIndexes, boolean allToComplement,
            String sortBy, boolean reverse) {
        val executables = getAllRunningExecutable(project);
        return getSegmentsResponse(modelId, project, start, end, status, withAllIndexes, withoutAnyIndexes, executables,
                allToComplement, sortBy, reverse);
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String status, Collection<Long> withAllIndexes, Collection<Long> withoutAnyIndexes,
            List<AbstractExecutable> executables, boolean allToComplement, String sortBy, boolean reverse) {
        aclEvaluate.checkProjectReadPermission(project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        List<NDataSegmentResponse> segmentResponseList = getSegmentsResponseCore(modelId, project, start, end, status,
                withAllIndexes, withoutAnyIndexes, executables, allToComplement, dataflow);
        addSecondStorageResponse(modelId, project, segmentResponseList, dataflow);
        segmentsResponseListSort(sortBy, reverse, segmentResponseList);
        return segmentResponseList;
    }

    public void segmentsResponseListSort(String sortBy, boolean reverse,
            List<NDataSegmentResponse> segmentResponseList) {
        Comparator<NDataSegmentResponse> comparator = BasicService
                .propertyComparator(StringUtils.isEmpty(sortBy) ? "create_time_utc" : sortBy, reverse);
        segmentResponseList.sort(comparator);
    }

    public List<NDataSegmentResponse> getSegmentsResponseCore(String modelId, String project, String start, String end,
            String status, Collection<Long> withAllIndexes, Collection<Long> withoutAnyIndexes,
            List<AbstractExecutable> executables, boolean allToComplement, NDataflow dataflow) {
        List<NDataSegmentResponse> segmentResponseList;
        val segs = getSegmentsByRange(modelId, project, start, end);

        // filtering on index
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getIndexPlan(modelId);
        Set<Long> allIndexes = indexPlan.getAllLayoutIds(true);
        if (CollectionUtils.isNotEmpty(withAllIndexes)) {
            for (Long idx : withAllIndexes) {
                if (!allIndexes.contains(idx)) {
                    throw new KylinException(ServerErrorCode.INVALID_SEGMENT_PARAMETER, "Invalid index id " + idx);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(withoutAnyIndexes)) {
            for (Long idx : withoutAnyIndexes) {
                if (!allIndexes.contains(idx)) {
                    throw new KylinException(ServerErrorCode.INVALID_SEGMENT_PARAMETER, "Invalid index id " + idx);
                }
            }
        }
        List<NDataSegment> indexFiltered = new LinkedList<>();
        segs.forEach(segment -> filterSeg(withAllIndexes, withoutAnyIndexes, allToComplement,
                indexPlan.getAllLayoutIds(false), indexFiltered, segment));
        segmentResponseList = indexFiltered.stream()
                .filter(segment -> !StringUtils.isNotEmpty(status) || status
                        .equalsIgnoreCase(SegmentUtil.getSegmentStatusToDisplay(segs, segment, executables).toString()))
                .map(segment -> new NDataSegmentResponse(dataflow, segment, executables)).collect(Collectors.toList());
        return segmentResponseList;
    }

    public void addSecondStorageResponse(String modelId, String project, List<NDataSegmentResponse> segmentResponseList,
            NDataflow dataflow) {

        if (!SecondStorageUtil.isModelEnable(project, modelId))
            return;

        val tableFlowManager = SecondStorage.tableFlowManager(getConfig(), project);
        val tableFlow = tableFlowManager.get(dataflow.getId()).orElse(null);
        if (tableFlow != null) {
            Map<String, List<TablePartition>> tablePartitions = tableFlow.getTableDataList().stream()
                    .flatMap(tableData -> tableData.getPartitions().stream())
                    .collect(Collectors.toMap(TablePartition::getSegmentId, partition -> Lists.newArrayList(partition),
                            (List<TablePartition> a, List<TablePartition> b) -> {
                                a.addAll(b);
                                return a;
                            }));
            segmentResponseList.forEach(segment -> {
                if (tablePartitions.containsKey(segment.getId())) {
                    val nodes = new ArrayList<String>();
                    Long sizes = 0L;
                    var partitions = tablePartitions.get(segment.getId());
                    for (TablePartition partition : partitions) {
                        nodes.addAll(partition.getShardNodes());
                        var size = partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L);
                        sizes += size;
                    }
                    try {
                        segment.setSecondStorageSize(sizes / SecondStorageConfig.getInstanceFromEnv().getReplicaNum());
                    } catch (Exception e) {
                        log.error("setSecondStorageSize failed", e);
                    }
                    Map<String, List<SecondStorageNode>> pairs = SecondStorageUtil.convertNodesToPairs(nodes);
                    segment.setSecondStorageNodes(pairs);
                } else {
                    segment.setSecondStorageNodes(Collections.emptyMap());
                    segment.setSecondStorageSize(0L);
                }
            });
        }
    }

    private void filterSeg(Collection<Long> withAllIndexes, Collection<Long> withoutAnyIndexes, boolean allToComplement,
            Set<Long> allIndexWithoutTobeDel, List<NDataSegment> indexFiltered, NDataSegment segment) {
        if (allToComplement) {
            // find seg that does not have all indexes(don't include tobeDeleted)
            val segLayoutIds = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId)
                    .collect(Collectors.toSet());
            if (!Sets.difference(allIndexWithoutTobeDel, segLayoutIds).isEmpty()) {
                indexFiltered.add(segment);
            }
        } else if (withAllIndexes != null && !withAllIndexes.isEmpty()) {
            // find seg that has all required indexes
            if (segment.getLayoutIds().containsAll(withAllIndexes)) {
                indexFiltered.add(segment);
            }
        } else if (withoutAnyIndexes != null && !withoutAnyIndexes.isEmpty()) {
            // find seg that miss any of the indexes
            if (!segment.getLayoutIds().containsAll(withoutAnyIndexes)) {
                indexFiltered.add(segment);
            }
        } else {
            indexFiltered.add(segment);
        }
    }

    public Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        val df = dataflowManager.getDataflow(modelId);
        if (df == null) {
            return Segments.empty();
        }
        val model = df.getModel();
        if (model == null || model.isBroken()) {
            return Segments.empty();
        }
        SegmentRange filterRange = getSegmentRangeByModel(project, modelId, start, end);
        return df.getSegmentsByRange(filterRange);
    }

    public IndicesResponse getAggIndices(String project, String modelId, Long indexId, String contentSeg,
            boolean isCaseSensitive, Integer pageOffset, Integer pageSize, String sortBy, Boolean reverse) {
        aclEvaluate.checkProjectReadPermission(project);

        logger.debug("find project={}, model={}, index={}, content={}, isCaseSensitive={}, sortBy={}, reverse={}",
                project, modelId, indexId, contentSeg, isCaseSensitive, sortBy, reverse);

        IndicesResponse result;
        if (Objects.nonNull(indexId)) {
            result = getIndicesById(project, modelId, indexId);
        } else {
            IndexPlan indexPlan = getIndexPlan(modelId, project);
            result = new IndicesResponse(indexPlan);
            indexPlan.getAllIndexes().stream().filter(e -> e.getId() < IndexEntity.TABLE_INDEX_START_ID)
                    .forEach(result::addIndexEntity);
        }
        List<IndicesResponse.Index> indices = result.getIndices();
        if (Objects.nonNull(contentSeg)) {
            indices = filterFuzzyMatchedIndices(indices, contentSeg, isCaseSensitive);
        }
        result.setSize(indices.size());
        result.setIndices(sortIndicesThenCutPage(indices, sortBy, reverse, pageOffset, pageSize));
        return result;
    }

    private List<IndicesResponse.Index> filterFuzzyMatchedIndices(List<IndicesResponse.Index> indices,
            String contentSeg, boolean isCaseSensitive) {
        if (StringUtils.isBlank(contentSeg)) {
            return indices;
        }
        return indices.stream().filter(index -> fuzzyMatched(contentSeg, isCaseSensitive, String.valueOf(index.getId())) // weird rule
                || index.getDimensions().stream().anyMatch(d -> fuzzyMatched(contentSeg, isCaseSensitive, d))
                || index.getMeasures().stream().anyMatch(m -> fuzzyMatched(contentSeg, isCaseSensitive, m)))
                .collect(Collectors.toList());
    }

    private List<IndicesResponse.Index> sortIndicesThenCutPage(List<IndicesResponse.Index> indices, String sortBy,
            boolean reverse, int pageOffset, int pageSize) {
        Comparator<IndicesResponse.Index> comparator = BasicService
                .propertyComparator(StringUtils.isEmpty(sortBy) ? IndicesResponse.LAST_MODIFY_TIME : sortBy, !reverse);
        indices.sort(comparator);
        return PagingUtil.cutPage(indices, pageOffset, pageSize);
    }

    private boolean fuzzyMatched(String contentSeg, boolean isCaseSensitive, String content) {
        if (isCaseSensitive) {
            return StringUtils.contains(content, contentSeg);
        }
        return StringUtils.containsIgnoreCase(content, contentSeg);
    }

    @VisibleForTesting
    public IndicesResponse getIndicesById(String project, String modelId, Long indexId) {
        aclEvaluate.checkProjectReadPermission(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        IndicesResponse result = new IndicesResponse(indexPlan);
        result.addIndexEntity(indexPlan.getIndexEntity(indexId));
        return result;
    }

    public IndicesResponse getTableIndices(String modelId, String project) {
        aclEvaluate.checkProjectReadPermission(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        IndicesResponse result = new IndicesResponse(indexPlan);
        indexPlan.getAllIndexes().stream().filter(e -> IndexEntity.isTableIndex(e.getId()))
                .forEach(result::addIndexEntity);
        return result;
    }

    @VisibleForTesting
    IndicesResponse getIndices(String modelId, String project) {
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        IndicesResponse result = new IndicesResponse(indexPlan);
        indexPlan.getAllIndexes().forEach(result::addIndexEntity);
        return result;
    }

    public String getModelJson(String modelId, String project) throws JsonProcessingException {
        aclEvaluate.checkProjectReadPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        return JsonUtil.writeValueAsIndentString(modelDesc);
    }

    public String getModelSql(String modelId, String project) {
        aclEvaluate.checkProjectReadPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        return JoinedFlatTable.generateSelectDataStatement(modelDesc, false);
    }

    public List<RelatedModelResponse> getRelateModels(String project, String table, String modelId) {
        aclEvaluate.checkProjectReadPermission(project);
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        val dataflowManager = getDataflowManager(project);
        val models = dataflowManager.getTableOrientedModelsUsingRootTable(tableDesc);
        List<RelatedModelResponse> relatedModel = new ArrayList<>();
        val errorExecutables = getExecutableManager(project).getExecutablesByStatus(ExecutableState.ERROR);
        for (var dataModelDesc : models) {
            Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
            val model = dataModelDesc.getUuid();
            if (StringUtils.isEmpty(modelId)
                    || dataModelDesc.getAlias().toLowerCase(Locale.ROOT).contains(modelId.toLowerCase(Locale.ROOT))) {
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

    @VisibleForTesting
    public IndexPlan getIndexPlan(String modelId, String project) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        return indexPlanManager.getIndexPlan(modelId);
    }

    private void checkAliasExist(String modelId, String newAlias, String project) {
        if (!checkModelAliasUniqueness(modelId, newAlias, project)) {
            throw new KylinException(ServerErrorCode.INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_ALIAS_DUPLICATED(), newAlias));
        }
    }

    public boolean checkModelAliasUniqueness(String modelId, String newAlias, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        List<NDataModel> models = getDataflowManager(project).listUnderliningDataModels();
        for (NDataModel model : models) {
            if ((StringUtils.isNotEmpty(modelId) || !model.getUuid().equals(modelId))
                    && model.getAlias().equalsIgnoreCase(newAlias)) {
                return false;
            }
        }
        return true;
    }

    @Transaction(project = 1)
    public void dropModel(String modelId, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelPermission(project, modelId);
        val model = getModelById(modelId, project);
        val modelName = model.getAlias();
        dropModel(modelId, project, false);
        EventBusFactory.getInstance().postSync(new ModelDropEvent(project, modelId, modelName));
    }

    void dropModel(String modelId, String project, boolean ignoreType) {
        val projectInstance = getProjectManager().getProject(project);
        if (!ignoreType) {
            Preconditions.checkState(MaintainModelType.MANUAL_MAINTAIN == projectInstance.getMaintainModelType());
        }

        NDataModel dataModelDesc = getModelById(modelId, project);
        boolean isStreamingModel = false;
        if (dataModelDesc.isStreaming()) {
            isStreamingModel = true;
        } else if (dataModelDesc.getModelType() == NDataModel.ModelType.UNKNOWN) {
            val streamingJobMgr = StreamingJobManager.getInstance(getConfig(), project);
            isStreamingModel = streamingJobMgr.getStreamingJobByUuid(modelId + "_build") != null
                    || streamingJobMgr.getStreamingJobByUuid(modelId + "_merge") != null;
        }
        if (isStreamingModel) {
            EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, modelId));
            EventBusFactory.getInstance().postSync(new StreamingJobDropEvent(project, modelId));
        }

        cleanModelWithSecondStorage(modelId, project);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        dataflowManager.dropDataflow(modelId);
        indexPlanManager.dropIndexPlan(modelId);
        dataModelManager.dropModel(dataModelDesc);
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
            cleanModelWithSecondStorage(modelId, project);
        }
        offlineModelIfNecessary(dataflowManager, modelId);
    }

    @Transaction(project = 1)
    public void purgeModelManually(String modelId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModelDesc = getModelById(modelId, project);
        if (ManagementType.TABLE_ORIENTED == dataModelDesc.getManagementType()) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_CAN_NOT_PURGE(), dataModelDesc.getAlias()));
        }
        purgeModel(modelId, project);
    }

    private void cleanModelWithSecondStorage(String modelId, String project) {
        if (SecondStorageUtil.isModelEnable(project, modelId)) {
            val jobHandler = new SecondStorageModelCleanJobHandler();
            final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, modelId, getUsername());
            getJobManager(project).addJob(param, jobHandler);
            SecondStorageUtil.disableModel(project, modelId);
        }
    }

    public void cloneModel(String modelId, String newModelName, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        checkAliasExist("", newModelName, project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager dataModelManager = getDataModelManager(project);
            NDataModel dataModelDesc = getModelById(modelId, project);
            //copyForWrite nDataModel do init,but can not set new modelname
            NDataModel nDataModel = semanticUpdater.deepCopyModel(dataModelDesc);
            nDataModel.setUuid(RandomUtil.randomUUIDStr());
            nDataModel.setAlias(newModelName);
            nDataModel.setLastModified(System.currentTimeMillis());
            nDataModel.setRecommendationsCount(0);
            nDataModel.setMvcc(-1);
            nDataModel.setProject(project);
            changeModelOwner(nDataModel);
            val newModel = dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
            cloneIndexPlan(modelId, project, nDataModel.getOwner(), newModel.getUuid(), RealizationStatusEnum.OFFLINE);
            return null;
        }, project);
    }

    private void cloneIndexPlan(String modelId, String project, String owner, String newModelId,
            RealizationStatusEnum realizationStatusEnum) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan copy = indexPlanManager.copy(indexPlan);
        copy.setUuid(newModelId);
        copy.setLastModified(System.currentTimeMillis());
        copy.setMvcc(-1);
        Set<Long> toBeDeletedLayouts = copy.getToBeDeletedIndexes().stream()
                .flatMap(indexEntity -> indexEntity.getLayouts().stream()).map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        copy.removeLayouts(toBeDeletedLayouts, true, true);
        indexPlanManager.createIndexPlan(copy);

        dataflowManager.createDataflow(copy, owner, realizationStatusEnum);
    }

    @Transaction(project = 0)
    public void renameDataModel(String project, String modelId, String newAlias) {
        aclEvaluate.checkProjectWritePermission(project);
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
        aclEvaluate.checkProjectWritePermission(project);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataModelManager dataModelManager = getDataModelManager(project);

        NDataModel nDataModel = getModelById(modelId, project);
        if (ManagementType.MODEL_BASED == nDataModel.getManagementType()) {
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
        }
    }

    public Set<String> listAllModelIdsInProject(String project) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        return dataModelManager.listAllModelIds();
    }

    @Transaction(project = 0)
    public void offlineAllModelsInProject(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> ids = listAllModelIdsInProject(project);
        for (String id : ids) {
            updateDataModelStatus(id, project, "OFFLINE");
        }
    }

    @Transaction(project = 0)
    public void offlineMultiPartitionModelsInProject(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> multiPartitionModels = getMultiPartitionModelsByStatus(project, getModelNonOffOnlineStatus())
                .stream().map(NDataModel::getId).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(multiPartitionModels)) {
            return;
        }
        offlineModelsInProjectById(project, new HashSet<>(multiPartitionModels));
    }

    @Transaction(project = 0)
    public void offlineModelsInProjectById(String project, Set<String> modelIds) {
        aclEvaluate.checkProjectWritePermission(project);
        for (String id : modelIds) {
            if (modelIds.contains(id)) {
                updateDataModelStatus(id, project, ModelStatusToDisplayEnum.OFFLINE.name());
            }
        }
    }

    @Transaction(project = 0)
    public void updateSCD2ModelStatusInProjectById(String project, ModelStatusToDisplayEnum status) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> scd2Models = getSCD2Models(project);
        if (CollectionUtils.isEmpty(scd2Models)) {
            return;
        }
        updateModelStatusInProjectById(project, new HashSet<>(scd2Models), status);
    }

    @Transaction(project = 0)
    public void updateModelStatusInProjectById(String project, Set<String> modelIds, ModelStatusToDisplayEnum status) {
        aclEvaluate.checkProjectWritePermission(project);
        for (String id : modelIds) {
            try {
                updateDataModelStatus(id, project, status.name());
            } catch (Exception e) {
                logger.warn("Failed update model {} status to {}, {}", id, status.name(), e.getMessage());
            }
        }
    }

    @Transaction(project = 0)
    public void onlineAllModelsInProject(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> ids = listAllModelIdsInProject(project);
        for (String id : ids) {
            updateDataModelStatus(id, project, "ONLINE");
        }
    }

    @Transaction(project = 1)
    public void updateDataModelStatus(String modelId, String project, String status) {
        NDataModel nDataModel = getModelById(modelId, project);
        if (nDataModel.isFusionModel()) {
            NDataflowManager dataflowManager = getDataflowManager(project);
            NDataflow dataflow = dataflowManager.getDataflow(nDataModel.getUuid());
            if (CollectionUtils.isNotEmpty(dataflow.getSegments())) {
                doUpdateDataModelStatus(nDataModel, project, status);
            }
            val fusionId = nDataModel.getFusionId();
            val fusionModelMgr = FusionModelManager.getInstance(getConfig(), project);
            val batchModel = fusionModelMgr.getFusionModel(fusionId).getBatchModel();
            if (batchModel != null) {
                dataflow = dataflowManager.getDataflow(batchModel.getUuid());
                if (CollectionUtils.isNotEmpty(dataflow.getSegments())) {
                    doUpdateDataModelStatus(batchModel, project, status);
                }
            }
        } else {
            doUpdateDataModelStatus(nDataModel, project, status);
        }
    }

    private void doUpdateDataModelStatus(NDataModel nDataModel, String project, String status) {
        String modelId = nDataModel.getUuid();
        aclEvaluate.checkProjectWritePermission(project);
        IndexPlan indexPlan = getIndexPlan(nDataModel.getUuid(), project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        checkDataflowStatus(dataflow, modelId);
        boolean needChangeStatus = (status.equals(RealizationStatusEnum.OFFLINE.name())
                && RealizationStatusEnum.ONLINE == dataflow.getStatus())
                || (status.equals(RealizationStatusEnum.ONLINE.name())
                        && RealizationStatusEnum.OFFLINE == dataflow.getStatus());
        if (needChangeStatus) {
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            if (status.equals(RealizationStatusEnum.OFFLINE.name())) {
                nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            } else if (status.equals(RealizationStatusEnum.ONLINE.name())) {
                if (SCD2CondChecker.INSTANCE.isScd2Model(dataflow.getModel())
                        && !projectService.getProjectConfig(project).isScd2Enabled()) {
                    throw new KylinException(ServerErrorCode.MODEL_ONLINE_ABANDON,
                            MsgPicker.getMsg().getSCD2_MODEL_ONLINE_WITH_SCD2_CONFIG_OFF());
                }
                if (dataflow.getSegments().isEmpty() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
                    throw new KylinException(ServerErrorCode.MODEL_ONLINE_ABANDON,
                            MsgPicker.getMsg().getMODEL_ONLINE_WITH_EMPTY_SEG());
                }
                if (dataflowManager.isOfflineModel(dataflow)) {
                    throw new KylinException(ServerErrorCode.MODEL_ONLINE_ABANDON,
                            MsgPicker.getMsg().getMODEL_ONLINE_FORBIDDEN());
                }
                nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            }
            dataflowManager.updateDataflow(nDataflowUpdate);
        }
    }

    private void checkDataflowStatus(NDataflow dataflow, String modelId) {
        if (RealizationStatusEnum.BROKEN == dataflow.getStatus()) {
            throw new KylinException(ServerErrorCode.DUPLICATE_JOIN_CONDITION,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBROKEN_MODEL_CANNOT_ONOFFLINE(), modelId));
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelId, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelId).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    public boolean isModelsUsingTable(String table, String project) {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        return CollectionUtils.isNotEmpty(getDataflowManager(project).getModelsUsingTable(tableDesc));
    }

    public List<NDataModel> getModelsUsingTable(String table, String project) {
        return getDataflowManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table));
    }

    public RefreshAffectedSegmentsResponse getRefreshAffectedSegmentsResponse(String project, String table,
            String start, String end) {
        aclEvaluate.checkProjectReadPermission(project);
        val dfManager = getDataflowManager(project);
        long byteSize = 0L;
        List<RelatedModelResponse> models = getRelateModels(project, table, "").stream().filter(
                relatedModelResponse -> ManagementType.TABLE_ORIENTED == relatedModelResponse.getManagementType())
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(models)) {
            logger.info("No segment to refresh, No related model.");
            return new RefreshAffectedSegmentsResponse(0, start, end);
        }

        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        SegmentRange toBeRefreshSegmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(start, end);

        val loadingRangeMgr = getDataLoadingRangeManager(project);
        val loadingRange = loadingRangeMgr.getDataLoadingRange(table);

        if (loadingRange != null) {
            // check if toBeRefreshSegmentRange is within covered ready segment range
            checkRefreshRangeWithinCoveredRange(loadingRange, toBeRefreshSegmentRange);
        }

        Segments<NDataSegment> affectedSegments = new Segments<>();

        for (NDataModel model : models) {
            val dataflow = dfManager.getDataflow(model.getId());
            Segments<NDataSegment> segments = getSegmentsByRange(model.getUuid(), project, start, end);
            if (RealizationStatusEnum.LAG_BEHIND != dataflow.getStatus()) {
                if (CollectionUtils.isEmpty(segments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING))) {
                    if (loadingRange == null) {
                        //full build
                        logger.info("No segment to refresh, full build.");
                        return new RefreshAffectedSegmentsResponse(0, start, end);
                    } else {
                        throw new KylinException(ServerErrorCode.EMPTY_SEGMENT_RANGE,
                                "No segments to refresh, please select new range and try again!");
                    }
                }

                if (CollectionUtils.isNotEmpty(segments.getBuildingSegments())) {
                    throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                            MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
                }
            } else {
                checkSegRefreshingInLagBehindModel(segments);
            }
            affectedSegments.addAll(segments);
        }
        Preconditions.checkState(CollectionUtils.isNotEmpty(affectedSegments));
        Collections.sort(affectedSegments);
        String affectedStart = affectedSegments.getFirstSegment().getSegRange().getStart().toString();
        String affectedEnd = affectedSegments.getLastSegment().getSegRange().getEnd().toString();
        for (NDataSegment segment : affectedSegments) {
            byteSize += segment.getStorageBytesSize();
        }
        return new RefreshAffectedSegmentsResponse(byteSize, affectedStart, affectedEnd);

    }

    private void checkSegRefreshingInLagBehindModel(Segments<NDataSegment> segments) {
        for (val seg : segments) {
            if (SegmentStatusEnumToDisplay.REFRESHING == SegmentUtil.getSegmentStatusToDisplay(segments, seg, null)) {
                throw new KylinException(ServerErrorCode.FAILED_REFRESH_SEGMENT,
                        MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
            }
        }
    }

    private void checkRefreshRangeWithinCoveredRange(NDataLoadingRange dataLoadingRange,
            SegmentRange toBeRefreshSegmentRange) {
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new KylinException(ServerErrorCode.INVALID_SEGMENT_RANGE, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_INVALID_RANGE(), toBeRefreshSegmentRange, coveredReadySegmentRange));
        }
    }

    @VisibleForTesting
    public void checkFlatTableSql(NDataModel dataModel) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        if (getModelConfig(dataModel).skipCheckFlatTable()) {
            return;
        }
        long rangePartitionTableCount = dataModel.getAllTableRefs().stream()
                .filter(p -> p.getTableDesc().isRangePartition()).count();
        if (rangePartitionTableCount > 0) {
            logger.info("Range partitioned tables do not support pushdown, so do not need to perform subsequent logic");
            return;
        }

        try {
            ProjectInstance projectInstance = getProjectManager().getProject(dataModel.getProject());
            if (projectInstance.getSourceType() == ISourceAware.ID_SPARK
                    && dataModel.getModelType() == NDataModel.ModelType.BATCH) {
                SparkSession ss = SparderEnv.getSparkSession();
                String flatTableSql = JoinedFlatTable.generateSelectDataStatement(dataModel, false);
                QueryParams queryParams = new QueryParams(dataModel.getProject(), flatTableSql, "default", false);
                queryParams.setKylinConfig(projectInstance.getConfig());
                queryParams.setAclInfo(
                        AclPermissionUtil.prepareQueryContextACLInfo(dataModel.getProject(), getCurrentUserGroups()));
                String pushdownSql = QueryUtil.massagePushDownSql(queryParams);
                ss.sql(pushdownSql);
            }
        } catch (Exception e) {
            Pattern pattern = Pattern.compile("cannot resolve '(.*?)' given input columns");
            Matcher matcher = pattern.matcher(e.getMessage().replace("`", ""));
            if (matcher.find()) {
                String column = matcher.group(1);
                String table = column.contains(".") ? column.split("\\.")[0] : dataModel.getRootFactTableName();
                String error = String.format(Locale.ROOT, MsgPicker.getMsg().getTABLENOTFOUND(), dataModel.getAlias(),
                        column, table);
                throw new KylinException(ServerErrorCode.TABLE_NOT_EXIST, error);
            } else {
                String errorMsg = String.format(Locale.ROOT, "model [%s], %s", dataModel.getAlias(),
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDEFAULT_REASON(),
                                null != e.getMessage() ? e.getMessage() : "null"));
                throw new KylinException(ServerErrorCode.FAILED_EXECUTE_MODEL_SQL, errorMsg);
            }
        }
    }

    private KylinConfig getModelConfig(NDataModel dataModel) {
        IndexPlan indexPlan = getIndexPlan(dataModel.getId(), dataModel.getProject());
        if (indexPlan == null || indexPlan.getConfig() == null) {
            return getProjectManager().getProject(dataModel.getProject()).getConfig();
        }
        return indexPlan.getConfig();
    }

    private void validatePartitionDateColumn(ModelRequest modelRequest) {
        if (Objects.nonNull(modelRequest.getPartitionDesc())) {
            if (StringUtils.isNotEmpty(modelRequest.getPartitionDesc().getPartitionDateColumn())) {
                Preconditions.checkArgument(
                        StringUtils.isNotEmpty(modelRequest.getPartitionDesc().getPartitionDateFormat()),
                        "Partition column format can not be empty!");
            } else {
                modelRequest.getPartitionDesc().setPartitionDateFormat("");
            }
            validateFusionModelDimension(modelRequest);
        }
    }

    public void validateFusionModelDimension(ModelRequest modelRequest) {
        val rootFactTableName = modelRequest.getRootFactTableName();
        // fusion model check timestamp
        val modelType = modelRequest.getModelType();
        if (!StringUtils.isEmpty(rootFactTableName)
                && (modelType != NDataModel.ModelType.BATCH && modelType != NDataModel.ModelType.STREAMING)) {
            val mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), modelRequest.getProject());
            val TableDesc = mgr.getTableDesc(rootFactTableName);
            if (TableDesc != null && TableDesc.getKafkaConfig() != null && TableDesc.getKafkaConfig().hasBatchTable()) {
                val fullColumnName = modelRequest.getPartitionDesc().getPartitionDateColumn();
                val columnName = fullColumnName.substring(fullColumnName.indexOf(".") + 1);
                val hasPartitionColumn = modelRequest.getSimplifiedDimensions().stream()
                        .filter(column -> column.getName().equalsIgnoreCase(columnName)).findAny().isPresent();
                if (!hasPartitionColumn && !modelRequest.getDimensionNameIdMap().containsKey(fullColumnName)) {
                    throw new KylinException(ServerErrorCode.TIMESTAMP_COLUMN_NOT_EXIST,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getTIMESTAMP_PARTITION_COLUMN_NOT_EXIST()));
                }
            }
        }
    }

    public void batchCreateModel(String project, List<ModelRequest> newModels, List<ModelRequest> reusedModels) {
        checkNewModels(project, newModels);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            saveNewModelsAndIndexes(project, newModels);
            updateReusedModelsAndIndexPlans(project, reusedModels);
            return null;
        }, project);
    }

    public void checkNewModels(String project, List<ModelRequest> newModels) {
        aclEvaluate.checkProjectWritePermission(project);
        checkDuplicateAliasInModelRequests(newModels);
        for (ModelRequest modelRequest : newModels) {
            validatePartitionDateColumn(modelRequest);
            modelRequest.setProject(project);
            doCheckBeforeModelSave(project, modelRequest);
        }
    }

    public void saveNewModelsAndIndexes(String project, List<ModelRequest> newModels) {
        if (CollectionUtils.isEmpty(newModels)) {
            return;
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        for (ModelRequest modelRequest : newModels) {
            if (modelRequest.getIndexPlan() == null) {
                continue;
            }
            // create model
            NDataModel model = JsonUtil.deepCopyQuietly(modelRequest, NDataModel.class);
            IndexPlan indexPlan = modelRequest.getIndexPlan();
            model.setProject(project);
            indexPlan.setProject(project);

            NDataModel saved;
            if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                saved = dataModelManager.updateDataModelDesc(model);
            } else {
                saved = dataModelManager.createDataModelDesc(model, model.getOwner());
            }

            // expand measures
            semanticUpdater.deleteExpandableMeasureInternalMeasures(saved);
            semanticUpdater.expandExpandableMeasure(saved);
            preProcessBeforeModelSave(saved, project);
            NDataModel expanded = getDataModelManager(project).updateDataModelDesc(saved);

            // create IndexPlan
            IndexPlan emptyIndex = new IndexPlan();
            emptyIndex.setUuid(expanded.getUuid());
            indexPlanManager.createIndexPlan(emptyIndex);
            indexPlanService.expandIndexPlanRequest(indexPlan, expanded);
            addBaseIndex(modelRequest, expanded, indexPlan);

            // create DataFlow
            val df = dataflowManager.createDataflow(emptyIndex, expanded.getOwner());
            if (modelRequest.isWithEmptySegment() && !modelRequest.isStreaming()) {
                dataflowManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                        SegmentStatusEnum.READY);
            }
            if (modelRequest.isWithModelOnline()) {
                dataflowManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);
            }

            createStreamingJob(project, expanded, modelRequest);
            updateIndexPlan(project, indexPlan);
            UnitOfWorkContext context = UnitOfWork.get();
            context.doAfterUnit(() -> EventBusFactory.getInstance()
                    .postSync(new ModelAddEvent(project, expanded.getId(), expanded.getAlias())));
        }
    }

    private void updateReusedModelsAndIndexPlans(String project, List<ModelRequest> modelRequestList) {
        if (CollectionUtils.isEmpty(modelRequestList)) {
            return;
        }

        if (modelRequestList.stream()
                .anyMatch(modelRequest -> !FusionIndexService.checkUpdateIndexEnabled(project, modelRequest.getId()))) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    MsgPicker.getMsg().getSTREAMING_INDEXES_CONVERT());
        }

        for (ModelRequest modelRequest : modelRequestList) {
            modelRequest.setProject(project);
            semanticUpdater.expandModelRequest(modelRequest);

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

            Map<String, NDataModel.NamedColumn> columnMap = Maps.newHashMap();
            modelRequest.getAllNamedColumns().forEach(column -> {
                Preconditions.checkArgument(!columnMap.containsKey(column.getAliasDotColumn()));
                columnMap.put(column.getAliasDotColumn(), column);
            });

            BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(
                    modelManager.getDataModelDesc(modelRequest.getId()), false);
            // update model
            List<LayoutRecDetailResponse> recItems = modelRequest.getRecItems();
            NDataModel updated = modelManager.updateDataModel(modelRequest.getId(), copyForWrite -> {
                copyForWrite.setJoinTables(modelRequest.getJoinTables());
                List<NDataModel.NamedColumn> allNamedColumns = copyForWrite.getAllNamedColumns();
                Map<Integer, NDataModel.NamedColumn> namedColumnMap = Maps.newHashMap();
                allNamedColumns.forEach(col -> namedColumnMap.put(col.getId(), col));
                Map<String, ComputedColumnDesc> newCCMap = Maps.newLinkedHashMap();
                Map<String, NDataModel.NamedColumn> newDimMap = Maps.newLinkedHashMap();
                Map<String, NDataModel.Measure> newMeasureMap = Maps.newLinkedHashMap();
                recItems.forEach(recItem -> {
                    recItem.getComputedColumns().stream() //
                            .filter(LayoutRecDetailResponse.RecComputedColumn::isNew) //
                            .forEach(recCC -> {
                                ComputedColumnDesc cc = recCC.getCc();
                                newCCMap.putIfAbsent(cc.getFullName(), cc);
                            });
                    recItem.getDimensions().stream() //
                            .filter(LayoutRecDetailResponse.RecDimension::isNew) //
                            .forEach(recDim -> {
                                NDataModel.NamedColumn dim = recDim.getDimension();
                                newDimMap.putIfAbsent(dim.getAliasDotColumn(), dim);
                            });
                    recItem.getMeasures().stream() //
                            .filter(LayoutRecDetailResponse.RecMeasure::isNew) //
                            .forEach(recMeasure -> {
                                NDataModel.Measure measure = recMeasure.getMeasure();
                                newMeasureMap.putIfAbsent(measure.getName(), measure);
                            });
                });
                newCCMap.forEach((ccName, cc) -> {
                    copyForWrite.getComputedColumnDescs().add(cc);
                    NDataModel.NamedColumn column = columnMap.get(cc.getFullName());
                    allNamedColumns.add(column);
                    namedColumnMap.putIfAbsent(column.getId(), column);
                });
                newDimMap.forEach((colName, dim) -> {
                    if (namedColumnMap.containsKey(dim.getId())) {
                        namedColumnMap.get(dim.getId()).setStatus(NDataModel.ColumnStatus.DIMENSION);
                    } else {
                        allNamedColumns.add(dim);
                    }
                });
                Set<Integer> namedColumnIds = allNamedColumns.stream().map(NDataModel.NamedColumn::getId)
                        .collect(Collectors.toSet());
                modelRequest.getAllNamedColumns().forEach(col -> {
                    if (!namedColumnIds.contains(col.getId())) {
                        allNamedColumns.add(col);
                        namedColumnIds.add(col.getId());
                    }
                });
                newMeasureMap.forEach((measureName, measure) -> copyForWrite.getAllMeasures().add(measure));
                // keep order of all columns and measures
                copyForWrite.keepColumnOrder();
                copyForWrite.keepMeasureOrder();
            });

            // expand
            semanticUpdater.deleteExpandableMeasureInternalMeasures(updated);
            semanticUpdater.expandExpandableMeasure(updated);
            preProcessBeforeModelSave(updated, project);
            NDataModel expanded = getDataModelManager(project).updateDataModelDesc(updated);

            // update IndexPlan
            IndexPlan indexPlan = modelRequest.getIndexPlan();
            indexPlanService.expandIndexPlanRequest(indexPlan, expanded);
            Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();
            indexPlan.getAllLayouts().forEach(layout -> layoutMap.putIfAbsent(layout.getId(), layout));
            indexPlanManager.updateIndexPlan(modelRequest.getId(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (LayoutRecDetailResponse recItem : recItems) {
                    long layoutId = recItem.getIndexId();
                    LayoutEntity layout = layoutMap.get(layoutId);
                    updateHandler.add(layout, IndexEntity.isAggIndex(recItem.getIndexId()));
                }
                updateHandler.complete();
            });
            modelChangeSupporters.forEach(listener -> listener.onUpdateSingle(project, modelRequest.getUuid()));
            baseIndexUpdater.update(indexPlanService);
        }
    }

    public NDataModel createModel(String project, ModelRequest modelRequest) {
        checkNewModels(project, Lists.newArrayList(modelRequest));
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModel model = saveModel(project, modelRequest);
            modelRequest.setUuid(model.getUuid());
            updateExcludedCheckerResult(project, modelRequest);
            // enable second storage
            if (modelRequest.isWithSecondStorage() && !SecondStorageUtil.isModelEnable(project, model.getId())) {
                SecondStorageUtil.initModelMetaData(project, model.getId());
            }
            return getDataModelManager(project).getDataModelDesc(model.getUuid());
        }, project);
    }

    private NDataModel doCheckBeforeModelSave(String project, ModelRequest modelRequest) {
        checkAliasExist(modelRequest.getUuid(), modelRequest.getAlias(), project);
        modelRequest.setOwner(AclPermissionUtil.getCurrentUsername());
        modelRequest.setLastModified(modelRequest.getCreateTime());
        checkModelRequest(modelRequest);

        //remove some attributes in modelResponse to fit NDataModel
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);
        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        if (MaintainModelType.AUTO_MAINTAIN == prj.getMaintainModelType()
                || ManagementType.TABLE_ORIENTED == dataModel.getManagementType()) {
            throw new KylinException(ServerErrorCode.FAILED_CREATE_MODEL, MsgPicker.getMsg().getINVALID_CREATE_MODEL());
        }

        preProcessBeforeModelSave(dataModel, project);
        checkFlatTableSql(dataModel);
        return dataModel;
    }

    private NDataModel saveModel(String project, ModelRequest modelRequest) {
        validatePartitionDateColumn(modelRequest);

        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        preProcessBeforeModelSave(dataModel, project);
        createStreamingJob(project, dataModel, modelRequest);
        var careted = getDataModelManager(project).createDataModelDesc(dataModel, dataModel.getOwner());

        semanticUpdater.expandExpandableMeasure(careted);
        preProcessBeforeModelSave(careted, project);
        val model = getDataModelManager(project).updateDataModelDesc(careted);

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(model.getUuid());
        indexPlan.setLastModified(System.currentTimeMillis());
        addBaseIndex(modelRequest, model, indexPlan);
        indexPlanManager.createIndexPlan(indexPlan);
        val df = dataflowManager.createDataflow(indexPlan, model.getOwner(), RealizationStatusEnum.OFFLINE);
        SegmentRange range = null;
        if (PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc())) {
            range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        } else if (StringUtils.isNotEmpty(modelRequest.getStart()) && StringUtils.isNotEmpty(modelRequest.getEnd())) {
            range = getSegmentRangeByModel(project, model.getUuid(), modelRequest.getStart(), modelRequest.getEnd());
        }
        if (range != null) {
            dataflowManager.fillDfManually(df, Lists.newArrayList(range));
        }
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> EventBusFactory.getInstance()
                .postSync(new ModelAddEvent(project, model.getId(), model.getAlias())));
        return getDataModelManager(project).getDataModelDesc(model.getUuid());
    }

    public void addBaseIndex(ModelRequest modelRequest, NDataModel model, IndexPlan indexPlan) {
        if (!modelRequest.isWithSecondStorage() && NDataModel.ModelType.BATCH == model.getModelType()
                && modelRequest.isWithBaseIndex()) {
            indexPlan.createAndAddBaseIndex(model);
        } else if (modelRequest.isWithSecondStorage()) {
            indexPlan.createAndAddBaseIndex(Collections.singletonList(indexPlan.createBaseTableIndex(model)));
        }
    }

    // for streaming & fusion model
    private void createStreamingJob(String project, NDataModel model, ModelRequest request) {
        if (NDataModel.ModelType.BATCH != model.getModelType()) {
            val jobManager = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            jobManager.createStreamingJob(model);
            createBatchModelInFusion(project, model, request);
        }
    }

    // only for fusion model
    // create batch side model
    private void createBatchModelInFusion(String project, NDataModel model, ModelRequest request) {
        KafkaConfig kafkaConfig = model.getRootFactTableRef().getTableDesc().getKafkaConfig();
        if (kafkaConfig.hasBatchTable()) {
            String tableName = kafkaConfig.getBatchTable();

            ModelRequest copy = JsonUtil.deepCopyQuietly(request, ModelRequest.class);
            copy.setAlias(FusionModel.getBatchName(request.getAlias(), model.getUuid()));
            copy.setRootFactTableName(tableName);
            copy.setFusionId(model.getUuid());
            copy.setProject(project);
            model.setFusionId(model.getUuid());

            String tableAlias = kafkaConfig.getBatchTableAlias();
            String oldAliasName = model.getRootFactTableRef().getTableName();
            convertToDataModelResponse(copy, tableAlias, oldAliasName);
            NDataModel copyModel = saveModel(project, copy);
            createFusionModel(project, model, copyModel);
        }
    }

    private void createFusionModel(String project, NDataModel model, NDataModel copyModel) {
        FusionModel fusionModel = new FusionModel(model, copyModel);
        FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        fusionModelManager.createModel(fusionModel);
    }

    private void convertToDataModelResponse(ModelRequest copy, String tableName, String oldAliasName) {
        copy.getSimplifiedJoinTableDescs()
                .forEach(x -> x.getSimplifiedJoinDesc().changeFKTableAlias(oldAliasName, tableName));
        copy.getSimplifiedDimensions().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getSimplifiedMeasures().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
    }

    void updateIndexPlan(String project, IndexPlan indexPlan) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        indexPlanManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            if (indexPlan.getAggShardByColumns() != null) {
                copyForWrite.setAggShardByColumns(indexPlan.getAggShardByColumns());
            }
            if (CollectionUtils.isNotEmpty(indexPlan.getIndexes())) {
                copyForWrite.setIndexes(indexPlan.getIndexes());
            }
            copyForWrite.setEngineType(indexPlan.getEngineType());
            copyForWrite.setIndexPlanOverrideIndexes(indexPlan.getIndexPlanOverrideIndexes());
            copyForWrite.setLastModified(System.currentTimeMillis());
        });
    }

    private void checkModelRequest(ModelRequest request) {
        checkModelOwner(request);
        checkModelDimensions(request);
        checkModelMeasures(request);
        checkModelJoinConditions(request);
    }

    private void checkModelOwner(ModelRequest request) {
        if (StringUtils.isBlank(request.getOwner())) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, "Invalid parameter, model owner is empty.");
        }
    }

    @VisibleForTesting
    public void checkModelDimensions(ModelRequest request) {
        Set<String> dimensionNames = new HashSet<>();

        KylinConfig kylinConfig = getProjectManager().getProject(request.getProject()).getConfig();
        int maxModelDimensionMeasureNameLength = kylinConfig.getMaxModelDimensionMeasureNameLength();

        for (NDataModel.NamedColumn dimension : request.getSimplifiedDimensions()) {
            dimension.setName(StringUtils.trim(dimension.getName()));
            // check if the dimension name is valid
            if (StringUtils.length(dimension.getName()) > maxModelDimensionMeasureNameLength
                    || !Pattern.compile(VALID_NAME_FOR_DIMENSION).matcher(dimension.getName()).matches())
                throw new KylinException(ServerErrorCode.INVALID_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_DIMENSION_NAME(), dimension.getName(),
                                maxModelDimensionMeasureNameLength));

            // check duplicate dimension names
            if (dimensionNames.contains(dimension.getName()))
                throw new KylinException(ServerErrorCode.DUPLICATE_DIMENSION_NAME, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDUPLICATE_DIMENSION_NAME(), dimension.getName()));

            dimensionNames.add(dimension.getName());
        }
    }

    @VisibleForTesting
    public void checkModelMeasures(ModelRequest request) {
        Set<String> measureNames = new HashSet<>();
        Set<SimplifiedMeasure> measures = new HashSet<>();
        KylinConfig kylinConfig = getProjectManager().getProject(request.getProject()).getConfig();
        int maxModelDimensionMeasureNameLength = kylinConfig.getMaxModelDimensionMeasureNameLength();

        for (SimplifiedMeasure measure : request.getSimplifiedMeasures()) {
            measure.setName(StringUtils.trim(measure.getName()));
            // check if the measure name is valid
            if (StringUtils.length(measure.getName()) > maxModelDimensionMeasureNameLength
                    || !checkIsValidMeasureName(measure.getName()))
                throw new KylinException(ServerErrorCode.INVALID_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MEASURE_NAME(), measure.getName(),
                                maxModelDimensionMeasureNameLength));

            // check duplicate measure names
            if (measureNames.contains(measure.getName()))
                throw new KylinException(ServerErrorCode.DUPLICATE_MEASURE_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDUPLICATE_MEASURE_NAME(), measure.getName()));

            // check duplicate measure definitions
            SimplifiedMeasure dupMeasure = null;
            for (SimplifiedMeasure m : measures) {
                if (isDupMeasure(measure, m)) {
                    dupMeasure = m;
                    break;
                }
            }

            if (dupMeasure != null) {
                dupMeasure = dupMeasure.getId() != 0 ? dupMeasure : measure;
                if (request.getId() != null) {
                    NDataModel.Measure existingMeasure = getModelById(request.getId(), request.getProject())
                            .getEffectiveMeasures().get(dupMeasure.getId());
                    if (existingMeasure != null && existingMeasure.getType() == NDataModel.MeasureType.INTERNAL) {
                        throw new KylinException(ServerErrorCode.DUPLICATE_MEASURE_EXPRESSION,
                                String.format(Locale.ROOT,
                                        MsgPicker.getMsg().getDUPLICATE_INTERNAL_MEASURE_DEFINITION(),
                                        measure.getName()));
                    }
                }
                throw new KylinException(ServerErrorCode.DUPLICATE_MEASURE_EXPRESSION, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDUPLICATE_MEASURE_DEFINITION(), measure.getName()));
            }

            measureNames.add(measure.getName());
            measures.add(measure);
        }
    }

    private boolean checkIsValidMeasureName(String measureName) {
        if (!KylinConfig.getInstanceFromEnv().isMeasureNameCheckEnabled()) {
            return true;
        }
        return Pattern.compile(VALID_NAME_FOR_MEASURE).matcher(measureName).matches();
    }

    private boolean isDupMeasure(SimplifiedMeasure measure, SimplifiedMeasure measure1) {
        if (measure.getExpression().equalsIgnoreCase(measure1.getExpression())
                && Objects.equals(measure.getParameterValue(), measure1.getParameterValue())
                && Objects.equals(measure.getConfiguration(), measure1.getConfiguration())) {

            // if both has a return type then compare the return type
            if (!Strings.isNullOrEmpty(measure1.getReturnType()) && !Strings.isNullOrEmpty(measure.getReturnType())) {
                return Objects.equals(measure1.getReturnType(), measure.getReturnType());
            } else {
                // otherwise if return type is null on any side, then just compare expr and params
                // one measure may be a newly added one and has not been assigned a return type yet
                return true;
            }
        }
        return false;
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
                    throw new KylinException(ServerErrorCode.DUPLICATE_JOIN_CONDITION, String.format(Locale.ROOT,
                            MsgPicker.getMsg().getDUPLICATE_JOIN_CONDITIONS(), primaryKeys[i], foreignKey[i]));

                joinKeys.add(Pair.newPair(primaryKeys[i], foreignKey[i]));
            }
        }
    }

    public String probeDateFormatIfNotExist(String project, NDataModel modelDesc) throws Exception {
        val partitionDesc = modelDesc.getPartitionDesc();
        if (PartitionDesc.isEmptyPartitionDesc(partitionDesc)
                || StringUtils.isNotEmpty(partitionDesc.getPartitionDateFormat()))
            return "";

        if (StringUtils.isNotEmpty(partitionDesc.getPartitionDateColumn())
                && StringUtils.isNotEmpty(partitionDesc.getPartitionDateFormat())) {
            return partitionDesc.getPartitionDateColumn();
        }

        String partitionColumn = modelDesc.getPartitionDesc().getPartitionDateColumnRef().getExpressionInSourceDB();

        val date = PushDownUtil.getFormatIfNotExist(modelDesc.getRootFactTableName(), partitionColumn, project);
        return DateFormat.proposeDateFormat(date);
    }

    public void saveDateFormatIfNotExist(String project, String modelId, String format) {
        if (StringUtils.isEmpty(format)) {
            return;
        }
        getDataModelManager(project).updateDataModel(modelId, model -> {
            model.getPartitionDesc().setPartitionDateFormat(format);
        });

    }

    private Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String table,
            PartitionDesc desc) throws Exception {
        Preconditions.checkNotNull(desc);
        String partitionColumn = desc.getPartitionDateColumn();
        String dateFormat = desc.getPartitionDateFormat();
        Preconditions.checkArgument(StringUtils.isNotEmpty(dateFormat) && StringUtils.isNotEmpty(partitionColumn));

        val minAndMaxTime = PushDownUtil.getMaxAndMinTimeWithTimeOut(partitionColumn, table, project);

        return new Pair<>(DateFormat.getFormattedDate(minAndMaxTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(minAndMaxTime.getSecond(), dateFormat));
    }

    public SegmentCheckResponse checkSegHoleExistIfNewRangeBuild(String project, String modelId, String start,
            String end) {
        aclEvaluate.checkProjectOperationPermission(project);
        Preconditions.checkArgument(!PushDownUtil.needPushdown(start, end), "Load data must set start and end date");
        NDataModel dataModelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        TableDesc table = getTableManager(project).getTableDesc(dataModelDesc.getRootFactTableName());
        SegmentRange segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);
        List<NDataSegment> segmentGaps = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .checkHoleIfNewSegBuild(modelId, segmentRangeToBuild);

        Segments<NDataSegment> segments = getSegmentsByRange(modelId, project, "0", "" + Long.MAX_VALUE);
        val overlapSegments = segments.stream().filter(seg -> seg.getSegRange().overlaps(segmentRangeToBuild))
                .map(seg -> new SegmentRangeResponse(seg.getTSRange().getStart(), seg.getTSRange().getEnd()))
                .collect(Collectors.toList());

        SegmentCheckResponse segmentCheckResponse = new SegmentCheckResponse();
        val segHoles = segmentGaps.stream()
                .map(seg -> new SegmentRangeResponse(seg.getTSRange().getStart(), seg.getTSRange().getEnd()))
                .collect(Collectors.toList());
        segmentCheckResponse.setSegmentHoles(segHoles);
        segmentCheckResponse.setOverlapSegments(overlapSegments);
        return segmentCheckResponse;
    }

    public SegmentCheckResponse checkSegHoleIfSegDeleted(String model, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (ManagementType.TABLE_ORIENTED == dataModel.getManagementType()) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getMODEL_SEGMENT_CAN_NOT_REMOVE(), dataModel.getAlias()));
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsExistById(model, project, ids);
        checkSegmentsStatus(model, project, ids, SegmentStatusEnumToDisplay.LOCKED);
        NDataflow dataflow = dataflowManager.getDataflow(model);
        val toDeletedSeg = dataflow.getSegments().stream().filter(seg -> Arrays.asList(ids).contains(seg.getId()))
                .collect(Collectors.toList());
        val remainSegs = dataflow.getSegments().stream().filter(seg -> !Arrays.asList(ids).contains(seg.getId()))
                .collect(Collectors.toList());
        val segHoles = dataflowManager.calculateHoles(model, remainSegs).stream()
                .filter(seg -> toDeletedSeg.stream()
                        .anyMatch(deletedSeg -> deletedSeg.getSegRange().overlaps(seg.getSegRange())))
                .map(seg -> new SegmentRangeResponse(seg.getTSRange().getStart(), seg.getTSRange().getEnd()))
                .collect(Collectors.toList());
        SegmentCheckResponse response = new SegmentCheckResponse();
        response.setSegmentHoles(segHoles);
        return response;
    }

    public JobInfoResponse fixSegmentHoles(String project, String modelId, List<SegmentTimeRequest> segmentHoles,
            Set<String> ignoredSnapshotTables) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        checkModelAndIndexManually(project, modelId);
        String format = probeDateFormatIfNotExist(project, modelDesc);

        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            List<JobInfoResponse.JobInfo> jobInfos = Lists.newArrayList();
            List<String[]> allPartitions = null;
            if (modelDesc.isMultiPartitionModel()) {
                allPartitions = modelDesc.getMultiPartitionDesc().getPartitions().stream()
                        .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
            }
            for (SegmentTimeRequest hole : segmentHoles) {
                jobInfos.add(modelBuildService.constructIncrementBuild(
                        new IncrementBuildSegmentParams(project, modelId, hole.getStart(), hole.getEnd(), format, true,
                                allPartitions).withIgnoredSnapshotTables(ignoredSnapshotTables)));
            }
            return jobInfos;
        }, project);

        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;

    }

    public void removeIndexesFromSegments(String project, String modelId, List<String> segmentIds,
            List<Long> indexIds) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkModelPermission(project, modelId);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val dfManger = getDataflowManager(project);
            NDataflow dataflow = dfManger.getDataflow(modelId);
            for (String segmentId : segmentIds) {
                NDataSegment seg = dataflow.getSegment(segmentId);
                NDataSegDetails segDetails = seg.getSegDetails();
                List<NDataLayout> layouts = new LinkedList<>(segDetails.getLayouts());
                layouts.removeIf(layout -> indexIds.contains(layout.getLayoutId()));
                dfManger.updateDataflowDetailsLayouts(seg, layouts);
            }
            getIndexPlanManager(project).updateIndexPlan(dataflow.getUuid(),
                    IndexPlan::removeTobeDeleteIndexIfNecessary);

            if (SecondStorageUtil.isModelEnable(project, modelId)) {
                SecondStorage.tableFlowManager(getConfig(), project).get(modelId).ifPresent(tableFlow -> {
                    val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
                    Preconditions.checkState(tablePlanManager.isPresent());
                    Preconditions.checkState(tablePlanManager.get().get(modelId).isPresent());
                    val tablePlan = tablePlanManager.get().get(modelId).get();

                    Set<Long> needDeleteLayoutIds = tableFlow.getTableDataList().stream()
                            .filter(tableData -> indexIds.contains(tableData.getLayoutID()))
                            .filter(tableData -> tableData.getPartitions().stream()
                                    .allMatch(tablePartition -> segmentIds.contains(tablePartition.getSegmentId()))
                            )
                            .map(TableData::getLayoutID)
                            .filter(layoutId -> dataflow.getIndexPlan().getBaseTableLayoutId().longValue() != layoutId.longValue())
                            .collect(Collectors.toSet());

                    SecondStorageUtil.cleanSegments(project, modelId, new HashSet<>(segmentIds), new HashSet<>(indexIds));

                    tableFlow.update(copied -> copied.cleanTableData(tableData -> needDeleteLayoutIds.contains(tableData.getLayoutID())));
                    tablePlan.update(t -> t.cleanTable(needDeleteLayoutIds));

                    val jobHandler = new SecondStorageIndexCleanJobHandler();
                    final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(project, modelId,
                            BasicService.getUsername(), new HashSet<>(indexIds), new HashSet<>(segmentIds));
                    getJobManager(project).addJob(param, jobHandler);
                });
            }
            return null;
        }, project);
    }

    ModelRequest convertToRequest(NDataModel modelDesc) throws IOException {
        val request = new ModelRequest(JsonUtil.deepCopy(modelDesc, NDataModel.class));
        request.setSimplifiedMeasures(modelDesc.getEffectiveMeasures().values().stream()
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(modelDesc.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList()));
        request.setComputedColumnDescs(modelDesc.getComputedColumnDescs());

        if (SecondStorageUtil.isModelEnable(modelDesc.getProject(), modelDesc.getId())) {
            request.setWithSecondStorage(true);
        }

        return request;
    }

    @Transaction(project = 0)
    public void updatePartitionColumn(String project, String modelId, PartitionDesc partitionDesc,
            MultiPartitionDesc multiPartitionDesc) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelPermission(project, modelId);
        val dataflowManager = getDataflowManager(project);
        val df = dataflowManager.getDataflow(modelId);
        val model = df.getModel();
        ModelUtils.checkPartitionColumn(model, partitionDesc, MsgPicker.getMsg().getPARTITION_COLUMN_SAVE_ERROR());

        if (PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc()) && df.getFirstSegment() == null
                && !model.isMultiPartitionModel()) {
            dataflowManager.fillDfManually(df,
                    Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
        }
        if (!Objects.equals(partitionDesc, model.getPartitionDesc())
                || !ModelSemanticHelper.isMultiPartitionDescSame(model.getMultiPartitionDesc(), multiPartitionDesc)) {
            val request = convertToRequest(model);
            request.setProject(project);
            request.setPartitionDesc(partitionDesc);
            request.setSaveOnly(true);
            request.setMultiPartitionDesc(multiPartitionDesc);
            boolean isClean = updateSecondStorageModel(project, modelId, true);
            updateDataModelSemantic(project, request, !isClean);
        }
    }

    public List<MultiPartitionValueResponse> getMultiPartitionValues(String project, String modelId) {
        val model = getModelById(modelId, project);
        val multiPartitionDesc = model.getMultiPartitionDesc();
        val segments = getDataflowManager(project).getDataflow(modelId).getSegments();
        val totalSegCount = segments.size();
        val responses = Lists.<MultiPartitionValueResponse> newArrayList();

        if (multiPartitionDesc == null || CollectionUtils.isEmpty(multiPartitionDesc.getPartitions())) {
            return responses;
        }

        for (MultiPartitionDesc.PartitionInfo partition : multiPartitionDesc.getPartitions()) {
            val builtSegmentCount = (int) segments.stream()
                    .filter(segment -> segment.getMultiPartitionIds().contains(partition.getId())).count();
            responses.add(new MultiPartitionValueResponse(partition.getId(), partition.getValues(), builtSegmentCount,
                    totalSegCount));
        }

        return responses;
    }

    /**
     * batch update multiple partition values
     * @param project
     * @param modelId
     * @param partitionValues
     * @return
     */
    @Transaction(project = 0)
    public NDataModel batchUpdateMultiPartition(String project, String modelId, List<String[]> partitionValues) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel dataModel = modelManager.getDataModelDesc(modelId);
        if (dataModel == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }

        MultiPartitionDesc multiPartitionDesc = dataModel.getMultiPartitionDesc();

        Set<Long> tobeDeletedPartitions = multiPartitionDesc.getPartitions().stream()
                .filter(partitionInfo -> partitionValues.stream()
                        .noneMatch(pv -> Objects.deepEquals(pv, partitionInfo.getValues())))
                .map(MultiPartitionDesc.PartitionInfo::getId).collect(Collectors.toSet());

        if (!tobeDeletedPartitions.isEmpty()) {
            logger.debug("Import model {} delete partitions {}", dataModel.getAlias(), tobeDeletedPartitions);
            deletePartitions(dataModel.getProject(), null, dataModel.getUuid(), tobeDeletedPartitions);
        }

        dataModel = modelManager.getDataModelDesc(modelId);

        modelManager.addPartitionsIfAbsent(dataModel, partitionValues);

        return modelManager.getDataModelDesc(modelId);
    }

    @Transaction(project = 0)
    public void addMultiPartitionValues(String project, String modelId, List<String[]> subPartitionValues) {
        aclEvaluate.checkProjectOperationPermission(project);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelManager.getDataModelDesc(modelId);
        modelManager.addPartitionsIfAbsent(model, subPartitionValues);
    }

    private void checkModelAndIndexManually(String project, String modelId) {
        checkModelAndIndexManually(new FullBuildSegmentParams(project, modelId, true));
    }

    public void checkModelAndIndexManually(FullBuildSegmentParams params) {
        NDataModel modelDesc = getDataModelManager(params.getProject()).getDataModelDesc(params.getModelId());
        if (ManagementType.MODEL_BASED != modelDesc.getManagementType()) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT_MANUALLY(), modelDesc.getAlias()));
        }

        if (params.isNeedBuild()) {
            val indexPlan = getIndexPlan(params.getModelId(), params.getProject());
            if (indexPlan == null || indexPlan.getAllLayouts().isEmpty()) {
                throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                        MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT());
            }
        }
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

    public void checkSegmentToBuildOverlapsBuilt(String project, String model, SegmentRange segmentRangeToBuild) {
        Segments<NDataSegment> segments = getSegmentsByRange(model, project, "0", "" + Long.MAX_VALUE);
        if (!CollectionUtils.isEmpty(segments)) {
            for (NDataSegment existedSegment : segments) {
                if (existedSegment.getSegRange().overlaps(segmentRangeToBuild)) {
                    throw new KylinException(ServerErrorCode.SEGMENT_RANGE_OVERLAP,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_RANGE_OVERLAP(),
                                    existedSegment.getSegRange().getStart().toString(),
                                    existedSegment.getSegRange().getEnd().toString()));
                }
            }
        }
    }

    public void primaryCheck(NDataModel modelDesc) {
        Message msg = MsgPicker.getMsg();

        if (modelDesc == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST, msg.getINVALID_MODEL_DEFINITION());
        }

        String modelAlias = modelDesc.getAlias();

        if (StringUtils.isEmpty(modelAlias)) {
            throw new KylinException(ServerErrorCode.EMPTY_MODEL_NAME, msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL)) {
            throw new KylinException(ServerErrorCode.INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, msg.getINVALID_MODEL_NAME(), modelAlias));
        }
    }

    public ComputedColumnUsageResponse getComputedColumnUsages(String project) {
        aclEvaluate.checkProjectWritePermission(project);
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
     * check if the computed column expressions are valid (in hive)
     * <p>
     * ccInCheck is optional, if provided, other cc in the model will skip hive check
     */
    public ComputedColumnCheckResponse checkComputedColumn(NDataModel model, String project, String ccInCheck) {
        aclEvaluate.checkProjectWritePermission(project);
        if (model.getUuid() == null) {
            model.updateRandomUuid();
        }

        model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);
        model.getComputedColumnDescs().forEach(cc -> {
            String innerExp = KapQueryUtil.massageComputedColumn(model, project, cc, null);
            cc.setInnerExpression(innerExp);
        });

        if (model.isSeekingCCAdvice()) {
            // if it's seeking for advice, it should have thrown exceptions by far
            throw new IllegalStateException("No advice could be provided");
        }

        checkCCNameAmbiguity(model);
        ComputedColumnDesc checkedCC = null;

        Set<String> excludedTables = getFavoriteRuleManager(project).getExcludedTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, model.getJoinTables(), model);
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            checkCCName(cc.getColumnName());

            if (!StringUtils.isEmpty(ccInCheck) && !StringUtils.equalsIgnoreCase(cc.getFullName(), ccInCheck)) {
                checkCascadeErrorOfNestedCC(cc, ccInCheck);
            } else {
                //replace computed columns with basic columns
                String antiFlattenLookup = checker.detectAntiFlattenLookup(cc);
                if (antiFlattenLookup != null) {
                    throw new KylinException(ServerErrorCode.COMPUTED_COLUMN_DEPENDS_ANTI_FLATTEN_LOOKUP, String
                            .format(Locale.ROOT, MsgPicker.getMsg().getCC_ON_ANTI_FLATTEN_LOOKUP(), antiFlattenLookup));
                }
                ComputedColumnDesc.simpleParserCheck(cc.getExpression(), model.getAliasMap().keySet());
                String innerExpression = KapQueryUtil.massageComputedColumn(model, project, cc,
                        AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
                cc.setInnerExpression(innerExpression);

                //check by data source, this could be slow
                long ts = System.currentTimeMillis();
                ComputedColumnEvalUtil.evaluateExprAndType(model, cc);
                logger.debug("Spent {} ms to visit data source to validate computed column expression: {}",
                        (System.currentTimeMillis() - ts), cc.getExpression());
                checkedCC = cc;
            }
        }

        Preconditions.checkState(checkedCC != null, "No computed column match: {}", ccInCheck);
        // check invalid measure removed due to cc data type change
        val modelManager = getDataModelManager(model.getProject());
        val oldDataModel = modelManager.getDataModelDesc(model.getUuid());

        // brand new model, no measure to remove
        if (oldDataModel == null)
            return getComputedColumnCheckResponse(checkedCC, new ArrayList<>());

        val copyModel = modelManager.copyForWrite(oldDataModel);
        val request = new ModelRequest(model);
        request.setProject(model.getProject());
        request.setMeasures(model.getAllMeasures());
        UpdateImpact updateImpact = semanticUpdater.updateModelColumns(copyModel, request);
        // get invalid measure names in original model
        val removedMeasures = updateImpact.getInvalidMeasures();
        val measureNames = oldDataModel.getAllMeasures().stream().filter(m -> removedMeasures.contains(m.getId()))
                .map(NDataModel.Measure::getName).collect(Collectors.toList());
        // get invalid measure names in request
        val removedRequestMeasures = updateImpact.getInvalidRequestMeasures();
        val requestMeasureNames = request.getAllMeasures().stream()
                .filter(m -> removedRequestMeasures.contains(m.getId())).map(NDataModel.Measure::getName)
                .collect(Collectors.toList());
        measureNames.addAll(requestMeasureNames);
        return getComputedColumnCheckResponse(checkedCC, measureNames);
    }

    static void checkCCName(String name) {
        if (PushDownConverterKeyWords.CALCITE.contains(name.toUpperCase(Locale.ROOT))
                || PushDownConverterKeyWords.HIVE.contains(name.toUpperCase(Locale.ROOT))) {
            throw new KylinException(ServerErrorCode.INVALID_NAME, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_COMPUTER_COLUMN_NAME_WITH_KEYWORD(), name));
        }
        if (!Pattern.compile("^[a-zA-Z]+\\w*$").matcher(name).matches()) {
            throw new KylinException(ServerErrorCode.INVALID_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_COMPUTER_COLUMN_NAME(), name));
        }
    }

    public void checkCCNameAmbiguity(NDataModel model) {
        Set<String> ambiguousCCNameSet = Sets.newHashSet();

        Set<String> ccColumnNames = Sets.newHashSet();
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            if (ccColumnNames.contains(cc.getColumnName())) {
                ambiguousCCNameSet.add(cc.getColumnName());
            } else {
                ccColumnNames.add(cc.getColumnName());
            }
        }
        if (CollectionUtils.isEmpty(ccColumnNames)) {
            return;
        }

        for (TableRef table : model.getFactTables()) {
            for (TblColRef tblColRef : table.getColumns()) {
                if (tblColRef.getColumnDesc().isComputedColumn()) {
                    continue;
                }

                if (ccColumnNames.contains(tblColRef.getName())) {
                    ambiguousCCNameSet.add(tblColRef.getName());
                }
            }
        }

        if (CollectionUtils.isNotEmpty(ambiguousCCNameSet)) {
            StringBuilder error = new StringBuilder();
            ambiguousCCNameSet.forEach(name -> {
                error.append(String.format(Locale.ROOT, MsgPicker.getMsg().getCHECK_CC_AMBIGUITY(), name));
                error.append("\r\n");
            });
            throw new KylinException(ServerErrorCode.DUPLICATE_COMPUTED_COLUMN_NAME, error.toString());
        }
    }

    void preProcessBeforeModelSave(NDataModel model, String project) {
        if (!model.getComputedColumnDescs().isEmpty()) {
            model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                    getDataflowManager(project).listUnderliningDataModels(), project, false, true);
        } else {
            model.init(getConfig(), getTableManager(project).getAllTablesMap(), Lists.newArrayList(), project, false,
                    true);
        }

        massageModelFilterCondition(model);

        checkCCNameAmbiguity(model);

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = KapQueryUtil.massageComputedColumn(model, project, ccDesc,
                    AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
            ccDesc.setInnerExpression(ccExpression);
            TblColRef tblColRef = model.findColumn(ccDesc.getTableAlias(), ccDesc.getColumnName());
            tblColRef.getColumnDesc().setComputedColumn(ccExpression);
        }

        ComputedColumnEvalUtil.evalDataTypeOfCCInBatch(model, model.getComputedColumnDescs());
    }

    @Transaction(project = 0)
    public void updateModelDataCheckDesc(String project, String modelId, long checkOptions, long faultThreshold,
            long faultActions) {
        aclEvaluate.checkProjectWritePermission(project);
        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelId);
        if (dataModel == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }

        dataModel.setDataCheckDesc(DataCheckDesc.valueOf(checkOptions, faultThreshold, faultActions));
        getDataModelManager(project).updateDataModelDesc(dataModel);
    }

    @Transaction(project = 1)
    public void deleteSegmentById(String model, String project, String[] ids, boolean force) {
        aclEvaluate.checkProjectOperationPermission(project);
        if (SecondStorageUtil.isModelEnable(project, model)) {
            LockTypeEnum.checkLock(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(project));
        }
        SecondStorageUtil.checkSegmentRemove(project, model, ids);
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (ManagementType.TABLE_ORIENTED == dataModel.getManagementType()) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getMODEL_SEGMENT_CAN_NOT_REMOVE(), dataModel.getAlias()));
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsExistById(model, project, ids);
        checkSegmentsStatus(model, project, ids, SegmentStatusEnumToDisplay.LOCKED);
        val indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Set<String> idsToDelete = Sets.newHashSet();
        for (String id : ids) {
            if (dataflow.getSegment(id) != null) {
                idsToDelete.add(id);
            } else {
                throw new IllegalArgumentException(String.format(Locale.ROOT, MsgPicker.getMsg().getSEG_NOT_FOUND(), id,
                        dataflow.getModelAlias()));
            }
        }
        if (SecondStorageUtil.isModelEnable(project, model)) {
            SecondStorageUtil.cleanSegments(project, model, idsToDelete);
            val jobHandler = new SecondStorageSegmentCleanJobHandler();
            final JobParam param = SecondStorageJobParamUtil.segmentCleanParam(project, model,
                    BasicService.getUsername(), idsToDelete);
            getJobManager(project).addJob(param, jobHandler);
        }
        segmentHelper.removeSegment(project, dataflow.getUuid(), idsToDelete);
        offlineModelIfNecessary(dataflowManager, model);
    }

    private void offlineModelIfNecessary(NDataflowManager dfManager, String modelId) {
        NDataflow df = dfManager.getDataflow(modelId);
        if (df.getSegments().isEmpty() && RealizationStatusEnum.ONLINE == df.getStatus()) {
            dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.OFFLINE);
        }
    }

    public void checkSegmentsExistById(String modelId, String project, String[] ids) {
        checkSegmentsExistById(modelId, project, ids, true);
    }

    public boolean checkSegmentsExistById(String modelId, String project, String[] ids, boolean shouldThrown) {
        Preconditions.checkNotNull(modelId);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(ids);
        aclEvaluate.checkProjectOperationPermission(project);

        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());

        List<String> notExistIds = Stream.of(ids).filter(segmentId -> null == dataflow.getSegment(segmentId))
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (shouldThrown && !CollectionUtils.isEmpty(notExistIds)) {
            throw new KylinException(ServerErrorCode.SEGMENT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_ID_NOT_EXIST(), StringUtils.join(notExistIds, ",")));
        }
        return CollectionUtils.isEmpty(notExistIds);
    }

    private void checkSegmentsExistByName(String model, String project, String[] names) {
        checkSegmentsExistByName(model, project, names, true);
    }

    public boolean checkSegmentsExistByName(String model, String project, String[] names, boolean shouldThrow) {
        Preconditions.checkNotNull(model);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(names);
        aclEvaluate.checkProjectOperationPermission(project);

        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());

        List<String> notExistNames = Stream.of(names)
                .filter(segmentName -> null == dataflow.getSegmentByName(segmentName)).filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (shouldThrow && !CollectionUtils.isEmpty(notExistNames)) {
            throw new KylinException(ServerErrorCode.SEGMENT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_NAME_NOT_EXIST(), StringUtils.join(notExistNames, ",")));
        }
        return CollectionUtils.isEmpty(notExistNames);
    }

    public void checkSegmentsStatus(String model, String project, String[] ids,
            SegmentStatusEnumToDisplay... statuses) {
        for (SegmentStatusEnumToDisplay status : statuses) {
            checkSegmentsStatus(model, project, ids, status);
        }
    }

    public void checkSegmentsStatus(String model, String project, String[] ids, SegmentStatusEnumToDisplay status) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Segments<NDataSegment> segments = dataflow.getSegments();
        String message = SegmentStatusEnumToDisplay.LOCKED == status ? MsgPicker.getMsg().getSEGMENT_LOCKED()
                : MsgPicker.getMsg().getSEGMENT_STATUS(status.name());
        for (String id : ids) {
            val segment = dataflow.getSegment(id);
            if (SegmentUtil.getSegmentStatusToDisplay(segments, segment, null) == status) {
                throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                        String.format(Locale.ROOT, message, segment.displayIdName()));
            }
        }
    }

    private void checkSegmentsContinuous(String modelId, String project, String[] ids) {
        val dfManager = getDataflowManager(project);
        val indexPlan = getIndexPlan(modelId, project);
        val df = dfManager.getDataflow(indexPlan.getUuid());
        List<NDataSegment> segmentList = Arrays.stream(ids).map(df::getSegment).sorted(NDataSegment::compareTo)
                .collect(Collectors.toList());

        for (int i = 0; i < segmentList.size() - 1; i++) {
            if (!segmentList.get(i).getSegRange().connects(segmentList.get(i + 1).getSegRange())) {
                throw new KylinException(ServerErrorCode.FAILED_MERGE_SEGMENT,
                        MsgPicker.getMsg().getSEGMENT_CONTAINS_GAPS());
            }
        }
    }

    public Pair<Long, Long> checkMergeSegments(MergeSegmentParams params) {
        String project = params.getProject();
        String modelId = params.getModelId();
        String[] ids = params.getSegmentIds();

        aclEvaluate.checkProjectOperationPermission(project);

        val dfManager = getDataflowManager(project);
        val df = dfManager.getDataflow(modelId);

        checkSegmentsExistById(modelId, project, ids);
        checkSegmentsStatus(modelId, project, ids, SegmentStatusEnumToDisplay.LOADING,
                SegmentStatusEnumToDisplay.REFRESHING, SegmentStatusEnumToDisplay.MERGING,
                SegmentStatusEnumToDisplay.LOCKED);
        checkSegmentsContinuous(modelId, project, ids);

        long start = Long.MAX_VALUE;
        long end = -1;

        for (String id : ids) {
            val segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSEG_NOT_FOUND(), id, df.getModelAlias()));
            }

            if (SegmentStatusEnum.READY != segment.getStatus() && SegmentStatusEnum.WARNING != segment.getStatus()) {
                throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                        MsgPicker.getMsg().getINVALID_MERGE_SEGMENT());
            }

            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();

            if (segmentStart < start)
                start = segmentStart;

            if (segmentEnd > end)
                end = segmentEnd;
        }

        return Pair.newPair(start, end);
    }

    @Transaction(project = 0)
    public List<JobInfoResponse.JobInfo> exportSegmentToSecondStorage(String project, String model,
            String[] segmentIds) {
        aclEvaluate.checkProjectOperationPermission(project);
        SecondStorageJobUtil.validateSegment(project, model, Arrays.asList(segmentIds));
        checkSegmentsExistById(model, project, segmentIds);
        checkSegmentsStatus(model, project, segmentIds, SegmentStatusEnumToDisplay.LOADING,
                SegmentStatusEnumToDisplay.REFRESHING, SegmentStatusEnumToDisplay.MERGING,
                SegmentStatusEnumToDisplay.LOCKED);

        if (!SecondStorage.enabled()) {
            throw new KylinException(JobErrorCode.JOB_CONFIGURATION_ERROR, "!!!No Tiered Storage is installed!!!");
        }
        val jobHandler = new SecondStorageSegmentLoadJobHandler();

        final JobParam param = SecondStorageJobParamUtil.of(project, model, BasicService.getUsername(),
                Stream.of(segmentIds));
        return Collections.singletonList(new JobInfoResponse.JobInfo(JobTypeEnum.EXPORT_TO_SECOND_STORAGE.toString(),
                getJobManager(project).addJob(param, jobHandler)));
    }

    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request) {
        return updateDataModelSemantic(project, request, true, true);
    }

    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request, boolean needClean) {
        return updateDataModelSemantic(project, request, needClean, true);
    }

    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request, boolean needClean,
            boolean isCheckFlat) {
        final boolean[] isClean = { needClean };
        try {
            return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                aclEvaluate.checkProjectWritePermission(project);
                semanticUpdater.expandModelRequest(request);
                checkModelRequest(request);
                checkModelPermission(project, request.getUuid());
                validatePartitionDateColumn(request);

                val modelId = request.getUuid();
                val modelManager = getDataModelManager(project);
                val originModel = modelManager.getDataModelDesc(modelId);

                val copyModel = modelManager.copyForWrite(originModel);
                UpdateImpact updateImpact = semanticUpdater.updateModelColumns(copyModel, request, true);
                val allTables = getTableManager(request.getProject()).getAllTablesMap();
                copyModel.init(modelManager.getConfig(), allTables,
                        getDataflowManager(project).listUnderliningDataModels(), project);

                BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(originModel,
                        request.isWithBaseIndex());

                preProcessBeforeModelSave(copyModel, project);
                val updated = modelManager.updateDataModelDesc(copyModel);

                // 1. delete old internal measure
                // 2. expand new measuers
                // the ordering of the two steps is important as tomb internal measure
                // should not be used to construct new expandable measures
                semanticUpdater.deleteExpandableMeasureInternalMeasures(updated);
                semanticUpdater.expandExpandableMeasure(updated);
                preProcessBeforeModelSave(updated, project);
                getDataModelManager(project).updateDataModelDesc(updated);

                indexPlanService.updateForMeasureChange(project, modelId, updateImpact.getInvalidMeasures(),
                        updateImpact.getReplacedMeasures());
                Set<Integer> affectedSet = updateImpact.getAffectedIds();
                val affectedLayoutSet = getAffectedLayouts(project, modelId, affectedSet);
                if (affectedLayoutSet.size() > 0)
                    indexPlanService.reloadLayouts(project, modelId, affectedLayoutSet);
                indexPlanService.clearShardColIfNotDim(project, modelId);

                var newModel = modelManager.getDataModelDesc(modelId);

                checkIndexColumnExist(project, modelId, originModel);

                if (isCheckFlat) {
                    checkFlatTableSql(newModel);
                }

                val result = semanticUpdater.doHandleSemanticUpdate(project, modelId, originModel, request.getStart(),
                        request.getEnd(), isClean[0]);
                var needBuild = result.getFirst();
                if (result.getSecond()) {
                    isClean[0] = false;
                }
                updateExcludedCheckerResult(project, request);
                baseIndexUpdater.setSecondStorageEnabled(request.isWithSecondStorage());
                baseIndexUpdater.setNeedCleanSecondStorage(isClean[0]);
                BuildBaseIndexResponse baseIndexResponse = baseIndexUpdater.update(indexPlanService);
                if (!request.isSaveOnly() && (needBuild || baseIndexResponse.hasIndexChange())) {
                    semanticUpdater.buildForModel(project, modelId);
                }
                modelChangeSupporters.forEach(listener -> listener.onUpdate(project, modelId));
                if (baseIndexResponse.isCleanSecondStorage()) {
                    isClean[0] = false;
                }
                changeSecondStorageIfNeeded(project, request, isClean[0]);
                return baseIndexResponse;
            }, project);
        } catch (TransactionException te) {
            Throwable root = ExceptionUtils.getCause(te);
            if (root instanceof RuntimeException) {
                throw (RuntimeException) root;
            }
            throw te;
        }
    }

    public boolean updateSecondStorageModel(String project, String modelId, boolean needCleanSecondStorage) {
        if (SecondStorageUtil.isModelEnable(project, modelId)) {
            SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);
            return updater.onUpdate(project, modelId, needCleanSecondStorage);
        }
        return false;
    }

    public void changeSecondStorageIfNeeded(String project, ModelRequest request, boolean needClean) {
        // disable second storage
        if (request.getId() != null && SecondStorageUtil.isModelEnable(project, request.getId())
                && !request.isWithSecondStorage()) {
            SecondStorageUtil.validateDisableModel(project, request.getId());
            if (needClean) {
                triggerModelClean(project, request.getId());
            }
            SecondStorageUtil.disableModel(project, request.getId());
        } else if (request.getId() != null && !SecondStorageUtil.isModelEnable(project, request.getId())
                && request.isWithSecondStorage()) {
            val indexPlanManager = getIndexPlanManager(project);
            if (!indexPlanManager.getIndexPlan(request.getId()).containBaseTableLayout()) {
                indexPlanManager.updateIndexPlan(request.getId(), copied -> {
                    copied.createAndAddBaseIndex(
                            Collections.singletonList(copied.createBaseTableIndex(copied.getModel())));
                });
            }
            SecondStorageUtil.initModelMetaData(project, request.getId());
        }
    }

    private void triggerModelClean(String project, String model) {
        val jobHandler = new SecondStorageModelCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, model, BasicService.getUsername());
        getJobManager(project).addJob(param, jobHandler);
    }

    public void updateExcludedCheckerResult(String project, ModelRequest request) {
        NDataModelManager modelManager = getDataModelManager(project);
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);

        String uuid = request.getUuid();
        NDataModel convertedModel = modelManager.getDataModelDesc(uuid);
        List<JoinTableDesc> joinTables = convertedModel.getJoinTables();

        IndexPlan indexPlan = indexPlanManager.getIndexPlan(uuid);
        Set<String> excludedTables = getFavoriteRuleManager(project).getExcludedTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, joinTables, convertedModel);
        List<ComputedColumnDesc> invalidCCList = checker.getInvalidComputedColumns(convertedModel);
        Set<Integer> invalidDimensions = checker.getInvalidDimensions(convertedModel);
        Set<Integer> invalidMeasures = checker.getInvalidMeasures(convertedModel);
        Set<Integer> invalidScope = Sets.newHashSet();
        invalidScope.addAll(invalidDimensions);
        invalidScope.addAll(invalidMeasures);
        Set<Long> invalidIndexes = checker.getInvalidIndexes(indexPlan, invalidScope);

        Map<String, ComputedColumnDesc> invalidCCMap = Maps.newHashMap();
        invalidCCList.forEach(cc -> invalidCCMap.put(cc.getColumnName(), cc));
        if (!invalidIndexes.isEmpty()) {
            indexPlanService.removeIndexes(project, uuid, invalidIndexes, invalidDimensions, invalidMeasures);
        }

        modelManager.updateDataModel(uuid, copyForWrite -> {
            copyForWrite.getComputedColumnDescs().removeIf(cc -> invalidCCMap.containsKey(cc.getColumnName()));
            copyForWrite.getAllMeasures().forEach(measure -> {
                if (invalidMeasures.contains(measure.getId())) {
                    measure.setTomb(true);
                }
            });
            copyForWrite.getAllNamedColumns().forEach(column -> {
                if (!column.isExist()) {
                    return;
                }
                if (invalidDimensions.contains(column.getId())) {
                    column.setStatus(NDataModel.ColumnStatus.EXIST);
                }

                if (invalidCCMap.containsKey(column.getName())) {
                    String colName = column.getAliasDotColumn();
                    final String fullName = invalidCCMap.get(column.getName()).getFullName();
                    if (fullName.equalsIgnoreCase(colName)) {
                        column.setStatus(NDataModel.ColumnStatus.TOMB);
                    }
                }
            });
        });
    }

    public String[] convertSegmentIdWithName(String modelId, String project, String[] segIds, String[] segNames) {
        String[] ids = segIds;

        if (ArrayUtils.isEmpty(segNames)) {
            return ids;
        }

        aclEvaluate.checkProjectOperationPermission(project);
        checkSegmentsExistByName(modelId, project, segNames);

        NDataflow dataflow = getDataflowManager(project).getDataflow(getIndexPlan(modelId, project).getUuid());

        ids = Stream.of(segNames).map(segmentName -> {
            val segmentByName = dataflow.getSegmentByName(segmentName);
            return Objects.isNull(segmentByName) ? null : segmentByName.getId();
        }).toArray(String[]::new);

        return ids;
    }

    private Set<Long> getAffectedLayouts(String project, String modelId, Set<Integer> modifiedSet) {
        var affectedLayoutSet = new HashSet<Long>();
        val indePlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indePlanManager.getIndexPlan(modelId);
        for (LayoutEntity layoutEntity : indexPlan.getAllLayouts()) {
            if (layoutEntity.getColOrder().stream().anyMatch(modifiedSet::contains)) {
                affectedLayoutSet.add(layoutEntity.getId());
            }
        }
        return affectedLayoutSet;
    }

    private void checkIndexColumnExist(String project, String modelId, NDataModel originModel) {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        var newModel = getDataModelManager(project).getDataModelDesc(modelId);

        // check agg group contains removed dimensions
        val rule = indexPlan.getRuleBasedIndex();
        if (rule != null) {
            if (!newModel.getEffectiveDimensions().keySet().containsAll(rule.getDimensions())) {
                val allDimensions = rule.getDimensions();
                val dimensionNames = allDimensions.stream()
                        .filter(id -> !newModel.getEffectiveDimensions().containsKey(id))
                        .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
                throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDIMENSION_NOTFOUND(), StringUtils.join(dimensionNames, ",")));
            }

            for (NAggregationGroup agg : rule.getAggregationGroups()) {
                if (!newModel.getEffectiveMeasures().keySet().containsAll(Sets.newHashSet(agg.getMeasures()))) {
                    val measureNames = Arrays.stream(agg.getMeasures())
                            .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                            .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                    throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                            MsgPicker.getMsg().getMEASURE_NOTFOUND(), StringUtils.join(measureNames, ",")));
                }
            }
        }

        //check table index contains removed columns
        val tableIndexColumns = indexPlan.getIndexes().stream().filter(IndexEntity::isTableIndex)
                .map(IndexEntity::getDimensions).flatMap(List::stream).collect(Collectors.toSet());
        val allSelectedColumns = newModel.getAllSelectedColumns().stream().map(NDataModel.NamedColumn::getId)
                .collect(Collectors.toList());
        if (!allSelectedColumns.containsAll(tableIndexColumns)) {
            val columnNames = tableIndexColumns.stream().filter(x -> !allSelectedColumns.contains(x))
                    .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
            throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getDIMENSION_NOTFOUND(), StringUtils.join(columnNames, ",")));
        }

        //check recommend agg index contains removed columns
        val recommendAggIndex = indexPlan.getIndexes().stream().filter(x -> !x.isTableIndex())
                .collect(Collectors.toList());
        for (val aggIndex : recommendAggIndex) {
            if (!newModel.getEffectiveDimensions().keySet().containsAll(aggIndex.getDimensions())) {
                val allDimensions = aggIndex.getDimensions();
                val dimensionNames = allDimensions.stream()
                        .filter(id -> !newModel.getEffectiveDimensions().containsKey(id))
                        .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
                throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDIMENSION_NOTFOUND(), StringUtils.join(dimensionNames, ",")));
            }

            if (!newModel.getEffectiveMeasures().keySet().containsAll(aggIndex.getMeasures())) {
                val measureNames = aggIndex.getMeasures().stream()
                        .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                        .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getMEASURE_NOTFOUND(), StringUtils.join(measureNames, ",")));

            }
        }
    }

    public void updateBrokenModel(String project, ModelRequest modelRequest, Set<Integer> columnIds) {
        val modelManager = getDataModelManager(project);
        val origin = modelManager.getDataModelDesc(modelRequest.getUuid());
        val copyModel = modelManager.copyForWrite(origin);
        semanticUpdater.updateModelColumns(copyModel, modelRequest);
        copyModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        copyModel.getAllNamedColumns().forEach(namedColumn -> {
            if (columnIds.contains(namedColumn.getId())) {
                namedColumn.setStatus(NDataModel.ColumnStatus.TOMB);
            }
        });
        modelManager.updateDataModelDesc(copyModel);
    }

    public NDataModel repairBrokenModel(String project, ModelRequest modelRequest) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        semanticUpdater.expandModelRequest(modelRequest);
        val modelManager = getDataModelManager(project);
        val origin = modelManager.getDataModelDesc(modelRequest.getId());
        val broken = modelQuerySupporter.getBrokenModel(project, origin.getId());
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);

        if (MaintainModelType.AUTO_MAINTAIN == prj.getMaintainModelType()
                || ManagementType.TABLE_ORIENTED == broken.getManagementType()) {
            throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL, "Can not repair model manually smart mode!");
        }
        broken.setPartitionDesc(modelRequest.getPartitionDesc());
        broken.setFilterCondition(modelRequest.getFilterCondition());
        broken.setJoinTables(modelRequest.getJoinTables());
        discardInvalidColumnAndMeasure(broken, modelRequest);
        broken.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);
        broken.setBrokenReason(NDataModel.BrokenReason.NULL);
        String format = probeDateFormatIfNotExist(project, broken);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager modelMgr = getDataModelManager(project);
            semanticUpdater.updateModelColumns(broken, modelRequest);
            NDataModel model = modelMgr.updateDataModelDesc(broken);
            modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                modelChangeSupporters.forEach(listener -> listener.onUpdateSingle(project, model.getUuid()));
                saveDateFormatIfNotExist(project, model.getUuid(), format);
            });
            getDataflowManager(project).updateDataflowStatus(broken.getId(), RealizationStatusEnum.ONLINE);
            return modelMgr.getDataModelDesc(model.getUuid());
        }, project);
    }

    public void discardInvalidColumnAndMeasure(NDataModel brokenModel, ModelRequest modelRequest) {
        NDataModel newModel = convertAndInitDataModel(modelRequest, brokenModel.getProject());
        Set<String> aliasDotColumns = newModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet());
        for (NDataModel.NamedColumn column : brokenModel.getAllNamedColumns()) {
            if (!aliasDotColumns.contains(column.getAliasDotColumn())) {
                column.setStatus(NDataModel.ColumnStatus.TOMB);
            }
        }
        for (NDataModel.Measure measure : brokenModel.getAllMeasures()) {
            if (measure.isTomb()) {
                continue;
            }
            FunctionDesc function = measure.getFunction();
            for (ParameterDesc param : function.getParameters()) {
                if (!param.isConstant() && !aliasDotColumns.contains(param.getValue())) {
                    measure.setTomb(true);
                    break;
                }
            }
        }
    }

    public NDataModel convertToDataModel(ModelRequest modelDesc) {
        return semanticUpdater.convertToDataModel(modelDesc);
    }

    public AffectedModelsResponse getAffectedModelsByToggleTableType(String tableName, String project) {
        aclEvaluate.checkProjectReadPermission(project);
        val dataflowManager = getDataflowManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        val response = new AffectedModelsResponse();
        val models = dataflowManager.getTableOrientedModelsUsingRootTable(table).stream()
                .map(RootPersistentEntity::getUuid).collect(Collectors.toList());
        var size = 0;
        response.setModels(models);
        for (val model : models) {
            size += dataflowManager.getDataflowStorageSize(model);
        }
        response.setByteSize(size);
        return response;
    }

    public AffectedModelsResponse getAffectedModelsByDeletingTable(String tableName, String project) {
        aclEvaluate.checkProjectReadPermission(project);
        val dataflowManager = getDataflowManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        val response = new AffectedModelsResponse();
        val models = dataflowManager.getModelsUsingTable(table).stream().map(RootPersistentEntity::getUuid)
                .collect(Collectors.toList());
        var size = 0;
        response.setModels(models);
        for (val model : models) {
            size += dataflowManager.getDataflowStorageSize(model);
        }
        response.setByteSize(size);
        return response;
    }

    public void checkSingleIncrementingLoadingTable(String project, String tableName) {
        aclEvaluate.checkProjectReadPermission(project);
        val dataflowManager = getDataflowManager(project);
        val table = getTableManager(project).getTableDesc(tableName);
        val modelsUsingTable = dataflowManager.getModelsUsingTable(table);
        for (val modelDesc : modelsUsingTable) {
            if (!modelDesc.getRootFactTable().getTableDesc().getIdentity().equals(tableName)
                    || modelDesc.isJoinTable(tableName)) {
                Preconditions.checkState(getDataLoadingRangeManager(project).getDataLoadingRange(tableName) == null);
                throw new KylinException(ServerErrorCode.PERMISSION_DENIED, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getINVALID_SET_TABLE_INC_LOADING(), tableName, modelDesc.getAlias()));
            }
        }
    }

    public List<ModelConfigResponse> getModelConfig(String project, String modelName) {
        aclEvaluate.checkProjectReadPermission(project);
        val responseList = Lists.<ModelConfigResponse> newArrayList();
        getDataflowManager(project).listUnderliningDataModels().stream()
                .filter(model -> (StringUtils.isEmpty(modelName) || model.getAlias().contains(modelName)))
                .filter(model -> NDataModelManager.isModelAccessible(model) && !model.fusionModelBatchPart())
                .forEach(dataModel -> {
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
                        MODEL_CONFIG_BLOCK_LIST.forEach(item -> response.getOverrideProps().remove(item));
                    }
                    responseList.add(response);
                });
        return responseList;
    }

    @Transaction(project = 0)
    public void updateModelConfig(String project, String modelId, ModelConfigRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelConfigParameters(request);
        val dataModelManager = getDataModelManager(project);
        dataModelManager.updateDataModel(modelId, copyForWrite -> {
            val segmentConfig = copyForWrite.getSegmentConfig();
            segmentConfig.setAutoMergeEnabled(request.getAutoMergeEnabled());
            segmentConfig.setAutoMergeTimeRanges(request.getAutoMergeTimeRanges());
            segmentConfig.setVolatileRange(request.getVolatileRange());
            segmentConfig.setRetentionRange(request.getRetentionRange());

            copyForWrite.setConfigLastModified(System.currentTimeMillis());
            copyForWrite.setConfigLastModifier(BasicService.getUsername());
        });
        val indexPlan = getIndexPlan(modelId, project);
        val indexPlanManager = getIndexPlanManager(project);
        val overrideProps = request.getOverrideProps();
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            // affected by "kylin.cube.aggrgroup.is-base-cuboid-always-valid" config
            String affectProp = "kylin.cube.aggrgroup.is-base-cuboid-always-valid";
            String oldProp = copyForWrite.getOverrideProps().get(affectProp);

            copyForWrite.setOverrideProps(overrideProps);
            String newProp = copyForWrite.getOverrideProps().get(affectProp);

            boolean affectedByProp = !StringUtils.equals(oldProp, newProp);
            if (affectedByProp && copyForWrite.getRuleBasedIndex() != null) {
                val newRule = JsonUtil.deepCopyQuietly(copyForWrite.getRuleBasedIndex(), RuleBasedIndex.class);
                newRule.setLastModifiedTime(System.currentTimeMillis());
                newRule.setLayoutIdMapping(Lists.newArrayList());
                copyForWrite.setRuleBasedIndex(newRule);
            }
        });
    }

    private void checkPropParameter(ModelConfigRequest request) {
        val props = request.getOverrideProps();
        if (props == null) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_NULL_VALUE(), "override_props"));
        }
        for (val pair : props.entrySet()) {
            if (Objects.isNull(pair.getValue())) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_NULL_VALUE(), pair.getKey()));
            }
        }
        String cores = props.get("kylin.engine.spark-conf.spark.executor.cores");
        String instances = props.get("kylin.engine.spark-conf.spark.executor.instances");
        String pattitions = props.get("kylin.engine.spark-conf.spark.sql.shuffle.partitions");
        String memory = props.get("kylin.engine.spark-conf.spark.executor.memory");
        String baseCuboidAllowed = props.get("kylin.cube.aggrgroup.is-base-cuboid-always-valid");
        if (null != cores && !StringUtil.validateNumber(cores)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.cores"));
        }
        if (null != instances && !StringUtil.validateNumber(instances)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.instances"));
        }
        if (null != pattitions && !StringUtil.validateNumber(pattitions)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.sql.shuffle.partitions"));
        }
        if (null != memory
                && (!memory.endsWith("g") || !StringUtil.validateNumber(memory.substring(0, memory.length() - 1)))) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MEMORY_SIZE(), "spark.executor.memory"));
        }
        if (null != baseCuboidAllowed && !StringUtil.validateBoolean(baseCuboidAllowed)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_BOOLEAN_FORMAT(), "is-base-cuboid-always-valid"));
        }
    }

    @VisibleForTesting
    public void checkModelConfigParameters(ModelConfigRequest request) {
        Boolean autoMergeEnabled = request.getAutoMergeEnabled();
        List<AutoMergeTimeEnum> timeRanges = request.getAutoMergeTimeRanges();
        VolatileRange volatileRange = request.getVolatileRange();
        RetentionRange retentionRange = request.getRetentionRange();

        if (Boolean.TRUE.equals(autoMergeEnabled) && (null == timeRanges || timeRanges.isEmpty())) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    MsgPicker.getMsg().getINVALID_AUTO_MERGE_CONFIG());
        }
        if (null != volatileRange && volatileRange.isVolatileRangeEnabled()
                && (volatileRange.getVolatileRangeNumber() < 0 || null == volatileRange.getVolatileRangeType())) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    MsgPicker.getMsg().getINVALID_VOLATILE_RANGE_CONFIG());
        }
        if (null != retentionRange && retentionRange.isRetentionRangeEnabled()
                && retentionRange.getRetentionRangeNumber() < 0) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    MsgPicker.getMsg().getINVALID_RETENTION_RANGE_CONFIG());
        }
        checkPropParameter(request);
    }

    public ExistedDataRangeResponse getLatestDataRange(String project, String modelId, PartitionDesc desc)
            throws Exception {
        Preconditions.checkNotNull(modelId);
        aclEvaluate.checkProjectReadPermission(project);

        val df = getDataflowManager(project).getDataflow(modelId);
        val model = df.getModel();
        val table = model.getRootFactTableName();
        if (Objects.nonNull(desc) && !desc.equals(model.getPartitionDesc())) {
            Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table, desc);
            return new ExistedDataRangeResponse(pushdownResult.getFirst(), pushdownResult.getSecond());
        }
        Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table,
                model.getPartitionDesc());
        pushdownResult.setFirst(PushDownUtil.calcStart(pushdownResult.getFirst(), df.getCoveredRange()));
        if (Long.parseLong(pushdownResult.getFirst()) > Long.parseLong(pushdownResult.getSecond())) {
            pushdownResult.setSecond(pushdownResult.getFirst());
        }
        return new ExistedDataRangeResponse(pushdownResult.getFirst(), pushdownResult.getSecond());
    }

    public PurgeModelAffectedResponse getPurgeModelAffectedResponse(String project, String model) {
        aclEvaluate.checkProjectWritePermission(project);
        val response = new PurgeModelAffectedResponse();
        val byteSize = getDataflowManager(project).getDataflowStorageSize(model);
        response.setByteSize(byteSize);
        long jobSize = getExecutableManager(project).countByModelAndStatus(model, ExecutableState::isProgressing);
        response.setRelatedJobSize(jobSize);
        return response;
    }

    public ComputedColumnCheckResponse getComputedColumnCheckResponse(ComputedColumnDesc ccDesc,
            List<String> removedMeasures) {
        val response = new ComputedColumnCheckResponse();
        response.setComputedColumnDesc(ccDesc);
        response.setRemovedMeasures(removedMeasures);
        return response;
    }

    public ModelSaveCheckResponse checkBeforeModelSave(ModelRequest modelRequest) {
        aclEvaluate.checkProjectWritePermission(modelRequest.getProject());
        semanticUpdater.expandModelRequest(modelRequest);

        checkModelDimensions(modelRequest);
        checkModelMeasures(modelRequest);
        checkModelJoinConditions(modelRequest);

        validateFusionModelDimension(modelRequest);
        NDataModel model = semanticUpdater.convertToDataModel(modelRequest);

        if (modelRequest.getPartitionDesc() != null
                && !KylinConfig.getInstanceFromEnv().isUseBigIntAsTimestampForPartitionColumn()) {
            PartitionDesc partitionDesc = modelRequest.getPartitionDesc();
            partitionDesc.init(model);
            if (!partitionDesc.checkIntTypeDateFormat()) {
                throw new KylinException(JobErrorCode.JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR,
                        "int/bigint data type only support yyyymm/yyyymmdd format");
            }
        }

        NDataModel oldDataModel = getDataModelManager(model.getProject()).getDataModelDesc(model.getUuid());
        Set<Long> affectedLayouts = Sets.newHashSet();
        if (oldDataModel != null && !oldDataModel.isBroken()) {
            model = getDataModelManager(model.getProject()).copyForWrite(oldDataModel);
            UpdateImpact updateImpact = semanticUpdater.updateModelColumns(model, modelRequest);
            Set<Integer> modifiedSet = updateImpact.getAffectedIds();
            affectedLayouts = getAffectedLayouts(oldDataModel.getProject(), oldDataModel.getId(), modifiedSet);
        }
        try {
            massageModelFilterCondition(model);
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.INVALID_FILTER_CONDITION, e);
        }
        if (!affectedLayouts.isEmpty())
            return new ModelSaveCheckResponse(true);
        return new ModelSaveCheckResponse();
    }

    private void checkCascadeErrorOfNestedCC(ComputedColumnDesc cc, String ccInCheck) {
        String upperCcInCheck = ccInCheck.toUpperCase(Locale.ROOT);
        try {
            SqlVisitor<Void> sqlVisitor = new SqlBasicVisitor<Void>() {
                @Override
                public Void visit(SqlIdentifier id) {
                    if (id.toString().equals(upperCcInCheck))
                        throw new Util.FoundOne(id);
                    return null;
                }
            };
            CalciteParser.getExpNode(cc.getExpression()).accept(sqlVisitor);
        } catch (Util.FoundOne e) {
            throw new KylinException(ServerErrorCode.COMPUTED_COLUMN_CASCADE_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getNESTED_CC_CASCADE_ERROR(), ccInCheck, cc.getFullName()));
        }
    }

    /**
     * massage and update model filter condition
     * 1. expand computed columns
     * 2. add missing identifier quotes
     * 3. add missing table identifiers
     * @param model
     */
    @VisibleForTesting
    public void massageModelFilterCondition(final NDataModel model) {
        if (StringUtils.isEmpty(model.getFilterCondition())) {
            return;
        }

        String massagedFilterCond = KapQueryUtil.massageExpression(model, model.getProject(),
                model.getFilterCondition(),
                AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(), getCurrentUserGroups()), false);

        String filterConditionWithTableName = addTableNameIfNotExist(massagedFilterCond, model);

        model.setFilterCondition(filterConditionWithTableName);
    }

    @VisibleForTesting
    String addTableNameIfNotExist(final String expr, final NDataModel model) {
        Map<String, String> colToTable = Maps.newHashMap();
        Set<String> ambiguityCol = Sets.newHashSet();
        Set<String> allColumn = Sets.newHashSet();
        for (val col : model.getAllNamedColumns()) {
            if (col.getStatus() == NDataModel.ColumnStatus.TOMB) {
                continue;
            }
            String aliasDotColumn = col.getAliasDotColumn();
            allColumn.add(aliasDotColumn);
            String table = aliasDotColumn.split("\\.")[0];
            String column = aliasDotColumn.split("\\.")[1];
            if (colToTable.containsKey(column)) {
                ambiguityCol.add(column);
            } else {
                colToTable.put(column, table);
            }
        }

        SqlNode sqlNode = CalciteParser.getExpNode(expr);

        SqlVisitor<Object> sqlVisitor = new AddTableNameSqlVisitor(expr, colToTable, ambiguityCol, allColumn);

        sqlNode.accept(sqlVisitor);

        if (!KylinConfig.getInstanceFromEnv().isBuildExcludedTableEnabled()) {
            Set<String> excludedTables = getFavoriteRuleManager(model.getProject()).getExcludedTables();
            ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, model.getJoinTables(), model);
            String antiFlattenLookup = checker.detectFilterConditionDependsLookups(sqlNode.toString(),
                    checker.getExcludedLookups());
            if (antiFlattenLookup != null) {
                throw new KylinException(ServerErrorCode.FILTER_CONDITION_DEPENDS_ANTI_FLATTEN_LOOKUP,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getFILTER_CONDITION_ON_ANTI_FLATTEN_LOOKUP(),
                                antiFlattenLookup));
            }
        }
        return sqlNode
                .toSqlString(new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE)), true)
                .toString();
    }

    public NModelDescResponse getModelDesc(String modelAlias, String project) {
        if (getProjectManager().getProject(project) == null) {
            throw new KylinException(ServerErrorCode.PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel dataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        NDataModelResponse model = new NDataModelResponse(dataModel);
        NModelDescResponse response = new NModelDescResponse();
        response.setUuid(model.getUuid());
        response.setLastModified(model.getLastModified());
        response.setCreateTime(model.getCreateTime());
        response.setVersion(model.getVersion());
        response.setName(model.getAlias());
        response.setProject(model.getProject());
        response.setDescription(model.getDescription());
        response.setComputedColumnDescs(model.getComputedColumnDescs());
        response.setFactTable(model.getRootFactTableName());
        response.setJoinTables(model.getJoinTables());

        response.setMeasures(model.getMeasures());
        List<NModelDescResponse.Dimension> dims = model.getNamedColumns().stream() //
                .map(namedColumn -> new NModelDescResponse.Dimension(namedColumn, false)) //
                .collect(Collectors.toList());
        response.setDimensions(dims);

        IndexPlan indexPlan = getIndexPlan(response.getUuid(), project);
        if (indexPlan.getRuleBasedIndex() != null) {
            List<AggGroupResponse> aggGroupResponses = indexPlan.getRuleBasedIndex().getAggregationGroups().stream() //
                    .map(x -> new AggGroupResponse(dataModel, x)) //
                    .collect(Collectors.toList());
            response.setAggregationGroups(aggGroupResponses);
        } else {
            response.setAggregationGroups(new ArrayList<>());
        }
        return response;
    }

    @Transaction(project = 0)
    public void updateDataModelParatitionDesc(String project, String modelAlias,
            ModelParatitionDescRequest modelParatitionDescRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        if (getProjectManager().getProject(project) == null) {
            throw new KylinException(ServerErrorCode.PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel oldDataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (oldDataModel == null) {
            throw new KylinException(ServerErrorCode.INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        checkModelPermission(project, oldDataModel.getId());

        PartitionDesc partitionDesc = modelParatitionDescRequest.getPartitionDesc();
        if (partitionDesc != null) {
            String rootFactTable = oldDataModel.getRootFactTableName().split("\\.")[1];
            if (!partitionDesc.getPartitionDateColumn().toUpperCase(Locale.ROOT).startsWith(rootFactTable + ".")) {
                throw new KylinException(ServerErrorCode.INVALID_PARTITION_COLUMN,
                        MsgPicker.getMsg().getINVALID_PARTITION_COLUMN());
            }
        }

        getDataModelManager(project).updateDataModel(oldDataModel.getUuid(),
                copyForWrite -> copyForWrite.setPartitionDesc(modelParatitionDescRequest.getPartitionDesc()));
        semanticUpdater.handleSemanticUpdate(project, oldDataModel.getUuid(), oldDataModel,
                modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd(), false);
    }

    @Transaction(project = 0)
    public void updateModelOwner(String project, String modelId, OwnerChangeRequest ownerChangeRequest) {
        try {
            aclEvaluate.checkProjectAdminPermission(project);
            checkTargetOwnerPermission(project, modelId, ownerChangeRequest.getOwner());
        } catch (AccessDeniedException e) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED,
                    MsgPicker.getMsg().getMODEL_CHANGE_PERMISSION());
        } catch (IOException e) {
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED, MsgPicker.getMsg().getOWNER_CHANGE_ERROR());
        }

        getDataModelManager(project).updateDataModel(modelId,
                copyForWrite -> copyForWrite.setOwner(ownerChangeRequest.getOwner()));
    }

    private void checkTargetOwnerPermission(String project, String modelId, String owner) throws IOException {
        Set<String> projectManagementUsers = accessService.getProjectManagementUsers(project);
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (Objects.isNull(model) || model.isBroken()) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    "Model " + modelId + " does not exist or broken in project " + project);
        }
        projectManagementUsers.remove(model.getOwner());

        if (CollectionUtils.isEmpty(projectManagementUsers) || !projectManagementUsers.contains(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(ServerErrorCode.PERMISSION_DENIED, msg.getMODEL_OWNER_CHANGE_INVALID_USER());
        }
    }

    @Transaction(project = 0)
    public void updateMultiPartitionMapping(String project, String modelId, MultiPartitionMappingRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        val model = checkModelIsMLP(modelId, project);
        checkModelPermission(project, modelId);
        val multiPartitionDesc = model.getMultiPartitionDesc();
        val msg = MsgPicker.getMsg();
        if (CollectionUtils.isNotEmpty(request.getPartitionCols())) {
            // check size
            Preconditions.checkArgument(request.getAliasCols().size() == multiPartitionDesc.getColumns().size(),
                    new KylinException(ServerErrorCode.INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID()));

            for (int i = 0; i < request.getPartitionCols().size(); i++) {
                val partitionCol = request.getPartitionCols().get(i);
                val columnRef = model.findColumn(partitionCol);
                if (!columnRef.equals(multiPartitionDesc.getColumnRefs().get(i))) {
                    throw new KylinException(ServerErrorCode.INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID());
                }
            }

            val partitionValues = request.getValueMapping().stream()
                    .map(pair -> pair.getOrigin().toArray(new String[0])).collect(Collectors.toList());
            for (MultiPartitionDesc.PartitionInfo partitionInfo : multiPartitionDesc.getPartitions()) {
                if (partitionValues.stream().noneMatch(value -> Arrays.equals(value, partitionInfo.getValues()))) {
                    throw new KylinException(ServerErrorCode.INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID());
                }
            }
        }

        getDataModelManager(project).updateDataModel(modelId,
                copyForWrite -> copyForWrite.setMultiPartitionKeyMapping(request.convertToMultiPartitionMapping()));
    }

    private void changeModelOwner(NDataModel model) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.nonNull(auth) && Objects.nonNull(auth.getPrincipal())) {
            if (auth.getPrincipal() instanceof UserDetails) {
                model.setOwner(((UserDetails) auth.getPrincipal()).getUsername());
            } else {
                model.setOwner(auth.getPrincipal().toString());
            }
        }
    }

    public List<NDataModel> updateReponseAcl(List<NDataModel> models, String project) {
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.isAdmin() || AclPermissionUtil.isAdminInProject(project, groups)) {
            models.forEach(model -> {
                NDataModelAclParams aclParams = new NDataModelAclParams();
                aclParams.setUnauthorizedTables(Sets.newHashSet());
                aclParams.setUnauthorizedColumns(Sets.newHashSet());
                if (model instanceof NDataModelResponse) {
                    ((NDataModelResponse) model).setAclParams(aclParams);
                } else if (model instanceof RelatedModelResponse) {
                    ((RelatedModelResponse) model).setAclParams(aclParams);
                }
            });
            return models;
        }
        String username = AclPermissionUtil.getCurrentUsername();
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        var auths = getAclTCRManager(project).getAuthTablesAndColumns(project, username, true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());

        for (val group : groups) {
            auths = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }
        List<NDataModel> normalModel = new ArrayList<>();
        List<NDataModel> noVisibleModel = new ArrayList<>();
        models.forEach(model -> {
            Set<String> tablesNoAcl = Sets.newHashSet();
            Set<String> columnsNoAcl = Sets.newHashSet();
            Set<String> tables = Sets.newHashSet();
            model.getJoinTables().forEach(table -> tables.add(table.getTable()));
            tables.add(model.getRootFactTableName());
            tables.stream().filter(table -> !allAuthTables.contains(table)).forEach(tablesNoAcl::add);
            tables.stream().filter(allAuthTables::contains).forEach(table -> {
                ColumnDesc[] columnDescs = NTableMetadataManager.getInstance(getConfig(), project).getTableDesc(table)
                        .getColumns();
                Arrays.stream(columnDescs).map(column -> table + "." + column.getName())
                        .filter(column -> !allAuthColumns.contains(column)).forEach(columnsNoAcl::add);
            });
            NDataModelAclParams aclParams = new NDataModelAclParams();
            aclParams.setUnauthorizedTables(tablesNoAcl);
            aclParams.setUnauthorizedColumns(columnsNoAcl);
            if (model instanceof NDataModelResponse) {
                ((NDataModelResponse) model).setAclParams(aclParams);
            } else if (model instanceof RelatedModelResponse) {
                ((RelatedModelResponse) model).setAclParams(aclParams);
            }
            (aclParams.isVisible() ? normalModel : noVisibleModel).add(model);
        });
        List<NDataModel> result = new ArrayList<>(normalModel);
        result.addAll(noVisibleModel);
        return result;
    }

    public NDataModel updateReponseAcl(NDataModel model, String project) {
        List<NDataModel> models = updateReponseAcl(Arrays.asList(model), project);
        return models.get(0);
    }

    public void checkDuplicateAliasInModelRequests(Collection<ModelRequest> modelRequests) {
        Map<String, List<ModelRequest>> aliasModelRequestMap = modelRequests.stream().collect(
                groupingBy(modelRequest -> modelRequest.getAlias().toLowerCase(Locale.ROOT), Collectors.toList()));
        aliasModelRequestMap.forEach((alias, requests) -> {
            if (requests.size() > 1) {
                throw new KylinException(ServerErrorCode.INVALID_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_ALIAS_DUPLICATED(),
                                requests.stream().map(ModelRequest::getAlias).collect(Collectors.joining(", "))));
            }
        });
    }

    /**
     * Validate computed column type and throw errors to report wrongly typed computed columns.
     * Models migrated from 3x may have wrongly typed computed columns. see KE-11862
     * @param modelId
     * @param project
     */
    public void validateCCType(String modelId, String project) {
        StringBuilder errorSb = new StringBuilder();
        NDataModel model = getDataModelManager(project).getDataModelDesc(modelId);
        try {
            List<ComputedColumnDesc> ccListCopy = Lists.newArrayList();
            for (ComputedColumnDesc computedColumnDesc : model.getComputedColumnDescs()) {
                // evaluate on a computedcolumn copy to avoid changing the metadata
                ccListCopy.add(JsonUtil.deepCopy(computedColumnDesc, ComputedColumnDesc.class));
            }
            ComputedColumnEvalUtil.evalDataTypeOfCCInBatch(model, ccListCopy);
            for (int n = 0; n < ccListCopy.size(); n++) {
                ComputedColumnDesc cc = model.getComputedColumnDescs().get(n);
                ComputedColumnDesc ccCopy = ccListCopy.get(n);

                if (!matchCCDataType(cc.getDatatype(), ccCopy.getDatatype())) {
                    errorSb.append(new MessageFormat(MsgPicker.getMsg().getCheckCCType(), Locale.ROOT)
                            .format(new String[] { cc.getFullName(), ccCopy.getDatatype(), cc.getDatatype() }));
                }
            }

        } catch (Exception e) {
            logger.error("Error validating computed column ", e);
        }

        if (errorSb.length() > 0) {
            throw new KylinException(ServerErrorCode.INVALID_COMPUTED_COLUMN_EXPRESSION, errorSb.toString());
        }
    }

    private static final Set<String> StringTypes = Sets.newHashSet("STRING", "CHAR", "VARCHAR");

    private boolean matchCCDataType(String actual, String expected) {
        if (actual == null) {
            return false;
        }

        actual = actual.toUpperCase(Locale.ROOT);
        expected = expected.toUpperCase(Locale.ROOT);
        // char, varchar, string are of the same
        if (StringTypes.contains(actual)) {
            return StringTypes.contains(expected);
        }

        // if actual type is of decimal type with no prec, scala
        // match it with any decimal types
        if (actual.equals("DECIMAL")) {
            return expected.startsWith("DECIMAL");
        }

        return actual.equalsIgnoreCase(expected);
    }

    public CheckSegmentResponse checkSegments(String project, String modelAlias, String start, String end) {
        NDataModel model = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (model == null) {
            throw new KylinException(ServerErrorCode.MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        if (model.isBroken()) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    "Failed to get segment information as " + modelAlias + " is broken. Please fix it and try again.");
        }
        Segments<NDataSegment> segments = getSegmentsByRange(model.getUuid(), project, start, end);
        CheckSegmentResponse checkSegmentResponse = new CheckSegmentResponse();
        segments.forEach(seg -> checkSegmentResponse.getSegmentsOverlap()
                .add(new CheckSegmentResponse.SegmentInfo(seg.getId(), seg.getName())));
        return checkSegmentResponse;
    }

    public String getPartitionColumnFormatById(String project, String modelId) {
        NDataModel dataModelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (dataModelDesc.isBroken())
            return null;
        return dataModelDesc.getPartitionDesc() == null ? null
                : dataModelDesc.getPartitionDesc().getPartitionDateFormat();
    }

    public String getPartitionColumnFormatByAlias(String project, String modelAlias) {
        NDataModel dataModelDesc = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (dataModelDesc.isBroken())
            return null;
        return dataModelDesc.getPartitionDesc() == null ? null
                : dataModelDesc.getPartitionDesc().getPartitionDateFormat();
    }

    public BISyncModel biExportCustomModel(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port) {
        Set<String> groups = getCurrentUserGroups();
        return exportCustomModel(projectName, modelId, targetBI, modelElement, host, port, groups);
    }

    public BISyncModel exportModel(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port) {
        NDataflow dataflow = getDataflowManager(projectName).getDataflow(modelId);
        if (dataflow.getStatus() == RealizationStatusEnum.BROKEN) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    "The model is broken and cannot be exported TDS file");
        }
        checkModelExportPermission(projectName, modelId);
        checkModelPermission(projectName, modelId);

        SyncContext syncContext = getSyncContext(projectName, modelId, targetBI, modelElement, host, port);

        return BISyncTool.dumpToBISyncModel(syncContext);
    }

    public BISyncModel exportCustomModel(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port, Set<String> groups) {
        String currentUserName = aclEvaluate.getCurrentUserName();
        NDataflow dataflow = getDataflowManager(projectName).getDataflow(modelId);
        if (dataflow.getStatus() == RealizationStatusEnum.BROKEN) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    "The model is broken and cannot be exported TDS file");
        }
        Set<String> authTables = getAllAuthTables(projectName, groups, currentUserName);
        Set<String> authColumns = getAllAuthColumns(projectName, groups, currentUserName);
        checkTableHasColumnPermission(projectName, modelId, authColumns);

        SyncContext syncContext = getSyncContext(projectName, modelId, targetBI, modelElement, host, port);

        return BISyncTool.dumpHasPermissionToBISyncModel(syncContext, authTables, authColumns);
    }

    private SyncContext getSyncContext(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port) {
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setTargetBI(targetBI);
        syncContext.setModelElement(modelElement);
        syncContext.setHost(host);
        syncContext.setPort(port);
        syncContext.setDataflow(getDataflowManager(projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getProjectManager().getProject(projectName).getConfig());
        return syncContext;
    }

    public void checkTableHasColumnPermission(String project, String modeId, Set<String> authColumns) {
        if (AclPermissionUtil.isAdmin()) {
            return;
        }
        aclEvaluate.checkProjectReadPermission(project);

        NDataModel model = getDataModelManager(project).getDataModelDesc(modeId);
        long jointCount = model.getJoinTables().stream()
                .filter(table -> authColumns
                        .containsAll(Arrays.stream(table.getJoin().getPrimaryKeyColumns())
                                .map(TblColRef::getCanonicalName).collect(Collectors.toSet()))
                        && authColumns.containsAll(Arrays.stream(table.getJoin().getForeignKeyColumns())
                                .map(TblColRef::getCanonicalName).collect(Collectors.toSet())))
                .count();
        long singleTableCount = model.getAllTables().stream().filter(ref -> ref.getColumns().stream()
                .map(TblColRef::getCanonicalName).collect(Collectors.toSet()).stream().anyMatch(authColumns::contains))
                .count();

        if (jointCount != model.getJoinTables().size() || singleTableCount == 0) {
            throw new KylinException(ServerErrorCode.INVALID_TABLE_AUTH,
                    MsgPicker.getMsg().getTABLE_NO_COLUMNS_PERMISSION());
        }

    }

    private Set<String> getAllAuthTables(String project, Set<String> groups, String user) {
        Set<String> allAuthTables = Sets.newHashSet();
        AclTCRDigest auths = getAclTCRManager(project).getAuthTablesAndColumns(project, user, true);
        allAuthTables.addAll(auths.getTables());
        for (String group : groups) {
            auths = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
        }
        return allAuthTables;
    }

    private Set<String> getAllAuthColumns(String project, Set<String> groups, String user) {
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRDigest auths = getAclTCRManager(project).getAuthTablesAndColumns(project, user, true);
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allAuthColumns.addAll(auths.getColumns());
        }
        return allAuthColumns;
    }

    private void checkModelExportPermission(String project, String modeId) {
        if (AclPermissionUtil.isAdmin()) {
            return;
        }
        aclEvaluate.checkProjectReadPermission(project);

        NDataModel model = getDataModelManager(project).getDataModelDesc(modeId);
        Map<String, Set<String>> modelTableColumns = new HashMap<>();
        for (TableRef tableRef : model.getAllTables()) {
            modelTableColumns.putIfAbsent(tableRef.getTableIdentity(), new HashSet<>());
            modelTableColumns.get(tableRef.getTableIdentity())
                    .addAll(tableRef.getColumns().stream().map(TblColRef::getName).collect(Collectors.toSet()));
        }
        AclTCRManager aclManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        String currentUserName = AclPermissionUtil.getCurrentUsername();
        Set<String> groupsOfExecuteUser = accessService.getGroupsOfExecuteUser(currentUserName);
        MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
        Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfExecuteUser, acl);
        AclTCRDigest digest = aclManager.getAllUnauthorizedTableColumn(currentUserName, groupsInProject,
                modelTableColumns);
        if (digest.getColumns() != null && !digest.getColumns().isEmpty()) {
            throw new KylinException(ServerErrorCode.UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
        if (digest.getTables() != null && !digest.getTables().isEmpty()) {
            throw new KylinException(ServerErrorCode.UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
    }

    public List<SegmentPartitionResponse> getSegmentPartitions(String project, String modelId, String segmentId,
            List<String> status, String sortBy, boolean reverse) {
        aclEvaluate.checkProjectReadPermission(project);
        checkModelPermission(project, modelId);
        val model = getModelById(modelId, project);
        val partitionDesc = model.getMultiPartitionDesc();
        val dataflow = getDataflowManager(project).getDataflow(modelId);
        val segment = dataflow.getSegment(segmentId);
        if (segment == null) {
            throw new KylinException(ServerErrorCode.SEGMENT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_ID_NOT_EXIST(), segmentId));
        }

        Comparator<SegmentPartitionResponse> comparator = BasicService
                .propertyComparator(StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy, !reverse);

        List<SegmentPartitionResponse> responses = segment.getMultiPartitions().stream() //
                .map(partition -> {
                    val partitionInfo = partitionDesc.getPartitionInfo(partition.getPartitionId());
                    val lastModifiedTime = partition.getLastBuildTime() != 0 ? partition.getLastBuildTime()
                            : partition.getCreateTimeUTC();
                    return new SegmentPartitionResponse(partitionInfo.getId(), partitionInfo.getValues(),
                            partition.getStatus(), lastModifiedTime, partition.getSourceCount(),
                            partition.getStorageSize());
                })
                .filter(partitionResponse -> CollectionUtils.isEmpty(status)
                        || status.contains(partitionResponse.getStatus().name()))
                .sorted(comparator).collect(Collectors.toList());

        return responses;
    }

    @Transaction(project = 0)
    public void deletePartitionsByValues(String project, String segmentId, String modelId,
            List<String[]> subPartitionValues) {
        NDataModel model = checkModelIsMLP(modelId, project);
        checkPartitionValues(model, subPartitionValues);
        Set<Long> ids = model.getMultiPartitionDesc().getPartitionIdsByValues(subPartitionValues);
        deletePartitions(project, segmentId, model.getId(), ids);
    }

    private void checkPartitionValues(NDataModel model, List<String[]> subPartitionValues) {
        List<String[]> modelPartitionValues = model.getMultiPartitionDesc().getPartitions().stream()
                .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
        List<String[]> absentValues = MultiPartitionUtil.findAbsentValues(modelPartitionValues, subPartitionValues);
        if (!absentValues.isEmpty()) {
            throw new KylinException(ServerErrorCode.INVALID_PARTITION_VALUES, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_PARTITION_VALUE(),
                    absentValues.stream().map(arr -> String.join(", ", arr)).collect(Collectors.joining(", "))));
        }
    }

    @Transaction(project = 0)
    public void deletePartitions(String project, String segmentId, String modelId, Set<Long> partitions) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkModelPermission(project, modelId);
        checkSegmentsExistById(modelId, project, new String[] { segmentId });
        checkModelIsMLP(modelId, project);
        if (CollectionUtils.isEmpty(partitions)) {
            return;
        }
        if (StringUtils.isNotEmpty(segmentId)) {
            // remove partition in target segment
            getDataflowManager(project).removeLayoutPartition(modelId, partitions, Sets.newHashSet(segmentId));
            // remove partition in target segment
            getDataflowManager(project).removeSegmentPartition(modelId, partitions, Sets.newHashSet(segmentId));
        } else {
            // remove partition in all layouts
            getDataflowManager(project).removeLayoutPartition(modelId, Sets.newHashSet(partitions), null);
            // remove partition in all  segments
            getDataflowManager(project).removeSegmentPartition(modelId, Sets.newHashSet(partitions), null);
            // remove partition in model
            getDataModelManager(project).updateDataModel(modelId, copyForWrite -> {
                val multiPartitionDesc = copyForWrite.getMultiPartitionDesc();
                multiPartitionDesc.removePartitionValue(Lists.newArrayList(partitions));
            });
        }
    }

    public NDataModel checkModelIsMLP(String modelId, String project) {
        NDataModel model = getModelById(modelId, project);
        if (!model.isMultiPartitionModel()) {
            throw new KylinException(ServerErrorCode.INVALID_MODEL_TYPE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_IS_NOT_MLP(), model.getAlias()));
        }
        return model;
    }

    public void checkModelPermission(String project, String modelId) {
        String userName = aclEvaluate.getCurrentUserName();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.isAdmin() || AclPermissionUtil.isAdminInProject(project, groups)) {
            return;
        }
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        var auths = getAclTCRManager(project).getAuthTablesAndColumns(project, userName, true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (val group : groups) {
            auths = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }

        NDataModel model = getModelById(modelId, project);
        Set<String> tablesInModel = Sets.newHashSet();
        model.getJoinTables().forEach(table -> tablesInModel.add(table.getTable()));
        tablesInModel.add(model.getRootFactTableName());
        tablesInModel.forEach(table -> {
            if (!allAuthTables.contains(table)) {
                throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL,
                        MsgPicker.getMsg().getMODEL_MODIFY_ABANDON(table));
            }
        });
        tablesInModel.stream().filter(allAuthTables::contains).forEach(table -> {
            ColumnDesc[] columnDescs = NTableMetadataManager.getInstance(getConfig(), project).getTableDesc(table)
                    .getColumns();
            Arrays.stream(columnDescs).map(column -> table + "." + column.getName()).forEach(column -> {
                if (!allAuthColumns.contains(column)) {
                    throw new KylinException(ServerErrorCode.FAILED_UPDATE_MODEL,
                            MsgPicker.getMsg().getMODEL_MODIFY_ABANDON(column));
                }
            });
        });
    }

    public InvalidIndexesResponse detectInvalidIndexes(ModelRequest request) {
        String project = request.getProject();
        aclEvaluate.checkProjectReadPermission(project);

        request.setPartitionDesc(null);
        NDataModel model = convertAndInitDataModel(request, project);

        String uuid = model.getUuid();
        List<JoinTableDesc> joinTables = model.getJoinTables();
        IndexPlan indexPlan = getIndexPlanManager(project).getIndexPlan(uuid);
        Set<String> excludedTables = getFavoriteRuleManager(project).getExcludedTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, joinTables, model);
        List<ComputedColumnDesc> invalidCCList = checker.getInvalidComputedColumns(model);
        Set<Integer> invalidDimensions = checker.getInvalidDimensions(model);
        Set<Integer> invalidMeasures = checker.getInvalidMeasures(model);
        Set<Integer> invalidScope = Sets.newHashSet();
        invalidScope.addAll(invalidDimensions);
        invalidScope.addAll(invalidMeasures);
        Set<Long> invalidIndexes = checker.getInvalidIndexes(indexPlan, invalidScope);
        AtomicInteger aggIndexCount = new AtomicInteger();
        AtomicInteger tableIndexCount = new AtomicInteger();
        invalidIndexes.forEach(layoutId -> {
            if (layoutId > IndexEntity.TABLE_INDEX_START_ID) {
                tableIndexCount.getAndIncrement();
            } else {
                aggIndexCount.getAndIncrement();
            }
        });
        List<String> antiFlattenLookupTables = checker.getAntiFlattenLookups();

        List<String> invalidDimensionNames = model.getAllNamedColumns().stream()
                .filter(col -> invalidDimensions.contains(col.getId())).map(NDataModel.NamedColumn::getAliasDotColumn)
                .collect(Collectors.toList());
        List<String> invalidMeasureNames = model.getAllMeasures().stream()
                .filter(measure -> invalidMeasures.contains(measure.getId())).map(MeasureDesc::getName)
                .collect(Collectors.toList());

        InvalidIndexesResponse response = new InvalidIndexesResponse();
        response.setCcList(invalidCCList);
        response.setDimensions(invalidDimensionNames);
        response.setMeasures(invalidMeasureNames);
        response.setIndexes(Lists.newArrayList(invalidIndexes));
        response.setInvalidAggIndexCount(aggIndexCount.get());
        response.setInvalidTableIndexCount(tableIndexCount.get());
        response.setAntiFlattenLookups(antiFlattenLookupTables);
        return response;
    }

    private NDataModel convertAndInitDataModel(ModelRequest request, String project) {
        NDataModel model = convertToDataModel(request);
        Map<String, TableDesc> allTables = getTableManager(project).getAllTablesMap();
        Map<String, TableDesc> initialAllTables = model.getExtendedTables(allTables);
        model.init(KylinConfig.getInstanceFromEnv(), initialAllTables,
                getDataflowManager(project).listUnderliningDataModels(), project);
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            String innerExp = cc.getInnerExpression();
            if (cc.getExpression().equalsIgnoreCase(innerExp)) {
                innerExp = KapQueryUtil.massageComputedColumn(model, project, cc, null);
            }
            cc.setInnerExpression(innerExp);
        }
        return model;
    }

    private void setRequest(ModelRequest request, NDataModel model, AffectedModelContext removeAffectedModel,
            AffectedModelContext changeTypeAffectedModel, String projectName) {
        request.setSimplifiedMeasures(model.getEffectiveMeasures().values().stream()
                .filter(m -> !removeAffectedModel.getMeasures().contains(m.getId())
                        && !changeTypeAffectedModel.getMeasures().contains(m.getId()))
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));

        request.setSimplifiedMeasures(request.getSimplifiedMeasures().stream().map(simplifiedMeasure -> {
            int measureId = simplifiedMeasure.getId();
            NDataModel.Measure updateMeasure = changeTypeAffectedModel.getUpdateMeasureMap().get(measureId);
            if (updateMeasure != null) {
                simplifiedMeasure = SimplifiedMeasure.fromMeasure(updateMeasure);
            }
            return simplifiedMeasure;
        }).collect(Collectors.toList()));

        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(col -> !removeAffectedModel.getDimensions().contains(col.getId()) && col.isDimension())
                .collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs().stream()
                .filter(cc -> !removeAffectedModel.getComputedColumns().contains(cc.getColumnName()))
                .collect(Collectors.toList()));

        // clean
        request.getAllNamedColumns().stream() //
                .filter(col -> removeAffectedModel.getColumns().contains(col.getId())) //
                .forEach(col -> col.setStatus(NDataModel.ColumnStatus.TOMB));
        String factTableAlias = model.getRootFactTable().getAlias();
        Set<String> ccFullNameSet = removeAffectedModel.getComputedColumns().stream() //
                .map(ccName -> factTableAlias + "." + ccName) //
                .collect(Collectors.toSet());
        request.getAllNamedColumns().stream() //
                .filter(col -> ccFullNameSet.contains(col.getAliasDotColumn())) //
                .forEach(col -> col.setStatus(NDataModel.ColumnStatus.TOMB));

        if (SecondStorageUtil.isModelEnable(model.getProject(), model.getId())) {
            request.setWithSecondStorage(true);
        }

        request.setProject(projectName);
    }

    @Override
    public void onUpdateBrokenModel(NDataModel model, AffectedModelContext removeAffectedModel,
            AffectedModelContext changeTypeAffectedModel, String projectName) throws Exception {
        val request = new ModelRequest(JsonUtil.deepCopy(model, NDataModel.class));
        setRequest(request, model, removeAffectedModel, changeTypeAffectedModel, projectName);
        updateBrokenModel(projectName, request, removeAffectedModel.getColumns());
    }

    @Override
    public void onUpdateDataModel(NDataModel model, AffectedModelContext removeAffectedModel,
            AffectedModelContext changeTypeAffectedModel, String projectName, TableDesc tableDesc) throws Exception {
        val request = new ModelRequest(JsonUtil.deepCopy(model, NDataModel.class));
        setRequest(request, model, removeAffectedModel, changeTypeAffectedModel, projectName);
        request.setColumnsFetcher((tableRef, isFilterCC) -> TableRef.filterColumns(
                tableRef.getIdentity().equals(tableDesc.getIdentity()) ? tableDesc : tableRef, isFilterCC));
        updateDataModelSemantic(projectName, request, true, false);
    }

    @Override
    public NDataModel onGetModelByAlias(String modelAlias, String project) {
        return getModelByAlias(modelAlias, project);
    }

    @Override
    public NDataModel onGetModelById(String modelId, String project) {
        return getModelById(modelId, project);
    }

    @Override
    public void onSyncPartition(String model, String project) {
        syncPartitionDesc(model, project);
    }

    @Override
    public void onPurgeModel(String modelId, String project) {
        purgeModel(modelId, project);
    }

    @Override
    public void onCheckLoadingRange(String project, String tableName) {
        checkSingleIncrementingLoadingTable(project, tableName);
    }

    @Override
    public void onModelUpdate(String project, String modelId) {
        modelChangeSupporters.forEach(listener -> listener.onUpdate(project, modelId));
    }

    @Override
    public void onUpdateSCD2ModelStatus(String project, RealizationStatusEnum status) {
        updateSCD2ModelStatusInProjectById(project, (ModelStatusToDisplayEnum.convert(status)));
    }

    @Override
    public void onOfflineMultiPartitionModels(String project) {
        offlineMultiPartitionModelsInProject(project);
    }

}
