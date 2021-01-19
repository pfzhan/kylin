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
import static org.apache.kylin.common.exception.ServerErrorCode.COMPUTED_COLUMN_CASCADE_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.CONCURRENT_SUBMIT_JOB_LIMIT;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_COMPUTED_COLUMN_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_DIMENSION_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_JOIN_CONDITION;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_MEASURE_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_MEASURE_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SEGMENT_RANGE;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_EXECUTE_MODEL_SQL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_MERGE_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_REFRESH_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_COMPUTED_COLUMN_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_FILTER_CONDITION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_TYPE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MULTI_PARTITION_MAPPING_REQUEST;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_SEGMENT_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_SEGMENT_RANGE;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_BROKEN;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_ONLINE_ABANDON;
import static org.apache.kylin.common.exception.ServerErrorCode.PARTITION_VALUE_NOT_SUPPORT;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SEGMENT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SEGMENT_RANGE_OVERLAP;
import static org.apache.kylin.common.exception.ServerErrorCode.SQL_NUMBER_EXCEEDS_LIMIT;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.UNAUTHORIZED_ENTITY;
import static org.apache.kylin.job.execution.JobTypeEnum.INDEX_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SUB_PARTITION_BUILD;

import java.io.IOException;
import java.math.BigDecimal;
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
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
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
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.adhocquery.PushDownConverterKeyWords;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
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
import io.kyligence.kap.metadata.cube.model.SegmentPartition;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelDropAddListener;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggGroupResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.ComputedColumnCheckResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.ModelSuggestionResponse;
import io.kyligence.kap.rest.response.ModelSuggestionResponse.NRecommendedModelResponse;
import io.kyligence.kap.rest.response.MultiPartitionValueResponse;
import io.kyligence.kap.rest.response.NCubeDescResponse;
import io.kyligence.kap.rest.response.NDataModelOldParams;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.OpenModelSuggestionResponse;
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
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.rest.util.ModelUtils;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractContext.ModelContext;
import io.kyligence.kap.smart.ModelCreateContextOfSemiV2;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.ModelSelectContextOfSemiV2;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.BISyncTool;
import io.kyligence.kap.tool.bisync.SyncContext;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final String LAST_MODIFY = "last_modify";
    private static final String REC_COUNT = "recommendations_count";

    public static final String VALID_NAME_FOR_MODEL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";

    public static final String VALID_NAME_FOR_DIMENSION_MEASURE = "^[\\u4E00-\\u9FA5a-zA-Z0-9 _\\-()%?（）]+$";

    private static final List<String> MODEL_CONFIG_BLOCK_LIST = Lists.newArrayList("kylin.index.rule-scheduler-data");

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpdater;

    @Setter
    @Autowired
    private SegmentHelper segmentHelper;

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private AccessService accessService;

    @Setter
    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    private RawRecService rawRecService;

    @Autowired
    private AclTCRService aclTCRService;

    @Setter
    @Autowired
    private List<ModelUpdateListener> updateListeners = Lists.newArrayList();

    public NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }
        return nDataModel;
    }

    public NDataModel getModelByAlias(String modelAlias, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (null == nDataModel) {
            throw new KylinException(MODEL_NOT_EXIST,
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

            long inputRecordCnt = 0L;
            long inputRecordSizeBytes = 0L;

            if (!model.isBroken()) {
                List<NDataSegmentResponse> segments = getSegmentsResponse(model.getId(), model.getProject(), "1",
                        String.valueOf(Long.MAX_VALUE - 1), null, executables, LAST_MODIFY, true);
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
                if (model instanceof NDataModelResponse) {
                    ((NDataModelResponse) model).setSegments(segments);
                }
            }

            if (model instanceof NDataModelResponse) {
                oldParams.setProjectName(model.getProject());
                oldParams.setSizeKB(((NDataModelResponse) model).getStorage() / 1024);
                oldParams.setInputRecordSizeBytes(inputRecordSizeBytes);
                oldParams.setInputRecordCnt(inputRecordCnt);
                ((NDataModelResponse) model).setOldParams(oldParams);
            } else if (model instanceof RelatedModelResponse) {
                ((RelatedModelResponse) model).setOldParams(oldParams);
            }
        });

        return modelList;
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
        cube.getJoinTables().stream().forEach(join -> {
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
            throw new KylinException(MODEL_NOT_EXIST,
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

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, List<String> status, String sortBy, boolean reverse, String modelAliasOrOwner,
            Long lastModifyFrom, Long lastModifyTo) {
        aclEvaluate.checkProjectReadPermission(projectName);
        List<Pair<NDataflow, NDataModel>> pairs = getFirstMatchModels(modelAlias, projectName, exactMatch, owner,
                modelAliasOrOwner, lastModifyFrom, lastModifyTo);
        val dfManager = getDataflowManager(projectName);
        List<NDataModelResponse> filterModels = new ArrayList<>();
        pairs.forEach(p -> {
            val dataflow = p.getKey();
            val modelDesc = p.getValue();
            long inconsistentSegmentCount = dfManager.getDataflow(modelDesc.getId())
                    .getSegments(SegmentStatusEnum.WARNING).size();
            ModelStatusToDisplayEnum modelResponseStatus = convertModelStatusToDisplay(modelDesc, projectName,
                    inconsistentSegmentCount);
            boolean isScd2ForbiddenOnline = checkSCD2ForbiddenOnline(modelDesc, projectName);
            boolean isModelStatusMatch = isListContains(status, modelResponseStatus);
            if (isModelStatusMatch) {
                NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
                nDataModelResponse.setForbiddenOnline(isScd2ForbiddenOnline);
                nDataModelResponse.setBroken(modelDesc.isBroken());
                nDataModelResponse.setStatus(modelResponseStatus);
                nDataModelResponse.setStorage(dfManager.getDataflowStorageSize(modelDesc.getUuid()));
                nDataModelResponse.setSource(dfManager.getDataflowSourceSize(modelDesc.getUuid()));
                nDataModelResponse.setSegmentHoles(dfManager.calculateSegHoles(modelDesc.getUuid()));
                nDataModelResponse.setExpansionrate(ModelUtils.computeExpansionRate(nDataModelResponse.getStorage(),
                        nDataModelResponse.getSource()));
                nDataModelResponse.setUsage(dataflow.getQueryHitCount());
                nDataModelResponse.setInconsistentSegmentCount(inconsistentSegmentCount);
                if (!modelDesc.isBroken()) {
                    nDataModelResponse
                            .setAvailableIndexesCount(getAvailableIndexesCount(projectName, modelDesc.getId()));
                    nDataModelResponse.setTotalIndexes(
                            getIndexPlan(modelDesc.getUuid(), modelDesc.getProject()).getAllLayouts().size());
                    nDataModelResponse.setEmptyIndexesCount(getEmptyIndexesCount(projectName, modelDesc.getId()));
                }
                filterModels.add(nDataModelResponse);
            }
        });
        if ("expansionrate".equalsIgnoreCase(sortBy)) {
            return sortExpansionRate(reverse, filterModels);
        } else if (getProjectManager().getProject(projectName).isSemiAutoMode()) {
            Comparator<NDataModelResponse> comparator = propertyComparator(
                    StringUtils.isEmpty(sortBy) ? ModelService.REC_COUNT : sortBy, !reverse);
            filterModels.sort(comparator);
            return filterModels;
        } else {
            Comparator<NDataModelResponse> comparator = propertyComparator(
                    StringUtils.isEmpty(sortBy) ? ModelService.LAST_MODIFY : sortBy, !reverse);
            filterModels.sort(comparator);
            return filterModels;
        }
    }

    private List<Pair<NDataflow, NDataModel>> getFirstMatchModels(final String modelAlias, final String projectName,
            boolean exactMatch, String owner, String modelAliasOrOwner, Long lastModifyFrom, Long lastModifyTo) {
        return getDataflowManager(projectName).listAllDataflows(true).stream()
                .map(df -> Pair.newPair(df,
                        df.checkBrokenWithRelatedInfo() ? getBrokenModel(projectName, df.getId()) : df.getModel()))
                .filter(p -> !(Objects.nonNull(lastModifyFrom) && lastModifyFrom > p.getValue().getLastModified())
                        && !(Objects.nonNull(lastModifyTo) && lastModifyTo <= p.getValue().getLastModified())
                        && (isArgMatch(modelAliasOrOwner, exactMatch, p.getValue().getAlias())
                                || isArgMatch(modelAliasOrOwner, exactMatch, p.getValue().getOwner()))
                        && isArgMatch(modelAlias, exactMatch, p.getValue().getAlias())
                        && isArgMatch(owner, exactMatch, p.getValue().getOwner()))
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

    private long getAvailableIndexesCount(String project, String id) {
        val dataflowManager = getDataflowManager(project);
        val dataflow = dataflowManager.getDataflow(id);
        if (dataflow == null) {
            return 0;
        }

        val readySegments = dataflow.getLatestReadySegment();

        if (readySegments == null) {
            return 0;
        }

        val readLayouts = readySegments.getLayoutsMap().keySet();
        return dataflow.getIndexPlan().getAllLayouts().stream()
                .filter(l -> readLayouts.contains(l.getId()) && !l.isToBeDeleted()).count();
    }

    private long getEmptyIndexesCount(String project, String id) {
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(id);
        return indexPlan.getAllLayouts().size() - getAvailableIndexesCount(project, id);
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

    private boolean isArgMatch(String valueToMatch, boolean exactMatch, String originValue) {
        return StringUtils.isEmpty(valueToMatch) || (exactMatch && originValue.equalsIgnoreCase(valueToMatch))
                || (!exactMatch
                        && originValue.toLowerCase(Locale.ROOT).contains(valueToMatch.toLowerCase(Locale.ROOT)));
    }

    private NDataModelResponse enrichModelResponse(NDataModel modelDesc, String projectName) {
        NDataModelResponse nDataModelResponse = new NDataModelResponse(modelDesc);

        if (modelDesc.isBroken()) {
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
            if (tableManager.getTableDesc(modelDesc.getRootFactTableName()) == null) {
                nDataModelResponse.setRootFactTableName(nDataModelResponse.getRootFactTableName() + " deleted");
                nDataModelResponse.setRootFactTableDeleted(true);
            }
            return nDataModelResponse;
        }
        nDataModelResponse.setAllTableRefs(modelDesc.getAllTables());
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
        return execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, INDEX_BUILD, SUB_PARTITION_BUILD);
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
        List<NDataSegmentResponse> segmentResponseList;
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        val segs = getSegmentsByRange(modelId, project, start, end);

        // filtering on index
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getIndexPlan(modelId);
        Set<Long> allIndexes = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        if (withAllIndexes != null && !withAllIndexes.isEmpty()) {
            for (Long idx : withAllIndexes) {
                if (!allIndexes.contains(idx)) {
                    throw new KylinException(INVALID_SEGMENT_PARAMETER, "Invalid index id " + idx);
                }
            }
        }
        if (withoutAnyIndexes != null && !withoutAnyIndexes.isEmpty()) {
            for (Long idx : withoutAnyIndexes) {
                if (!allIndexes.contains(idx)) {
                    throw new KylinException(INVALID_SEGMENT_PARAMETER, "Invalid index id " + idx);
                }
            }
        }
        List<NDataSegment> indexFiltered = new LinkedList<>();
        segs.forEach(segment -> filterSeg(withAllIndexes, withoutAnyIndexes, allToComplement, allIndexes, indexFiltered,
                segment));
        segmentResponseList = indexFiltered.stream()
                .filter(segment -> !StringUtils.isNotEmpty(status) || status
                        .equalsIgnoreCase(SegmentUtil.getSegmentStatusToDisplay(segs, segment, executables).toString()))
                .map(segment -> new NDataSegmentResponse(dataflow, segment, executables)).collect(Collectors.toList());
        Comparator<NDataSegmentResponse> comparator = propertyComparator(
                StringUtils.isEmpty(sortBy) ? "create_time_utc" : sortBy, reverse);
        segmentResponseList.sort(comparator);
        return segmentResponseList;
    }

    private void filterSeg(Collection<Long> withAllIndexes, Collection<Long> withoutAnyIndexes, boolean allToComplement,
            Set<Long> allIndexes, List<NDataSegment> indexFiltered, NDataSegment segment) {
        if (allToComplement) {
            // find seg that does not have all indexes
            if (segment.getSegDetails().getLayouts().size() != allIndexes.size()) {
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
        Comparator<IndicesResponse.Index> comparator = propertyComparator(
                StringUtils.isEmpty(sortBy) ? IndicesResponse.LAST_MODIFY_TIME : sortBy, !reverse);
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
            throw new KylinException(INVALID_MODEL_NAME,
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
        dropModel(modelId, project, false);
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> ModelDropAddListener.onDelete(project, modelId));
    }

    void dropModel(String modelId, String project, boolean ignoreType) {
        val projectInstance = getProjectManager().getProject(project);
        if (!ignoreType) {
            Preconditions.checkState(MaintainModelType.MANUAL_MAINTAIN == projectInstance.getMaintainModelType());
        }

        NDataModel dataModelDesc = getModelById(modelId, project);

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

            cleanIndexPlanWhenNoSegments(project, modelId);
        }
        offlineModelIfNecessary(dataflowManager, modelId);
    }

    @Transaction(project = 1)
    public void purgeModelManually(String modelId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModelDesc = getModelById(modelId, project);
        if (ManagementType.TABLE_ORIENTED == dataModelDesc.getManagementType()) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_CAN_NOT_PURGE(), dataModelDesc.getAlias()));
        }
        purgeModel(modelId, project);
    }

    @Transaction(project = 2)
    public void cloneModel(String modelId, String newModelName, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        checkAliasExist("", newModelName, project);
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel dataModelDesc = getModelById(modelId, project);
        //copyForWrite nDataModel do init,but can not set new modelname
        NDataModel nDataModel = semanticUpdater.deepCopyModel(dataModelDesc);
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setAlias(newModelName);
        nDataModel.setLastModified(System.currentTimeMillis());
        nDataModel.setMvcc(-1);
        changeModelOwner(nDataModel);
        val newModel = dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
        cloneIndexPlan(modelId, project, nDataModel.getOwner(), newModel.getUuid(), RealizationStatusEnum.OFFLINE);
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
        aclEvaluate.checkProjectWritePermission(project);
        NDataModel nDataModel = getModelById(modelId, project);
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
                    throw new KylinException(MODEL_ONLINE_ABANDON,
                            MsgPicker.getMsg().getSCD2_MODEL_ONLINE_WITH_SCD2_CONFIG_OFF());
                }
                if (dataflow.getSegments().isEmpty() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
                    throw new KylinException(MODEL_ONLINE_ABANDON, MsgPicker.getMsg().getMODEL_ONLINE_WITH_EMPTY_SEG());
                }
                if (dataflowManager.isOfflineModel(dataflow)) {
                    throw new KylinException(MODEL_ONLINE_ABANDON, MsgPicker.getMsg().getMODEL_ONLINE_FORBIDDEN());
                }
                nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            }
            dataflowManager.updateDataflow(nDataflowUpdate);
        }
    }

    private void checkDataflowStatus(NDataflow dataflow, String modelId) {
        if (RealizationStatusEnum.BROKEN == dataflow.getStatus()) {
            throw new KylinException(DUPLICATE_JOIN_CONDITION,
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
                        throw new KylinException(EMPTY_SEGMENT_RANGE,
                                "No segments to refresh, please select new range and try again!");
                    }
                }

                if (CollectionUtils.isNotEmpty(segments.getBuildingSegments())) {
                    throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
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
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
            }
        }
    }

    private void checkRefreshRangeWithinCoveredRange(NDataLoadingRange dataLoadingRange,
            SegmentRange toBeRefreshSegmentRange) {
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new KylinException(INVALID_SEGMENT_RANGE, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_INVALID_RANGE(), toBeRefreshSegmentRange, coveredReadySegmentRange));
        }
    }

    @Transaction(project = 0)
    public void refreshSegments(String project, String table, String refreshStart, String refreshEnd,
            String affectedStart, String affectedEnd) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        RefreshAffectedSegmentsResponse response = getRefreshAffectedSegmentsResponse(project, table, refreshStart,
                refreshEnd);
        if (!response.getAffectedStart().equals(affectedStart) || !response.getAffectedEnd().equals(affectedEnd)) {
            throw new KylinException(PERMISSION_DENIED,
                    MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE());
        }
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        SegmentRange segmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(refreshStart, refreshEnd);
        segmentHelper.refreshRelatedModelSegments(project, table, segmentRange);
    }

    @VisibleForTesting
    public void checkFlatTableSql(NDataModel dataModel) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        try {
            ProjectInstance project = getProjectManager().getProject(dataModel.getProject());
            if (project.getSourceType() == ISourceAware.ID_SPARK) {
                SparkSession ss = SparderEnv.getSparkSession();
                String flatTableSql = JoinedFlatTable.generateSelectDataStatement(dataModel, false);
                QueryParams queryParams = new QueryParams(dataModel.getProject(), flatTableSql, "default", false);
                queryParams.setKylinConfig(QueryUtil.getKylinConfig(dataModel.getProject()));
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
                throw new KylinException(TABLE_NOT_EXIST, error);
            } else {
                throw new KylinException(FAILED_EXECUTE_MODEL_SQL,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDEFAULT_MODEL_REASON()), e);
            }
        }
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
        }
    }

    public void batchCreateModel(String project, List<ModelRequest> newModels, List<ModelRequest> reusedModels) {
        aclEvaluate.checkProjectWritePermission(project);
        checkDuplicateAliasInModelRequests(newModels);
        for (ModelRequest modelRequest : newModels) {
            validatePartitionDateColumn(modelRequest);
            modelRequest.setProject(project);
            doCheckBeforeModelSave(project, modelRequest);
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            saveNewModelsAndIndexes(project, newModels);
            updateReusedModelsAndIndexPlans(project, reusedModels);
            return null;
        }, project);
    }

    private void saveNewModelsAndIndexes(String project, List<ModelRequest> newModels) {
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
            if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                dataModelManager.updateDataModelDesc(model);
            } else {
                dataModelManager.createDataModelDesc(model, model.getOwner());
            }

            // create IndexPlan
            IndexPlan emptyIndex = new IndexPlan();
            emptyIndex.setUuid(model.getUuid());
            indexPlanManager.createIndexPlan(emptyIndex);

            // create DataFlow
            val df = dataflowManager.createDataflow(emptyIndex, model.getOwner());
            if (modelRequest.isWithEmptySegment()) {
                dataflowManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                        SegmentStatusEnum.READY);
            }
            if (modelRequest.isWithModelOnline()) {
                dataflowManager.updateDataflow(df.getId(),
                        copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.ONLINE));
            }

            updateIndexPlan(project, indexPlan);

            UnitOfWorkContext context = UnitOfWork.get();
            context.doAfterUnit(() -> ModelDropAddListener.onAdd(project, model.getId(), model.getAlias()));
        }
    }

    private void updateReusedModelsAndIndexPlans(String project, List<ModelRequest> modelRequestList) {
        if (CollectionUtils.isEmpty(modelRequestList)) {
            return;
        }
        for (ModelRequest modelRequest : modelRequestList) {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

            Map<String, NDataModel.NamedColumn> columnMap = Maps.newHashMap();
            modelRequest.getAllNamedColumns().forEach(column -> {
                Preconditions.checkArgument(!columnMap.containsKey(column.getAliasDotColumn()));
                columnMap.put(column.getAliasDotColumn(), column);
            });

            // update model
            List<LayoutRecDetailResponse> recItems = modelRequest.getRecItems();
            modelManager.updateDataModel(modelRequest.getId(), copyForWrite -> {
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
                newMeasureMap.forEach((measureName, measure) -> copyForWrite.getAllMeasures().add(measure));
                // keep order of all columns and measures
                copyForWrite.keepColumnOrder();
                copyForWrite.keepMeasureOrder();
            });

            // update IndexPlan
            IndexPlan indexPlan = modelRequest.getIndexPlan();
            Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();
            indexPlan.getAllLayouts().forEach(layout -> layoutMap.putIfAbsent(layout.getId(), layout));
            indexPlanManager.updateIndexPlan(modelRequest.getId(), copyForWrite -> {
                Map<IndexEntity.IndexIdentifier, IndexEntity> existingIndexMap = copyForWrite.getAllIndexesMap();
                for (LayoutRecDetailResponse recItem : recItems) {
                    long layoutId = recItem.getIndexId();
                    LayoutEntity layout = layoutMap.get(layoutId);
                    IndexEntity.IndexIdentifier indexIdentifier = layout.getIndex().createIndexIdentifier();
                    if (existingIndexMap.containsKey(indexIdentifier)) {
                        IndexEntity index = existingIndexMap.get(indexIdentifier);
                        if (index.getLayout(layoutId) == null) {
                            index.getLayouts().add(layout);
                        }
                    } else {
                        IndexEntity index = layout.getIndex();
                        existingIndexMap.putIfAbsent(indexIdentifier, index);
                        copyForWrite.getIndexes().add(index);
                    }
                    copyForWrite.setApprovedAdditionalRecs(copyForWrite.getApprovedAdditionalRecs() + 1);
                }
            });
        }
    }

    @VisibleForTesting
    void saveRecResult(ModelSuggestionResponse modelSuggestionResponse, String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (NRecommendedModelResponse response : modelSuggestionResponse.getReusedModels()) {

                NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                modelMgr.updateDataModel(response.getId(), copyForWrite -> {
                    copyForWrite.setComputedColumnDescs(response.getComputedColumnDescs());
                    copyForWrite.setAllNamedColumns(response.getAllNamedColumns());
                    copyForWrite.setAllMeasures(response.getAllMeasures());
                });
                NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val targetIndexPlan = response.getIndexPlan();
                indexMgr.updateIndexPlan(response.getId(), copyForWrite -> {
                    copyForWrite.setIndexes(targetIndexPlan.getIndexes());
                });
            }
            return null;
        }, project);
    }

    public OpenModelSuggestionResponse suggestOrOptimizeModels(OpenSqlAccelerateRequest request) {
        AbstractContext proposeContext = suggestModel(request.getProject(), request.getSqls(),
                !request.getForce2CreateNewModel(), false);
        ModelSuggestionResponse modelSuggestionResponse = buildModelSuggestionResponse(proposeContext);

        OpenModelSuggestionResponse result;
        if (request.getForce2CreateNewModel()) {
            List<ModelRequest> modelRequests = modelSuggestionResponse.getNewModels().stream().map(modelResponse -> {
                ModelRequest modelRequest = new ModelRequest(modelResponse);
                modelRequest.setIndexPlan(modelResponse.getIndexPlan());
                modelRequest.setWithEmptySegment(request.isWithEmptySegment());
                modelRequest.setWithModelOnline(request.isWithModelOnline());
                return modelRequest;
            }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(modelRequests)) {
                batchCreateModel(request.getProject(), modelRequests, Lists.newArrayList());
            }

            result = OpenModelSuggestionResponse.convert(modelSuggestionResponse.getNewModels());
        } else {
            result = OpenModelSuggestionResponse.convert(modelSuggestionResponse.getReusedModels());
        }

        if (request.isAcceptRecommendation()) {
            saveRecResult(modelSuggestionResponse, request.getProject());
        } else {
            rawRecService.transferAndSaveRecommendations(proposeContext);
        }

        Set<String> normalRecommendedSqlSet = Sets.newHashSet();
        for (OpenModelSuggestionResponse.RecommendationsResponse modelResponse : result.getModels()) {
            modelResponse.getIndexes().forEach(layoutRecDetailResponse -> {
                List<String> sqlList = layoutRecDetailResponse.getSqlList();
                normalRecommendedSqlSet.addAll(sqlList);
            });
        }

        result.setErrorSqlList(Lists.newArrayList());
        for (String sql : request.getSqls()) {
            if (!normalRecommendedSqlSet.contains(sql)) {
                result.getErrorSqlList().add(sql);
            }
        }

        return result;
    }

    public NDataModel createModel(String project, ModelRequest modelRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        validatePartitionDateColumn(modelRequest);

        // for probing date-format is a time-costly action, it cannot be call in a transaction
        doCheckBeforeModelSave(project, modelRequest);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> saveModel(project, modelRequest), project);
    }

    public Map<String, List<NDataModel>> answeredByExistedModels(String project, Set<String> sqls) {
        Map<String, List<NDataModel>> result = Maps.newHashMap();
        if (CollectionUtils.isEmpty(sqls)) {
            return result;
        }

        for (String sql : sqls) {
            if (result.containsKey(sql)) {
                result.get(sql).addAll(couldAnsweredByExistedModels(project, Lists.newArrayList(sql)));
            } else {
                result.put(sql, couldAnsweredByExistedModels(project, Lists.newArrayList(sql)));
            }
        }

        return result;
    }

    public List<NDataModel> couldAnsweredByExistedModels(String project, List<String> sqls) {
        if (CollectionUtils.isEmpty(sqls)) {
            return Lists.newArrayList();
        }

        if (isProjectNotExist(project)) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }

        AbstractContext proposeContext = new ModelSelectContextOfSemiV2(KylinConfig.getInstanceFromEnv(), project,
                sqls.toArray(new String[0]));
        ProposerJob.propose(proposeContext);
        return proposeContext.getProposedModels();
    }

    public boolean couldAnsweredByExistedModel(String project, List<String> sqls) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return true;
        }

        return CollectionUtils.isNotEmpty(couldAnsweredByExistedModels(project, sqls));
    }

    private void checkBatchSqlSize(List<String> sqls) {
        KylinConfig kylinConfig1 = KylinConfig.getInstanceFromEnv();
        val msg = MsgPicker.getMsg();
        int limit = kylinConfig1.getSuggestModelSqlLimit();
        if (sqls.size() > limit) {
            throw new KylinException(SQL_NUMBER_EXCEEDS_LIMIT,
                    String.format(Locale.ROOT, msg.getSQL_NUMBER_EXCEEDS_LIMIT(), limit));
        }
    }

    public AbstractContext suggestModel(String project, List<String> sqls, boolean reuseExistedModel,
            boolean createNewModel) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return null;
        }
        checkBatchSqlSize(sqls);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        AbstractContext proposeContext = reuseExistedModel
                ? new ModelReuseContextOfSemiV2(kylinConfig, project, sqls.toArray(new String[0]), createNewModel)
                : new ModelCreateContextOfSemiV2(kylinConfig, project, sqls.toArray(new String[0]));
        return ProposerJob.propose(proposeContext);
    }

    public ModelSuggestionResponse buildModelSuggestionResponse(AbstractContext context) {
        List<NRecommendedModelResponse> responseOfNewModels = Lists.newArrayList();
        List<NRecommendedModelResponse> responseOfReusedModels = Lists.newArrayList();

        for (ModelContext modelContext : context.getModelContexts()) {
            if (modelContext.isTargetModelMissing()) {
                continue;
            }

            if (modelContext.getOriginModel() != null) {
                collectResponseOfReusedModels(modelContext, responseOfReusedModels);
            } else {
                collectResponseOfNewModels(context, modelContext, responseOfNewModels);
            }
        }
        return new ModelSuggestionResponse(responseOfReusedModels, responseOfNewModels);
    }

    private void collectResponseOfReusedModels(ModelContext modelContext,
            List<NRecommendedModelResponse> responseOfReusedModels) {
        Map<Long, Set<String>> layoutToSqlSet = mapLayoutToSqlSet(modelContext);
        Map<String, ComputedColumnDesc> oriCCMap = Maps.newHashMap();
        List<ComputedColumnDesc> oriCCList = modelContext.getOriginModel().getComputedColumnDescs();
        oriCCList.forEach(cc -> oriCCMap.put(cc.getFullName(), cc));
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        List<ComputedColumnDesc> ccList = modelContext.getTargetModel().getComputedColumnDescs();
        ccList.forEach(cc -> ccMap.put(cc.getFullName(), cc));
        NDataModel targetModel = modelContext.getTargetModel();
        NDataModel originModel = modelContext.getOriginModel();
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        modelContext.getIndexRexItemMap().forEach((key, layoutRecItemV2) -> {
            LayoutRecDetailResponse response = new LayoutRecDetailResponse();
            LayoutEntity layout = layoutRecItemV2.getLayout();
            ImmutableList<Integer> colOrder = layout.getColOrder();
            Map<ComputedColumnDesc, Boolean> ccStateMap = Maps.newHashMap();
            Map<Integer, NDataModel.NamedColumn> colsOfTargetModelMap = Maps.newHashMap();
            targetModel.getAllNamedColumns().forEach(col -> colsOfTargetModelMap.put(col.getId(), col));
            colOrder.forEach(idx -> {
                if (idx < NDataModel.MEASURE_ID_BASE && originModel.getEffectiveDimensions().containsKey(idx)) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    String dataType = originModel.getEffectiveDimensions().get(idx).getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, false, dataType));
                } else if (idx < NDataModel.MEASURE_ID_BASE) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    TblColRef tblColRef = targetModel.getEffectiveCols().get(idx);
                    String colRefAliasDotName = tblColRef.getAliasDotName();
                    if (tblColRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                        ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                    }
                    String dataType = tblColRef.getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, true, dataType));
                } else if (originModel.getEffectiveMeasures().containsKey(idx)) {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, false));
                } else {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    List<TblColRef> colRefs = measure.getFunction().getColRefs();
                    colRefs.forEach(colRef -> {
                        String colRefAliasDotName = colRef.getAliasDotName();
                        if (colRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                            ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                        }
                    });
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, true));
                }
            });
            List<LayoutRecDetailResponse.RecComputedColumn> newCCList = Lists.newArrayList();
            ccStateMap.forEach((k, v) -> newCCList.add(new LayoutRecDetailResponse.RecComputedColumn(k, v)));
            response.setComputedColumns(newCCList);
            response.setIndexId(layout.getId());
            Set<String> sqlSet = layoutToSqlSet.get(layout.getId());
            if (CollectionUtils.isNotEmpty(sqlSet)) {
                response.setSqlList(Lists.newArrayList(sqlSet));
            }
            indexRecItems.add(response);
        });

        NRecommendedModelResponse response = new NRecommendedModelResponse(targetModel);
        response.setIndexPlan(modelContext.getTargetIndexPlan());
        response.setIndexes(indexRecItems);
        responseOfReusedModels.add(response);
    }

    private Map<Long, Set<String>> mapLayoutToSqlSet(ModelContext modelContext) {
        if (modelContext == null) {
            return Maps.newHashMap();
        }
        Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
        Map<Long, Set<String>> layoutToSqlSet = Maps.newHashMap();
        accelerateInfoMap.forEach((sql, info) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : info.getRelatedLayouts()) {
                if (!StringUtils.equalsIgnoreCase(relation.getModelId(), modelContext.getTargetModel().getUuid())) {
                    continue;
                }
                layoutToSqlSet.putIfAbsent(relation.getLayoutId(), Sets.newHashSet());
                layoutToSqlSet.get(relation.getLayoutId()).add(relation.getSql());
            }
        });
        return layoutToSqlSet;
    }

    private void collectResponseOfNewModels(AbstractContext context, ModelContext modelContext,
            List<NRecommendedModelResponse> responseOfNewModels) {
        val sqlList = context.getAccelerateInfoMap().entrySet().stream()//
                .filter(entry -> entry.getValue().getRelatedLayouts().stream()//
                        .anyMatch(relation -> relation.getModelId().equals(modelContext.getTargetModel().getId())))
                .map(Map.Entry::getKey).collect(Collectors.toList());
        NDataModel model = modelContext.getTargetModel();
        IndexPlan indexPlan = modelContext.getTargetIndexPlan();
        ImmutableBiMap<Integer, TblColRef> effectiveDimensions = model.getEffectiveDimensions();
        List<LayoutRecDetailResponse.RecDimension> recDims = model.getAllNamedColumns().stream() //
                .filter(NDataModel.NamedColumn::isDimension) //
                .map(c -> {
                    String datatype = effectiveDimensions.get(c.getId()).getDatatype();
                    return new LayoutRecDetailResponse.RecDimension(c, true, datatype);
                }) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecMeasure> recMeasures = model.getAllMeasures().stream() //
                .map(measure -> new LayoutRecDetailResponse.RecMeasure(measure, true)) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecComputedColumn> recCCList = model.getComputedColumnDescs().stream() //
                .map(cc -> new LayoutRecDetailResponse.RecComputedColumn(cc, true)) //
                .collect(Collectors.toList());
        LayoutRecDetailResponse virtualResponse = new LayoutRecDetailResponse();
        virtualResponse.setIndexId(-1L);
        virtualResponse.setDimensions(recDims);
        virtualResponse.setMeasures(recMeasures);
        virtualResponse.setComputedColumns(recCCList);
        virtualResponse.setSqlList(sqlList);

        NRecommendedModelResponse response = new NRecommendedModelResponse(model);
        response.setIndexPlan(indexPlan);
        response.setIndexes(Lists.newArrayList(virtualResponse));
        responseOfNewModels.add(response);
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
            throw new KylinException(FAILED_CREATE_MODEL, MsgPicker.getMsg().getINVALID_CREATE_MODEL());
        }

        preProcessBeforeModelSave(dataModel, project);
        checkFlatTableSql(dataModel);
        return dataModel;
    }

    private NDataModel saveModel(String project, ModelRequest modelRequest) {
        validatePartitionDateColumn(modelRequest);

        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        preProcessBeforeModelSave(dataModel, project);
        val model = getDataModelManager(project).createDataModelDesc(dataModel, dataModel.getOwner());
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(model.getUuid());
        indexPlan.setLastModified(System.currentTimeMillis());
        indexPlanManager.createIndexPlan(indexPlan);
        val df = dataflowManager.createDataflow(indexPlan, model.getOwner(), RealizationStatusEnum.OFFLINE);
        SegmentRange range = null;
        if (model.getPartitionDesc() == null
                || StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        } else if (StringUtils.isNotEmpty(modelRequest.getStart()) && StringUtils.isNotEmpty(modelRequest.getEnd())) {
            range = getSegmentRangeByModel(project, model.getUuid(), modelRequest.getStart(), modelRequest.getEnd());
        }
        if (range != null) {
            dataflowManager.fillDfManually(df, Lists.newArrayList(range));
        }
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> ModelDropAddListener.onAdd(project, model.getId(), model.getAlias()));
        return getDataModelManager(project).getDataModelDesc(model.getUuid());
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

    private NDataModel getBrokenModel(String project, String modelId) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDescWithoutInit(modelId);
        model.setBroken(true);
        return model;
    }

    private void checkModelRequest(ModelRequest request) {
        checkModelOwner(request);
        checkModelDimensions(request);
        checkModelMeasures(request);
        checkModelJoinConditions(request);
    }

    private void checkModelOwner(ModelRequest request) {
        if (StringUtils.isBlank(request.getOwner())) {
            throw new KylinException(INVALID_PARAMETER, "Invalid parameter, model owner is empty.");
        }
    }

    @VisibleForTesting
    public void checkModelDimensions(ModelRequest request) {
        Set<String> dimensionNames = new HashSet<>();

        int maxModelDimensionMeasureNameLength = KylinConfig.getInstanceFromEnv()
                .getMaxModelDimensionMeasureNameLength();

        for (NDataModel.NamedColumn dimension : request.getSimplifiedDimensions()) {
            dimension.setName(StringUtils.trim(dimension.getName()));
            // check if the dimension name is valid
            if (StringUtils.length(dimension.getName()) > maxModelDimensionMeasureNameLength
                    || !Pattern.compile(VALID_NAME_FOR_DIMENSION_MEASURE).matcher(dimension.getName()).matches())
                throw new KylinException(INVALID_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_DIMENSION_NAME(), dimension.getName(),
                                maxModelDimensionMeasureNameLength));

            // check duplicate dimension names
            if (dimensionNames.contains(dimension.getName()))
                throw new KylinException(DUPLICATE_DIMENSION_NAME, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDUPLICATE_DIMENSION_NAME(), dimension.getName()));

            dimensionNames.add(dimension.getName());
        }
    }

    @VisibleForTesting
    public void checkModelMeasures(ModelRequest request) {
        Set<String> measureNames = new HashSet<>();
        Set<SimplifiedMeasure> measures = new HashSet<>();
        int maxModelDimensionMeasureNameLength = KylinConfig.getInstanceFromEnv()
                .getMaxModelDimensionMeasureNameLength();

        for (SimplifiedMeasure measure : request.getSimplifiedMeasures()) {
            measure.setName(StringUtils.trim(measure.getName()));
            // check if the measure name is valid
            if (StringUtils.length(measure.getName()) > maxModelDimensionMeasureNameLength
                    || !Pattern.compile(VALID_NAME_FOR_DIMENSION_MEASURE).matcher(measure.getName()).matches())
                throw new KylinException(INVALID_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MEASURE_NAME(), measure.getName(),
                                maxModelDimensionMeasureNameLength));

            // check duplicate measure names
            if (measureNames.contains(measure.getName()))
                throw new KylinException(DUPLICATE_MEASURE_NAME,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDUPLICATE_MEASURE_NAME(), measure.getName()));

            // check duplicate measure definitions
            if (measures.contains(measure))
                throw new KylinException(DUPLICATE_MEASURE_EXPRESSION, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDUPLICATE_MEASURE_DEFINITION(), measure.getName()));

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
                    throw new KylinException(DUPLICATE_JOIN_CONDITION, String.format(Locale.ROOT,
                            MsgPicker.getMsg().getDUPLICATE_JOIN_CONDITIONS(), primaryKeys[i], foreignKey[i]));

                joinKeys.add(Pair.newPair(primaryKeys[i], foreignKey[i]));
            }
        }
    }

    private String probeDateFormatIfNotExist(String project, NDataModel modelDesc) throws Exception {
        val partitionDesc = modelDesc.getPartitionDesc();
        if (partitionDesc == null || StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
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

    private void saveDateFormatIfNotExist(String project, String modelId, String format) {
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
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
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
                jobInfos.add(constructIncrementBuild(new IncrementBuildSegmentParams(project, modelId, hole.getStart(),
                        hole.getEnd(), format, true, allPartitions).withIgnoredSnapshotTables(ignoredSnapshotTables)));
            }
            return jobInfos;
        }, project);

        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;

    }

    //only fo test
    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end)
            throws Exception {
        return buildSegmentsManually(project, modelId, start, end, true, Sets.newHashSet(), null);
    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end,
            boolean needBuild, Set<String> ignoredSnapshotTables, List<String[]> multiPartitionValuess)
            throws Exception {
        return buildSegmentsManually(project, modelId, start, end, needBuild, ignoredSnapshotTables,
                multiPartitionValuess, ExecutablePO.DEFAULT_PRIORITY);
    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end,
            boolean needBuild, Set<String> ignoredSnapshotTables, List<String[]> multiPartitionValues, int priority)
            throws Exception {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (!modelDesc.isMultiPartitionModel() && !CollectionUtils.isEmpty(multiPartitionValues)) {
            throw new KylinException(PARTITION_VALUE_NOT_SUPPORT, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getPARTITION_VALUE_NOT_SUPPORT(), modelDesc.getAlias()));
        }
        if (modelDesc.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateColumn())) {
            return fullBuildSegmentsManually(new FullBuildSegmentParams(project, modelId, needBuild)
                    .withIgnoredSnapshotTables(ignoredSnapshotTables).withPriority(priority));
        } else {
            return incrementBuildSegmentsManually(
                    new IncrementBuildSegmentParams(project, modelId, start, end, modelDesc.getPartitionDesc(),
                            modelDesc.getMultiPartitionDesc(), Lists.newArrayList(), needBuild, multiPartitionValues)
                                    .withIgnoredSnapshotTables(ignoredSnapshotTables).withPriority(priority));
        }
    }

    public JobInfoResponse fullBuildSegmentsManually(FullBuildSegmentParams params) {
        aclEvaluate.checkProjectOperationPermission(params.getProject());

        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(() -> constructFullBuild(params), params.getProject());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> constructFullBuild(FullBuildSegmentParams params) {
        checkModelAndIndexManually(params);
        String project = params.getProject();
        String modelId = params.getModelId();
        boolean needBuild = params.isNeedBuild();

        NDataModel model = getDataModelManager(project).getDataModelDesc(modelId);
        if (model.getPartitionDesc() != null
                && !StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            //increment build model
            throw new IllegalArgumentException(MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT());

        }
        val dataflowManager = getDataflowManager(project);
        val df = dataflowManager.getDataflow(modelId);
        val seg = df.getFirstSegment();
        if (Objects.isNull(seg)) {
            NDataSegment newSegment = dataflowManager.appendSegment(df,
                    SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                    needBuild ? SegmentStatusEnum.NEW : SegmentStatusEnum.READY);
            if (!needBuild) {
                return new LinkedList<>();
            }
            return Lists.newArrayList(new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(),
                    getSourceUsageManager().licenseCheckWrap(project,
                            () -> getJobManager(project).addSegmentJob(new JobParam(newSegment, modelId, getUsername())
                                    .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                                    .withPriority(params.getPriority())))));
        }
        if (!needBuild) {
            return new LinkedList<>();
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(2);

        RefreshSegmentParams refreshSegmentParams = new RefreshSegmentParams(project, modelId,
                Lists.newArrayList(getDataflowManager(project).getDataflow(modelId).getSegments().get(0).getId())
                        .toArray(new String[0]),
                true).withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()).withPriority(params.getPriority());
        res.addAll(refreshSegmentById(refreshSegmentParams));
        return res;
    }

    private JobInfoResponse.JobInfo constructIncrementBuild(IncrementBuildSegmentParams params) {
        String project = params.getProject();
        String modelId = params.getModelId();

        NDataModel modelDescInTransaction = getDataModelManager(project).getDataModelDesc(modelId);
        JobManager jobManager = getJobManager(project);
        TableDesc table = getTableManager(project).getTableDesc(modelDescInTransaction.getRootFactTableName());
        val df = getDataflowManager(project).getDataflow(modelId);
        if (modelDescInTransaction.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDescInTransaction.getPartitionDesc().getPartitionDateColumn())) {
            throw new IllegalArgumentException("Can not add a new segment on full build model.");
        }
        Preconditions.checkArgument(!PushDownUtil.needPushdown(params.getStart(), params.getEnd()),
                "Load data must set start and end date");
        val segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(params.getStart(), params.getEnd());
        checkSegmentToBuildOverlapsBuilt(project, modelId, segmentRangeToBuild);
        saveDateFormatIfNotExist(project, modelId, params.getPartitionColFormat());
        checkMultiPartitionBuildParam(modelDescInTransaction, params);
        NDataSegment newSegment = getDataflowManager(project).appendSegment(df, segmentRangeToBuild,
                params.isNeedBuild() ? SegmentStatusEnum.NEW : SegmentStatusEnum.READY,
                params.getMultiPartitionValues());
        if (!params.isNeedBuild()) {
            return null;
        }
        JobParam jobParam = new JobParam(newSegment, modelId, getUsername())
                .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()).withPriority(params.getPriority());
        if (modelDescInTransaction.isMultiPartitionModel()) {
            val model = getDataModelManager(project).getDataModelDesc(modelId);
            jobParam.setTargetPartitions(
                    model.getMultiPartitionDesc().getPartitionIdsByValues(params.getMultiPartitionValues()));
        }
        return new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(),
                getSourceUsageManager().licenseCheckWrap(project, () -> jobManager.addSegmentJob(jobParam)));
    }

    public void checkMultiPartitionBuildParam(NDataModel model, IncrementBuildSegmentParams params) {
        if (!model.isMultiPartitionModel()) {
            return;
        }
        if (params.isNeedBuild() && CollectionUtils.isEmpty(params.getMultiPartitionValues())) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_EMPTY());
        }
        if (!params.isNeedBuild() && !CollectionUtils.isEmpty(params.getMultiPartitionValues())) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_ABANDON());
        }
        for (String[] values : params.getMultiPartitionValues()) {
            if (values.length != model.getMultiPartitionDesc().getColumns().size()) {
                throw new KylinException(FAILED_CREATE_JOB,
                        MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_ABANDON());
            }
        }
    }

    //only for test
    public JobInfoResponse incrementBuildSegmentsManually(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, List<SegmentTimeRequest> segmentHoles) throws Exception {
        return incrementBuildSegmentsManually(new IncrementBuildSegmentParams(project, modelId, start, end,
                partitionDesc, null, segmentHoles, true, null));
    }

    @Transaction(project = 0)
    public JobInfoResponseWithFailure addIndexesToSegments(String project, String modelId, List<String> segmentIds,
            List<Long> indexIds, boolean parallelBuildBySegment, int priority) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkModelPermission(project, modelId);
        val dfManger = getDataflowManager(project);
        NDataflow dataflow = dfManger.getDataflow(modelId);
        checkSegmentsExistById(modelId, project, segmentIds.toArray(new String[0]));
        if (parallelBuildBySegment) {
            return addIndexesToSegmentsParallelly(project, modelId, segmentIds, indexIds, dataflow);
        } else {
            val jobManager = getJobManager(project);
            JobInfoResponseWithFailure result = new JobInfoResponseWithFailure();
            List<JobInfoResponse.JobInfo> jobs = new LinkedList<>();
            try {
                JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                        getSourceUsageManager().licenseCheckWrap(project,
                                () -> jobManager.addRelatedIndexJob(new JobParam(Sets.newHashSet(segmentIds),
                                        indexIds == null ? null : new HashSet<>(indexIds), modelId, getUsername())
                                                .withPriority(priority))));
                jobs.add(jobInfo);
            } catch (JobSubmissionException e) {
                result.addFailedSeg(dataflow, e);
            }
            result.setJobs(jobs);
            return result;
        }
    }

    private JobInfoResponseWithFailure addIndexesToSegmentsParallelly(String project, String modelId,
            List<String> segmentIds, List<Long> indexIds, NDataflow dataflow) {
        JobInfoResponseWithFailure result = new JobInfoResponseWithFailure();
        List<JobInfoResponse.JobInfo> jobs = new LinkedList<>();
        val jobManager = getJobManager(project);
        for (String segmentId : segmentIds) {
            try {
                JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                        getSourceUsageManager().licenseCheckWrap(project,
                                () -> jobManager.addRelatedIndexJob(new JobParam(Sets.newHashSet(segmentId),
                                        indexIds == null ? null : new HashSet<>(indexIds), modelId, getUsername()))));
                jobs.add(jobInfo);
            } catch (JobSubmissionException e) {
                result.addFailedSeg(dataflow, e);
            }
        }
        result.setJobs(jobs);
        return result;
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
            return null;
        }, project);
    }

    public JobInfoResponse incrementBuildSegmentsManually(IncrementBuildSegmentParams params) throws Exception {
        String project = params.getProject();
        aclEvaluate.checkProjectOperationPermission(project);
        checkModelPermission(project, params.getModelId());
        val modelManager = getDataModelManager(project);
        if (params.getPartitionDesc() == null
                || StringUtils.isEmpty(params.getPartitionDesc().getPartitionDateColumn())) {
            throw new KylinException(EMPTY_PARTITION_COLUMN, "Partition column is null.'");
        }

        String startFormat = DateFormat
                .getFormatTimeStamp(params.getStart(), params.getPartitionDesc().getPartitionDateFormat()).toString();
        String endFormat = DateFormat
                .getFormatTimeStamp(params.getEnd(), params.getPartitionDesc().getPartitionDateFormat()).toString();

        NDataModel copyModel = modelManager.copyForWrite(modelManager.getDataModelDesc(params.getModelId()));
        copyModel.setPartitionDesc(params.getPartitionDesc());
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), project).getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(),
                project);
        String format = probeDateFormatIfNotExist(project, copyModel);

        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                () -> innerIncrementBuild(new IncrementBuildSegmentParams(project, params.getModelId(), startFormat,
                        endFormat, params.getPartitionDesc(), params.getMultiPartitionDesc(), format,
                        params.getSegmentHoles(), params.isNeedBuild(), params.getMultiPartitionValues())
                                .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                                .withPriority(params.getPriority())),
                project);
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> innerIncrementBuild(IncrementBuildSegmentParams params) throws IOException {

        checkModelAndIndexManually(params);
        if (CollectionUtils.isEmpty(params.getSegmentHoles())) {
            params.setSegmentHoles(Lists.newArrayList());
        }
        NDataModel modelDesc = getDataModelManager(params.getProject()).getDataModelDesc(params.getModelId());
        if (modelDesc.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateColumn())
                || !modelDesc.getPartitionDesc().equals(params.getPartitionDesc()) || !ModelSemanticHelper
                        .isMultiPartitionDescSame(modelDesc.getMultiPartitionDesc(), params.getMultiPartitionDesc())) {
            aclEvaluate.checkProjectWritePermission(params.getProject());
            val request = convertToRequest(modelDesc);
            request.setPartitionDesc(params.getPartitionDesc());
            request.setProject(params.getProject());
            request.setMultiPartitionDesc(params.getMultiPartitionDesc());
            updateDataModelSemantic(params.getProject(), request);
            params.getSegmentHoles().clear();
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(params.getSegmentHoles().size() + 2);
        List<String[]> allPartitions = null;
        if (modelDesc.isMultiPartitionModel()) {
            allPartitions = modelDesc.getMultiPartitionDesc().getPartitions().stream()
                    .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
        }
        for (SegmentTimeRequest hole : params.getSegmentHoles()) {
            res.add(constructIncrementBuild(new IncrementBuildSegmentParams(params.getProject(), params.getModelId(),
                    hole.getStart(), hole.getEnd(), params.getPartitionColFormat(), true, allPartitions)
                            .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                            .withPriority(params.getPriority())));
        }
        res.add(constructIncrementBuild(new IncrementBuildSegmentParams(params.getProject(), params.getModelId(),
                params.getStart(), params.getEnd(), params.getPartitionColFormat(), params.isNeedBuild(),
                params.getMultiPartitionValues()).withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                        .withPriority(params.getPriority())));
        return res;
    }

    ModelRequest convertToRequest(NDataModel modelDesc) throws IOException {
        val request = new ModelRequest(JsonUtil.deepCopy(modelDesc, NDataModel.class));
        request.setSimplifiedMeasures(modelDesc.getEffectiveMeasures().values().stream()
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(modelDesc.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList()));
        request.setComputedColumnDescs(modelDesc.getComputedColumnDescs());
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
        if (partitionDesc == null && model.getPartitionDesc() == null && df.getFirstSegment() == null
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
            updateDataModelSemantic(project, request);
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
            throw new KylinException(MODEL_NOT_EXIST,
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

    private void checkModelAndIndexManually(FullBuildSegmentParams params) {
        NDataModel modelDesc = getDataModelManager(params.getProject()).getDataModelDesc(params.getModelId());
        if (ManagementType.MODEL_BASED != modelDesc.getManagementType()) {
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT_MANUALLY(), modelDesc.getAlias()));
        }

        if (params.isNeedBuild()) {
            val indexPlan = getIndexPlan(params.getModelId(), params.getProject());
            if (indexPlan == null || indexPlan.getAllLayouts().isEmpty()) {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT());
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

    private void checkSegmentToBuildOverlapsBuilt(String project, String model, SegmentRange segmentRangeToBuild) {
        Segments<NDataSegment> segments = getSegmentsByRange(model, project, "0", "" + Long.MAX_VALUE);
        if (!CollectionUtils.isEmpty(segments)) {
            for (NDataSegment existedSegment : segments) {
                if (existedSegment.getSegRange().overlaps(segmentRangeToBuild)) {
                    throw new KylinException(SEGMENT_RANGE_OVERLAP,
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
            throw new KylinException(MODEL_NOT_EXIST, msg.getINVALID_MODEL_DEFINITION());
        }

        String modelAlias = modelDesc.getAlias();

        if (StringUtils.isEmpty(modelAlias)) {
            throw new KylinException(EMPTY_MODEL_NAME, msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL)) {
            throw new KylinException(INVALID_MODEL_NAME,
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
    public ComputedColumnCheckResponse checkComputedColumn(final NDataModel dataModelDesc, String project,
            String ccInCheck) {
        aclEvaluate.checkProjectWritePermission(project);
        if (dataModelDesc.getUuid() == null)
            dataModelDesc.updateRandomUuid();

        dataModelDesc.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);

        if (dataModelDesc.isSeekingCCAdvice()) {
            // if it's seeking for advice, it should have thrown exceptions by far
            throw new IllegalStateException("No advice could be provided");
        }

        checkCCNameAmbiguity(dataModelDesc);
        ComputedColumnDesc checkedCC = null;

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {
            checkCCName(cc.getColumnName());

            if (!StringUtils.isEmpty(ccInCheck) && !StringUtils.equalsIgnoreCase(cc.getFullName(), ccInCheck)) {
                checkCascadeErrorOfNestedCC(cc, ccInCheck);
            } else {
                //replace computed columns with basic columns
                ComputedColumnDesc.simpleParserCheck(cc.getExpression(), dataModelDesc.getAliasMap().keySet());
                String innerExpression = KapQueryUtil.massageComputedColumn(dataModelDesc, project, cc,
                        AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
                cc.setInnerExpression(innerExpression);

                //check by data source, this could be slow
                long ts = System.currentTimeMillis();
                ComputedColumnEvalUtil.evaluateExprAndType(dataModelDesc, cc);
                logger.debug("Spent {} ms to visit data source to validate computed column expression: {}",
                        (System.currentTimeMillis() - ts), cc.getExpression());
                checkedCC = cc;
            }
        }

        Preconditions.checkState(checkedCC != null, "No computed column match: {}", ccInCheck);
        // check invalid measure removed due to cc data type change
        val modelManager = getDataModelManager(dataModelDesc.getProject());
        val oldDataModel = modelManager.getDataModelDesc(dataModelDesc.getUuid());

        // brand new model, no measure to remove
        if (oldDataModel == null)
            return getComputedColoumnCheckResponse(checkedCC, new ArrayList<>());

        val copyModel = modelManager.copyForWrite(oldDataModel);
        val request = new ModelRequest(dataModelDesc);
        request.setProject(dataModelDesc.getProject());
        request.setMeasures(dataModelDesc.getAllMeasures());
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
        return getComputedColoumnCheckResponse(checkedCC, measureNames);
    }

    static void checkCCName(String name) {
        if (PushDownConverterKeyWords.CALCITE.contains(name.toUpperCase(Locale.ROOT))
                || PushDownConverterKeyWords.HIVE.contains(name.toUpperCase(Locale.ROOT))) {
            throw new KylinException(INVALID_NAME, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_COMPUTER_COLUMN_NAME_WITH_KEYWORD(), name));
        }
        if (!Pattern.compile("^[a-zA-Z]+\\w*$").matcher(name).matches()) {
            throw new KylinException(INVALID_NAME,
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

        for (TableRef table : model.getAllTables()) {
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
            throw new KylinException(DUPLICATE_COMPUTED_COLUMN_NAME, error.toString());
        }
    }

    void preProcessBeforeModelSave(NDataModel model, String project) {
        model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project, false, true);

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
        ComputedColumnEvalUtil.evaluateExprAndTypeBatchInManual(model, model.getComputedColumnDescs());
    }

    @Transaction(project = 0)
    public void updateModelDataCheckDesc(String project, String modelId, long checkOptions, long faultThreshold,
            long faultActions) {
        aclEvaluate.checkProjectWritePermission(project);
        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelId);
        if (dataModel == null) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }

        dataModel.setDataCheckDesc(DataCheckDesc.valueOf(checkOptions, faultThreshold, faultActions));
        getDataModelManager(project).updateDataModelDesc(dataModel);
    }

    private void cleanIndexPlanWhenNoSegments(String project, String dataFlowId) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        NDataflow dataflow = dataflowManager.getDataflow(dataFlowId);
        if (null == dataflow.getLatestReadySegment()) {
            getIndexPlanManager(project).updateIndexPlan(dataFlowId, copyForWrite -> {
                copyForWrite.getToBeDeletedIndexes().clear();
            });
        }
    }

    @Transaction(project = 1)
    public void deleteSegmentById(String model, String project, String[] ids, boolean force) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (ManagementType.TABLE_ORIENTED == dataModel.getManagementType()) {
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
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
        segmentHelper.removeSegment(project, dataflow.getUuid(), idsToDelete);
        cleanIndexPlanWhenNoSegments(project, dataflow.getUuid());
        offlineModelIfNecessary(dataflowManager, model);
    }

    private void offlineModelIfNecessary(NDataflowManager dfManager, String modelId) {
        val df = dfManager.getDataflow(modelId);
        if (df.getSegments().isEmpty() && RealizationStatusEnum.ONLINE == df.getStatus()) {
            dfManager.updateDataflow(df.getId(), copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.OFFLINE));
        }
    }

    private void checkSegmentsExistById(String modelId, String project, String[] ids) {
        Preconditions.checkNotNull(modelId);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(ids);

        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());

        List<String> notExistIds = Stream.of(ids).filter(segmentId -> null == dataflow.getSegment(segmentId))
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(notExistIds)) {
            throw new KylinException(SEGMENT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_ID_NOT_EXIST(), StringUtils.join(notExistIds, ",")));
        }
    }

    private void checkSegmentsExistByName(String model, String project, String[] names) {
        Preconditions.checkNotNull(model);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(names);

        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());

        List<String> notExistNames = Stream.of(names)
                .filter(segmentName -> null == dataflow.getSegmentByName(segmentName)).filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(notExistNames)) {
            throw new KylinException(SEGMENT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSEGMENT_NAME_NOT_EXIST(), StringUtils.join(notExistNames, ",")));
        }
    }

    private void checkSegmentsStatus(String model, String project, String[] ids,
            SegmentStatusEnumToDisplay... statuses) {
        for (SegmentStatusEnumToDisplay status : statuses) {
            checkSegmentsStatus(model, project, ids, status);
        }
    }

    private void checkSegmentsStatus(String model, String project, String[] ids, SegmentStatusEnumToDisplay status) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Segments<NDataSegment> segments = dataflow.getSegments();
        String message = SegmentStatusEnumToDisplay.LOCKED == status ? MsgPicker.getMsg().getSEGMENT_LOCKED()
                : MsgPicker.getMsg().getSEGMENT_STATUS(status.name());
        for (String id : ids) {
            val segment = dataflow.getSegment(id);
            if (SegmentUtil.getSegmentStatusToDisplay(segments, segment, null) == status) {
                throw new KylinException(PERMISSION_DENIED,
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
                throw new KylinException(FAILED_MERGE_SEGMENT,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_CONTAINS_GAPS(),
                                segmentList.get(i).displayIdName(), segmentList.get(i + 1).displayIdName()));
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
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_MERGE_SEGMENT());
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
    public JobInfoResponse.JobInfo mergeSegmentsManually(MergeSegmentParams params) {
        val startAndEnd = checkMergeSegments(params);

        String project = params.getProject();
        String modelId = params.getModelId();

        val dfManager = getDataflowManager(project);
        val jobManager = getJobManager(project);
        val indexPlan = getIndexPlan(modelId, project);
        val df = dfManager.getDataflow(indexPlan.getUuid());

        NDataSegment mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).mergeSegments(
                df, new SegmentRange.TimePartitionedSegmentRange(startAndEnd.getFirst(), startAndEnd.getSecond()),
                true);

        String jobId = getSourceUsageManager().licenseCheckWrap(project, () -> jobManager
                .mergeSegmentJob(new JobParam(mergeSeg, modelId, getUsername()).withPriority(params.getPriority())));

        return new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), jobId);
    }

    @Transaction(project = 0)
    public List<JobInfoResponse.JobInfo> refreshSegmentById(RefreshSegmentParams params) {

        aclEvaluate.checkProjectOperationPermission(params.getProject());
        checkSegmentsExistById(params.getModelId(), params.getProject(), params.getSegmentIds());
        checkSegmentsStatus(params.getModelId(), params.getProject(), params.getSegmentIds(),
                SegmentStatusEnumToDisplay.LOADING, SegmentStatusEnumToDisplay.REFRESHING,
                SegmentStatusEnumToDisplay.MERGING, SegmentStatusEnumToDisplay.LOCKED);

        List<JobInfoResponse.JobInfo> jobIds = new ArrayList<>();
        NDataflowManager dfMgr = getDataflowManager(params.getProject());
        val jobManager = getJobManager(params.getProject());
        IndexPlan indexPlan = getIndexPlan(params.getModelId(), params.getProject());
        NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());

        for (String id : params.getSegmentIds()) {
            NDataSegment segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSEG_NOT_FOUND(), id, df.getModelAlias()));
            }

            NDataSegment newSeg = dfMgr.refreshSegment(df, segment.getSegRange());

            String jobId = getSourceUsageManager().licenseCheckWrap(params.getProject(),
                    () -> jobManager.refreshSegmentJob(new JobParam(newSeg, params.getModelId(), getUsername())
                            .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                            .withPriority(params.getPriority()), params.isRefreshAllLayouts()));

            jobIds.add(new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_REFRESH.toString(), jobId));
        }
        return jobIds;
    }

    @Transaction(project = 0)
    public void updateDataModelSemantic(String project, ModelRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelRequest(request);
        checkModelPermission(project, request.getUuid());
        validatePartitionDateColumn(request);

        val modelId = request.getUuid();
        val modelManager = getDataModelManager(project);
        val originModel = modelManager.getDataModelDesc(modelId);

        val copyModel = modelManager.copyForWrite(originModel);
        UpdateImpact updateImpact = semanticUpdater.updateModelColumns(copyModel, request, true);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), request.getProject())
                .getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(),
                project);

        preProcessBeforeModelSave(copyModel, project);
        modelManager.updateDataModelDesc(copyModel);
        indexPlanService.updateForMeasureChange(project, modelId, updateImpact.getInvalidMeasures(),
                updateImpact.getReplacedMeasures());
        Set<Integer> affectedSet = updateImpact.getAffectedIds();
        val affectedLayoutSet = getAffectedLayouts(project, modelId, affectedSet);
        if (affectedLayoutSet.size() > 0)
            indexPlanService.reloadLayouts(project, modelId, affectedLayoutSet);

        var newModel = modelManager.getDataModelDesc(modelId);

        checkIndexColumnExist(project, modelId, originModel);

        checkFlatTableSql(newModel);
        semanticUpdater.handleSemanticUpdate(project, modelId, originModel, request.getStart(), request.getEnd(),
                request.isSaveOnly(), affectedLayoutSet.size() > 0);

        updateListeners.forEach(listener -> listener.onUpdate(project, modelId));
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
                throw new KylinException(FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDIMENSION_NOTFOUND(), StringUtils.join(dimensionNames, ",")));
            }

            for (NAggregationGroup agg : rule.getAggregationGroups()) {
                if (!newModel.getEffectiveMeasures().keySet().containsAll(Sets.newHashSet(agg.getMeasures()))) {
                    val measureNames = Arrays.stream(agg.getMeasures())
                            .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                            .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                    throw new KylinException(FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
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
            throw new KylinException(FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
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
                throw new KylinException(FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getDIMENSION_NOTFOUND(), StringUtils.join(dimensionNames, ",")));
            }

            if (!newModel.getEffectiveMeasures().keySet().containsAll(aggIndex.getMeasures())) {
                val measureNames = aggIndex.getMeasures().stream()
                        .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                        .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                throw new KylinException(FAILED_UPDATE_MODEL, String.format(Locale.ROOT,
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
        val modelManager = getDataModelManager(project);
        val origin = modelManager.getDataModelDesc(modelRequest.getId());
        val broken = getBrokenModel(project, origin.getId());
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);

        if (MaintainModelType.AUTO_MAINTAIN == prj.getMaintainModelType()
                || ManagementType.TABLE_ORIENTED == broken.getManagementType()) {
            throw new KylinException(FAILED_UPDATE_MODEL, "Can not repair model manually smart mode!");
        }
        broken.setPartitionDesc(modelRequest.getPartitionDesc());
        broken.setFilterCondition(modelRequest.getFilterCondition());
        broken.setJoinTables(modelRequest.getJoinTables());
        broken.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);
        broken.setBrokenReason(NDataModel.BrokenReason.NULL);
        val format = probeDateFormatIfNotExist(project, broken);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            semanticUpdater.updateModelColumns(broken, modelRequest);
            val model = getDataModelManager(project).updateDataModelDesc(broken);
            saveDateFormatIfNotExist(project, model.getUuid(), format);
            getDataflowManager(project).updateDataflow(broken.getId(),
                    copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.ONLINE));
            return getDataModelManager(project).getDataModelDesc(model.getUuid());
        }, project);
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
                throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getINVALID_SET_TABLE_INC_LOADING(), tableName, modelDesc.getAlias()));
            }
        }
    }

    public List<ModelConfigResponse> getModelConfig(String project, String modelName) {
        aclEvaluate.checkProjectReadPermission(project);
        val responseList = Lists.<ModelConfigResponse> newArrayList();
        getDataflowManager(project).listUnderliningDataModels().stream()
                .filter(model -> StringUtils.isEmpty(modelName) || model.getAlias().contains(modelName))
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
            copyForWrite.setConfigLastModifier(getUsername());
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
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_NULL_VALUE(), "override_props"));
        }
        for (val pair : props.entrySet()) {
            if (Objects.isNull(pair.getValue())) {
                throw new KylinException(INVALID_PARAMETER,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_NULL_VALUE(), pair.getKey()));
            }
        }
        String cores = props.get("kylin.engine.spark-conf.spark.executor.cores");
        String instances = props.get("kylin.engine.spark-conf.spark.executor.instances");
        String pattitions = props.get("kylin.engine.spark-conf.spark.sql.shuffle.partitions");
        String memory = props.get("kylin.engine.spark-conf.spark.executor.memory");
        String baseCuboidAllowed = props.get("kylin.cube.aggrgroup.is-base-cuboid-always-valid");
        if (null != cores && !StringUtil.validateNumber(cores)) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.cores"));
        }
        if (null != instances && !StringUtil.validateNumber(instances)) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.instances"));
        }
        if (null != pattitions && !StringUtil.validateNumber(pattitions)) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.sql.shuffle.partitions"));
        }
        if (null != memory
                && (!memory.endsWith("g") || !StringUtil.validateNumber(memory.substring(0, memory.length() - 1)))) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MEMORY_SIZE(), "spark.executor.memory"));
        }
        if (null != baseCuboidAllowed && !StringUtil.validateBoolean(baseCuboidAllowed)) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT,
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
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_AUTO_MERGE_CONFIG());
        }
        if (null != volatileRange && volatileRange.isVolatileRangeEnabled()
                && (volatileRange.getVolatileRangeNumber() < 0 || null == volatileRange.getVolatileRangeType())) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_VOLATILE_RANGE_CONFIG());
        }
        if (null != retentionRange && retentionRange.isRetentionRangeEnabled()
                && (null == autoMergeEnabled || !autoMergeEnabled || retentionRange.getRetentionRangeNumber() < 0
                        || Collections.max(timeRanges) != retentionRange.getRetentionRangeType())) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_RETENTION_RANGE_CONFIG());
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
        if (pushdownResult.getFirst().compareTo(pushdownResult.getSecond()) > 0) {
            pushdownResult.setSecond(pushdownResult.getFirst());
        }
        return new ExistedDataRangeResponse(pushdownResult.getFirst(), pushdownResult.getSecond());
    }

    @Transaction(project = 1)
    public BuildIndexResponse buildIndicesManually(String modelId, String project, int priority) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (ManagementType.MODEL_BASED != modelDesc.getManagementType()) {
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getCAN_NOT_BUILD_INDICES_MANUALLY(), modelDesc.getAlias()));
        }

        NDataflow df = getDataflowManager(project).getDataflow(modelId);
        val segments = df.getSegments();
        if (segments.isEmpty()) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
        }

        String jobId = getSourceUsageManager().licenseCheckWrap(project, () -> getJobManager(project)
                .addFullIndexJob(new JobParam(modelId, getUsername()).withPriority(priority)));

        return new BuildIndexResponse(StringUtils.isBlank(jobId) //
                ? BuildIndexResponse.BuildIndexType.NO_LAYOUT //
                : BuildIndexResponse.BuildIndexType.NORM_BUILD, jobId);
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

    public ComputedColumnCheckResponse getComputedColoumnCheckResponse(ComputedColumnDesc ccDesc,
            List<String> removedMeasures) {
        val response = new ComputedColumnCheckResponse();
        response.setComputedColumnDesc(ccDesc);
        response.setRemovedMeasures(removedMeasures);
        return response;
    }

    public ModelSaveCheckResponse checkBeforeModelSave(ModelRequest modelRequest) {
        aclEvaluate.checkProjectWritePermission(modelRequest.getProject());
        NDataModel model = semanticUpdater.convertToDataModel(modelRequest);
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
            throw new KylinException(INVALID_FILTER_CONDITION, e);
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
            throw new KylinException(COMPUTED_COLUMN_CASCADE_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getNESTED_CC_CASCADE_ERROR(), cc.getFullName(), ccInCheck, cc.getExpression()));
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
    void massageModelFilterCondition(final NDataModel model) {
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
        return sqlNode
                .toSqlString(new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE)), true)
                .toString();
    }

    public NModelDescResponse getModelDesc(String modelAlias, String project) {
        if (getProjectManager().getProject(project) == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel dataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new KylinException(MODEL_NOT_EXIST,
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

        response.setMeasures(model.getMeasures());
        IndexPlan indexPlan = getIndexPlan(response.getUuid(), project);
        if (indexPlan.getRuleBasedIndex() != null) {
            List<AggGroupResponse> aggGroupResponses = indexPlan.getRuleBasedIndex().getAggregationGroups().stream()
                    .map(x -> new AggGroupResponse(dataModel, x)).collect(Collectors.toList());
            response.setAggregationGroups(aggGroupResponses);
            Set<String> allAggDim = Sets.newHashSet();
            indexPlan.getRuleBasedIndex().getDimensions().stream().map(x -> dataModel.getEffectiveDimensions().get(x))
                    .forEach(x -> allAggDim.add(x.getIdentity()));
            List<NModelDescResponse.Dimension> dims = model.getNamedColumns().stream()
                    .filter(namedColumn -> allAggDim.contains(namedColumn.getAliasDotColumn()))
                    .map(namedColumn -> new NModelDescResponse.Dimension(namedColumn, false))
                    .collect(Collectors.toList());
            response.setDimensions(dims);
        } else {
            response.setAggregationGroups(new ArrayList<>());
            response.setDimensions(new ArrayList<>());
        }
        return response;
    }

    @Transaction(project = 0)
    public void updateDataModelParatitionDesc(String project, String modelAlias,
            ModelParatitionDescRequest modelParatitionDescRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        if (getProjectManager().getProject(project) == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel oldDataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (oldDataModel == null) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        checkModelPermission(project, oldDataModel.getId());

        PartitionDesc partitionDesc = modelParatitionDescRequest.getPartitionDesc();
        if (partitionDesc != null) {
            String rootFactTable = oldDataModel.getRootFactTableName().split("\\.")[1];
            if (!partitionDesc.getPartitionDateColumn().toUpperCase(Locale.ROOT).startsWith(rootFactTable + ".")) {
                throw new KylinException(INVALID_PARTITION_COLUMN, MsgPicker.getMsg().getINVALID_PARTITION_COLUMN());
            }
        }

        getDataModelManager(project).updateDataModel(oldDataModel.getUuid(), copyForWrite -> {
            copyForWrite.setPartitionDesc(modelParatitionDescRequest.getPartitionDesc());
        });
        semanticUpdater.handleSemanticUpdate(project, oldDataModel.getUuid(), oldDataModel,
                modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd());
    }

    @Transaction(project = 0)
    public void updateModelOwner(String project, String modelId, OwnerChangeRequest ownerChangeRequest) {
        try {
            aclEvaluate.checkProjectAdminPermission(project);
            checkTargetOwnerPermission(project, modelId, ownerChangeRequest.getOwner());
        } catch (AccessDeniedException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getMODEL_CHANGE_PERMISSION());
        } catch (IOException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getOWNER_CHANGE_ERROR());
        }

        getDataModelManager(project).updateDataModel(modelId,
                copyForWrite -> copyForWrite.setOwner(ownerChangeRequest.getOwner()));
    }

    private void checkTargetOwnerPermission(String project, String modelId, String owner) throws IOException {
        Set<String> projectManagementUsers = accessService.getProjectManagementUsers(project);
        val model = getDataModelManager(project).getDataModelDesc(modelId);
        if (Objects.isNull(model) || model.isBroken()) {
            throw new KylinException(MODEL_NOT_EXIST,
                    "Model " + modelId + " does not exist or broken in project " + project);
        }
        projectManagementUsers.remove(model.getOwner());

        if (CollectionUtils.isEmpty(projectManagementUsers) || !projectManagementUsers.contains(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED, msg.getMODEL_OWNER_CHANGE_INVALID_USER());
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
                    new KylinException(INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID()));

            for (int i = 0; i < request.getPartitionCols().size(); i++) {
                val partitionCol = request.getPartitionCols().get(i);
                val columnRef = model.findColumn(partitionCol);
                if (!columnRef.equals(multiPartitionDesc.getColumnRefs().get(i))) {
                    throw new KylinException(INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID());
                }
            }

            val partitionValues = request.getValueMapping().stream()
                    .map(pair -> pair.getOrigin().toArray(new String[0])).collect(Collectors.toList());
            for (MultiPartitionDesc.PartitionInfo partitionInfo : multiPartitionDesc.getPartitions()) {
                if (partitionValues.stream().noneMatch(value -> Arrays.equals(value, partitionInfo.getValues()))) {
                    throw new KylinException(INVALID_MULTI_PARTITION_MAPPING_REQUEST,
                            msg.getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID());
                }
            }
        }

        getDataModelManager(project).updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setMultiPartitionKeyMapping(request.convertToMultiPartitionMapping());
        });
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
                throw new KylinException(INVALID_NAME,
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
        for (ComputedColumnDesc computedColumnDesc : model.getComputedColumnDescs()) {
            try {
                // evaluate on a computedcolumn copy to avoid changing the metadata
                ComputedColumnDesc ccCopy = JsonUtil.deepCopy(computedColumnDesc, ComputedColumnDesc.class);
                ComputedColumnEvalUtil.evaluateExprAndType(model, ccCopy);
                if (!matchCCDataType(computedColumnDesc.getDatatype(), ccCopy.getDatatype())) {
                    errorSb.append(String.format(Locale.ROOT, MsgPicker.getMsg().getCheckCCType(),
                            computedColumnDesc.getFullName(), ccCopy.getDatatype(), computedColumnDesc.getDatatype()));
                }
            } catch (Exception e) {
                logger.error("Error validating computed column {}, ", computedColumnDesc, e);
            }
        }

        if (errorSb.length() > 0) {
            throw new KylinException(INVALID_COMPUTED_COLUMN_EXPRESSION, errorSb.toString());
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
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        if (model.isBroken()) {
            throw new KylinException(MODEL_BROKEN,
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

    public BISyncModel exportModel(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port) {
        NDataflow dataflow = getDataflowManager(projectName).getDataflow(modelId);
        if (dataflow.getStatus() == RealizationStatusEnum.BROKEN) {
            throw new KylinException(MODEL_BROKEN, "The model is broken and cannot be exported TDS file");
        }
        checkModelExportPermission(projectName, modelId);
        checkModelPermission(projectName, modelId);

        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setTargetBI(targetBI);
        syncContext.setModelElement(modelElement);
        syncContext.setHost(host);
        syncContext.setPort(port);
        syncContext.setDataflow(getDataflowManager(projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getProjectManager().getProject(projectName).getConfig());

        return BISyncTool.dumpToBISyncModel(syncContext);
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
        Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfExecuteUser, project);
        AclTCRDigest digest = aclManager.getAllUnauthorizedTableColumn(currentUserName, groupsInProject,
                modelTableColumns);
        if (digest.getColumns() != null && !digest.getColumns().isEmpty()) {
            throw new KylinException(UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
        if (digest.getTables() != null && !digest.getTables().isEmpty()) {
            throw new KylinException(UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
    }

    public List<SegmentPartitionResponse> getSegmentPartitions(String project, String modelId, String segmentId,
            List<String> status, String sortBy, boolean reverse) {
        aclEvaluate.checkProjectReadPermission(project);
        val model = getModelById(modelId, project);
        val partitionDesc = model.getMultiPartitionDesc();
        val dataflow = getDataflowManager(project).getDataflow(modelId);
        val segment = dataflow.getSegment(segmentId);
        if (segment == null) {
            throw new KylinException(SEGMENT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_ID_NOT_EXIST(), segmentId));
        }

        Comparator<SegmentPartitionResponse> comparator = propertyComparator(
                StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy, !reverse);

        List<SegmentPartitionResponse> responses = segment.getMultiPartitions().stream().map(partition -> {
            val partitionInfo = partitionDesc.getPartitionInfo(partition.getPartitionId());
            val lastModifiedTime = partition.getLastBuildTime() != 0 ? partition.getLastBuildTime()
                    : partition.getCreateTimeUTC();
            return new SegmentPartitionResponse(partitionInfo.getId(), partitionInfo.getValues(), partition.getStatus(),
                    lastModifiedTime, partition.getSourceCount(), partition.getStorageSize());
        }).filter(partitionResponse -> {
            if (CollectionUtils.isEmpty(status) || status.contains(partitionResponse.getStatus().name())) {
                return true;
            }
            return false;
        }).sorted(comparator).collect(Collectors.toList());

        return responses;
    }

    @Transaction(project = 0)
    public JobInfoResponse buildSegmentPartitionByValue(String project, String modelId, String segmentId,
            List<String[]> partitionValues, boolean parallelBuild) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkSegmentsExistById(modelId, project, new String[] { segmentId });
        checkModelIsMLP(modelId, project);
        val dfm = getDataflowManager(project);
        val df = dfm.getDataflow(modelId);
        val segment = df.getSegment(segmentId);
        val duplicatePartitions = segment.findDuplicatePartitions(partitionValues);
        if (!duplicatePartitions.isEmpty()) {
            val msg = duplicatePartitions.stream().map(partition -> String.join(",", partition))
                    .collect(Collectors.joining("_"));
            throw new KylinException(FAILED_CREATE_JOB,
                    MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_DUPLICATE(msg));
        }
        dfm.appendPartitions(df.getId(), segment.getId(), partitionValues);
        Set<Long> targetPartitions = getDataModelManager(project).getDataModelDesc(modelId).getMultiPartitionDesc()
                .getPartitionIdsByValues(partitionValues);
        return parallelBuildPartition(parallelBuild, project, modelId, segmentId, targetPartitions);
    }

    private JobInfoResponse parallelBuildPartition(boolean parallelBuild, String project, String modelId,
            String segmentId, Set<Long> partitionIds) {
        val jobIds = Lists.<String> newArrayList();
        if (parallelBuild) {
            checkConcurrentSubmit(partitionIds.size());
            partitionIds.forEach(partitionId -> {
                val jobParam = new JobParam(Sets.newHashSet(segmentId), null, modelId, getUsername(),
                        Sets.newHashSet(partitionId), null);
                val jobId = getSourceUsageManager().licenseCheckWrap(project,
                        () -> getJobManager(project).buildPartitionJob(jobParam));
                jobIds.add(jobId);
            });
        } else {
            val jobParam = new JobParam(Sets.newHashSet(segmentId), null, modelId, getUsername(), partitionIds, null);
            val jobId = getSourceUsageManager().licenseCheckWrap(project,
                    () -> getJobManager(project).buildPartitionJob(jobParam));
            jobIds.add(jobId);
        }
        return JobInfoResponse.of(jobIds, JobTypeEnum.SUB_PARTITION_BUILD.toString());
    }

    @Transaction(project = 0)
    public JobInfoResponse refreshSegmentPartition(PartitionsRefreshRequest param, String modelId) {
        val project = param.getProject();
        checkSegmentsExistById(modelId, project, new String[] { param.getSegmentId() });
        checkModelIsMLP(modelId, project);
        val dfm = getDataflowManager(project);
        val df = dfm.getDataflow(modelId);
        val segment = df.getSegment(param.getSegmentId());
        var partitions = param.getPartitionIds();
        aclEvaluate.checkProjectOperationPermission(project);

        if (CollectionUtils.isEmpty(param.getPartitionIds())) {
            partitions = getModelById(modelId, project).getMultiPartitionDesc()
                    .getPartitionIdsByValues(param.getSubPartitionValues());
            if (partitions.isEmpty() || partitions.size() != param.getSubPartitionValues().size()) {
                throw new KylinException(FAILED_CREATE_JOB,
                        MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_ABANDON());
            }
        }

        val oldPartitions = segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                .collect(Collectors.toSet());
        if (!Sets.difference(partitions, oldPartitions).isEmpty()) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_ABANDON());
        }
        val jobManager = getJobManager(project);
        JobParam jobParam = new JobParam(Sets.newHashSet(segment.getId()), null, modelId, getUsername(), partitions,
                null).withIgnoredSnapshotTables(param.getIgnoredSnapshotTables());

        val jobId = getSourceUsageManager().licenseCheckWrap(project, () -> jobManager.refreshSegmentJob(jobParam));
        return JobInfoResponse.of(Lists.newArrayList(jobId), JobTypeEnum.SUB_PARTITION_REFRESH.toString());
    }

    public void deletePartitionsByValues(String project, String segmentId, String modelId,
            List<String[]> subPartitionValues) {
        NDataModel model = checkModelIsMLP(modelId, project);
        Set<Long> ids = model.getMultiPartitionDesc().getPartitionIdsByValues(subPartitionValues);
        deletePartitions(project, segmentId, model.getId(), ids);
    }

    @Transaction(project = 0)
    public void deletePartitions(String project, String segmentId, String modelId, Set<Long> partitions) {
        aclEvaluate.checkProjectOperationPermission(project);
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

    private NDataModel checkModelIsMLP(String modelId, String project) {
        NDataModel model = getModelById(modelId, project);
        if (!model.isMultiPartitionModel()) {
            throw new KylinException(INVALID_MODEL_TYPE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_IS_NOT_MLP(), model.getAlias()));
        }
        return model;
    }

    private void checkConcurrentSubmit(int partitionSize) {
        int runningJobLimit = getConfig().getMaxConcurrentJobLimit();
        int submitJobLimit = runningJobLimit * 5;
        if (partitionSize > submitJobLimit) {
            throw new KylinException(CONCURRENT_SUBMIT_JOB_LIMIT,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getCONCURRENT_SUBMIT_JOB_LIMIT(), submitJobLimit));
        }
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
                throw new KylinException(FAILED_UPDATE_MODEL, MsgPicker.getMsg().getMODEL_MODIFY_ABANDON(table));
            }
        });
        tablesInModel.stream().filter(allAuthTables::contains).forEach(table -> {
            ColumnDesc[] columnDescs = NTableMetadataManager.getInstance(getConfig(), project).getTableDesc(table)
                    .getColumns();
            Arrays.stream(columnDescs).map(column -> table + "." + column.getName()).forEach(column -> {
                if (!allAuthColumns.contains(column)) {
                    throw new KylinException(FAILED_UPDATE_MODEL, MsgPicker.getMsg().getMODEL_MODIFY_ABANDON(column));
                }
            });
        });
    }
}