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
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_COMPUTER_COLUMN_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_DIMENSION_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_JOIN_CONDITION;
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_MEASURE_EXPRESSION;
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_MEASURE_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.DUPLICATE_MODEL_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.EMPTY_MODEL_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.EMPTY_PARTITION_COLUMN;
import static org.apache.kylin.rest.exception.ServerErrorCode.EMPTY_SEGMENT_RANGE;
import static org.apache.kylin.rest.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.rest.exception.ServerErrorCode.FAILED_EXECUTE_MODEL_SQL;
import static org.apache.kylin.rest.exception.ServerErrorCode.FAILED_MERGE_SEGMENT;
import static org.apache.kylin.rest.exception.ServerErrorCode.FAILED_REFRESH_SEGMENT;
import static org.apache.kylin.rest.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_FILTER_CONDITION;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_NAME;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_SEGMENT_RANGE;
import static org.apache.kylin.rest.exception.ServerErrorCode.MODEL_BROKEN;
import static org.apache.kylin.rest.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.rest.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.rest.exception.ServerErrorCode.PROJECT_NOT_EXIST;
import static org.apache.kylin.rest.exception.ServerErrorCode.SEGMENT_NOT_EXIST;
import static org.apache.kylin.rest.exception.ServerErrorCode.SEGMENT_RANGE_OVERLAP;
import static org.apache.kylin.rest.exception.ServerErrorCode.TABLE_NOT_EXIST;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.ColumnDesc;
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
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.util.AddTableNameSqlVisitor;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.acl.NDataModelAclParams;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelDropAddListener;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggGroupResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelInfoResponse;
import io.kyligence.kap.rest.response.NCubeDescResponse;
import io.kyligence.kap.rest.response.NDataModelOldParams;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.NRecomendationListResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.response.SegmentRangeResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.rest.util.ModelUtils;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiAutoContext;
import io.kyligence.kap.smart.ModelCreateContextOfSemiMode;
import io.kyligence.kap.smart.ModelReuseContextOfSemiMode;
import io.kyligence.kap.smart.ModelSelectContextOfSemiMode;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final String SEGMENT_PATH = "segment_path";

    private static final String FILE_COUNT = "file_count";

    private static final String LAST_MODIFY = "last_modify";

    public static final String VALID_NAME_FOR_MODEL_DIMENSION_MEASURE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";

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

    private NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(MODEL_NOT_EXIST, String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }
        return nDataModel;
    }

    /**
     * for 3x rest api
     * @param modelList
     * @return
     */
    public List<NDataModel> addOldParams(List<NDataModel> modelList) {
        modelList.forEach(model -> {
            NDataModelOldParams oldParams = new NDataModelOldParams();
            oldParams.setName(model.getAlias());
            oldParams.setJoinTables(model.getJoinTables());

            if (model instanceof NDataModelResponse) {
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
    public Boolean isProjectNotExist(String project) {
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(project, true);
        return CollectionUtils.isEmpty(projectInstances);
    }

    /**
     * for 3x rest api
     * @param modelAlias
     * @return
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
            List<AggGroupResponse> aggGroupResponses) {
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
        cube.getNamedColumns().stream().forEach(namedColumn -> {
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            String table = aliasDotColumn.split("\\.")[0];
            String column = aliasDotColumn.split("\\.")[1];
            if (!tableToAllDim.containsKey(table)) {
                tableToAllDim.put(table, new ArrayList());
            }
            tableToAllDim.get(table).add(column);
        });

        Set<String> allAggDim = Sets.newHashSet();//table.col
        indexPlan.getRuleBasedIndex().getDimensions().stream().map(x -> indexPlan.getEffectiveDimCols().get(x))
                .forEach(x -> allAggDim.add(x.getIdentity()));

        List<Set<String>> aggIncludes = new ArrayList<>();
        aggGroupResponses.stream().forEach(agg -> aggIncludes.add(Sets.newHashSet(agg.getIncludes())));

        cube.getNamedColumns().stream().forEach(namedColumn -> {
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
                    String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        NDataModelResponse cube = new NDataModelResponse(dataModel);
        NCubeDescResponse result = new NCubeDescResponse();

        result.setUuid(cube.getUuid());
        result.setName(cube.getAlias());
        result.setMeasures(
                cube.getMeasures().stream().map(x -> new NCubeDescResponse.Measure3X(x)).collect(Collectors.toList()));

        IndexPlan indexPlan = getIndexPlan(result.getUuid(), projectName);
        if (!dataModel.isBroken() && indexPlan.getRuleBasedIndex() != null) {
            List<AggGroupResponse> aggGroupResponses = indexPlan.getRuleBasedIndex().getAggregationGroups().stream()
                    .map(x -> new AggGroupResponse(indexPlan, x)).collect(Collectors.toList());
            result.setAggregationGroups(aggGroupResponses);
            result.setDimensions(getDimension3XES(indexPlan, cube, aggGroupResponses));
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
                        "" + (Long.MAX_VALUE - 1), LAST_MODIFY, true, null);
                for (NDataSegmentResponse segment : segments) {
                    Long sourceRows = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getSourceRows)
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

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, List<String> status, String sortBy, boolean reverse, String modelAliasOrOwner,
            Long lastModifyFrom, Long lastModifyTo) {
        aclEvaluate.checkProjectReadPermission(projectName);
        List<Pair<NDataflow, NDataModel>> pairs = getFirstMatchModels(modelAlias, projectName, exactMatch, owner,
                modelAliasOrOwner, lastModifyFrom, lastModifyTo);
        val dfManager = getDataflowManager(projectName);
        val optRecomManager = getOptRecommendationManager(projectName);
        List<NDataModelResponse> filterModels = new ArrayList<>();
        pairs.forEach(p -> {
            val dataflow = p.getKey();
            val modelDesc = p.getValue();
            ModelStatusToDisplayEnum modelResponseStatus = convertModelStatusToDisplay(modelDesc, projectName);
            boolean isModelStatusMatch = isListContains(status, modelResponseStatus);
            if (isModelStatusMatch) {
                NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
                nDataModelResponse.setModelBroken(modelDesc.isBroken());
                nDataModelResponse.setStatus(modelResponseStatus);
                nDataModelResponse.setStorage(dfManager.getDataflowStorageSize(modelDesc.getUuid()));
                nDataModelResponse.setSource(dfManager.getDataflowSourceSize(modelDesc.getUuid()));
                nDataModelResponse.setSegmentHoles(dfManager.calculateSegHoles(modelDesc.getUuid()));
                nDataModelResponse.setExpansionrate(ModelUtils.computeExpansionRate(nDataModelResponse.getStorage(),
                        nDataModelResponse.getSource()));
                nDataModelResponse.setUsage(dataflow.getQueryHitCount());
                nDataModelResponse.setRecommendationsCount(optRecomManager.getRecommendationCount(modelDesc.getId()));
                nDataModelResponse
                        .setAvailableIndexesCount(modelResponseStatus.equals(ModelStatusToDisplayEnum.BROKEN) ? 0
                                : getAvailableIndexesCount(projectName, modelDesc.getId()));
                nDataModelResponse.setTotalIndexes(modelDesc.isBroken() ? 0
                        : getIndexPlan(modelDesc.getUuid(), modelDesc.getProject()).getAllLayouts().size());
                nDataModelResponse.setEmptyIndexesCount(modelResponseStatus.equals(ModelStatusToDisplayEnum.BROKEN) ? 0
                        : getEmptyIndexesCount(projectName, modelDesc.getId()));
                filterModels.add(nDataModelResponse);
            }
        });
        if ("expansionrate".equalsIgnoreCase(sortBy)) {
            return sortExpansionRate(reverse, filterModels);
        } else {
            Comparator<NDataModelResponse> comparator = propertyComparator(
                    StringUtils.isEmpty(sortBy) ? LAST_MODIFY : sortBy, !reverse);
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

    private ModelStatusToDisplayEnum convertModelStatusToDisplay(NDataModel modelDesc, final String projectName) {
        RealizationStatusEnum modelStatus = modelDesc.isBroken() ? RealizationStatusEnum.BROKEN
                : getModelStatus(modelDesc.getUuid(), projectName);
        ModelStatusToDisplayEnum modelResponseStatus = ModelStatusToDisplayEnum.convert(modelStatus);
        val segmentHoles = getDataflowManager(projectName).calculateSegHoles(modelDesc.getUuid());
        if (modelResponseStatus == ModelStatusToDisplayEnum.ONLINE
                && (getEmptyIndexesCount(projectName, modelDesc.getId()) > 0
                        || CollectionUtils.isNotEmpty(segmentHoles))) {
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
                || (!exactMatch && originValue.toLowerCase().contains(valueToMatch.toLowerCase()));
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

    protected RealizationStatusEnum getModelStatus(String modelId, String projectName) {
        val indexPlan = getIndexPlan(modelId, projectName);
        if (indexPlan != null) {
            return getDataflowManager(projectName).getDataflow(indexPlan.getUuid()).getStatus();
        } else {
            return null;
        }
    }

    public List<NDataSegmentResponse> getSegmentsResponse(String modelId, String project, String start, String end,
            String sortBy, boolean reverse, String status) {
        aclEvaluate.checkProjectReadPermission(project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        List<NDataSegmentResponse> segmentResponse = Lists.newArrayList();
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        val segs = getSegmentsByRange(modelId, project, start, end);
        for (NDataSegment segment : segs) {
            if (StringUtils.isNotEmpty(status)
                    && !status.equalsIgnoreCase(segs.getSegmentStatusToDisplay(segment).toString())) {
                continue;
            }
            NDataSegmentResponse nDataSegmentResponse = new NDataSegmentResponse(segment);
            nDataSegmentResponse.setBytesSize(segment.getStorageBytesSize());
            nDataSegmentResponse.getAdditionalInfo().put(SEGMENT_PATH, dataflow.getSegmentHdfsPath(segment.getId()));
            nDataSegmentResponse.getAdditionalInfo().put(FILE_COUNT, segment.getStorageFileCount() + "");
            nDataSegmentResponse.setStatusToDisplay(dataflow.getSegments().getSegmentStatusToDisplay(segment));
            nDataSegmentResponse.setSourceBytesSize(segment.getSourceBytesSize());
            nDataSegmentResponse.setLastBuildTime(segment.getLastBuildTime());
            nDataSegmentResponse.setSegDetails(segment.getSegDetails());
            segmentResponse.add(nDataSegmentResponse);
        }
        Comparator<NDataSegmentResponse> comparator = propertyComparator(
                StringUtils.isEmpty(sortBy) ? "create_time" : sortBy, reverse);
        segmentResponse.sort(comparator);
        return segmentResponse;
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

    public List<NSpanningTreeForWeb> getModelRelations(String modelId, String project) {
        aclEvaluate.checkProjectReadPermission(project);
        val indexPlan = getIndexPlan(modelId, project);
        List<NSpanningTreeForWeb> result = new ArrayList<>();
        val allLayouts = Lists.<LayoutEntity> newArrayList();
        if (indexPlan.getRuleBasedIndex() != null) {
            val rule = indexPlan.getRuleBasedIndex();
            allLayouts.addAll(rule.genCuboidLayouts());
        }
        val autoLayouts = indexPlan.getWhitelistLayouts().stream()
                .filter(layout -> layout.getId() < IndexEntity.TABLE_INDEX_START_ID).collect(Collectors.toList());
        allLayouts.addAll(autoLayouts);
        val tree = NSpanningTreeFactory.forWebDisplay(allLayouts, indexPlan);
        result.add(tree);
        return result;
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
        if (!checkModelAliasUniqueness(modelId, newAlias, project)) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(MsgPicker.getMsg().getMODEL_ALIAS_DUPLICATED(), newAlias));
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
        dropModel(modelId, project, false);
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> ModelDropAddListener.onDelete(project, modelId));
    }

    void dropModel(String modelId, String project, boolean ignoreType) {
        val projectInstance = getProjectManager().getProject(project);
        if (!ignoreType) {
            Preconditions.checkState(MaintainModelType.MANUAL_MAINTAIN.equals(projectInstance.getMaintainModelType()));
        }

        NDataModel dataModelDesc = getModelById(modelId, project);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val recommendationManager = OptimizeRecommendationManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        recommendationManager.dropOptimizeRecommendation(modelId);
        dataflowManager.dropDataflow(modelId);
        indexPlanManager.dropIndexPlan(modelId);
        dataModelManager.dropModel(dataModelDesc);
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
        eventDao.getEventsByModel(modelId).stream().map(Event::getId).forEach(eventDao::deleteEvent);
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

    }

    @Transaction(project = 1)
    public void purgeModelManually(String modelId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModelDesc = getModelById(modelId, project);
        if (dataModelDesc.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getMODEL_CAN_NOT_PURGE(), dataModelDesc.getAlias()));
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
        changeModelOwner(nDataModel);
        val newModel = dataModelManager.createDataModelDesc(nDataModel, nDataModel.getOwner());
        cloneIndexPlan(modelId, project, nDataModel.getOwner(), newModel.getUuid());
    }

    private void cloneIndexPlan(String modelId, String project, String owner, String newModelId) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan copy = indexPlanManager.copy(indexPlan);
        copy.setUuid(newModelId);
        copy.setLastModified(0L);
        copy.setMvcc(-1);
        indexPlanManager.createIndexPlan(copy);
        NDataflow nDataflow = new NDataflow();
        nDataflow.setStatus(RealizationStatusEnum.ONLINE);
        dataflowManager.createDataflow(copy, owner);
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
            throw new KylinException(DUPLICATE_JOIN_CONDITION,
                    String.format(MsgPicker.getMsg().getBROKEN_MODEL_CANNOT_ONOFFLINE(), modelId));
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelId, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelId).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    public boolean isModelsUsingTable(String table, String project) {
        return getDataflowManager(project).getModelsUsingTable(getTableManager(project).getTableDesc(table)).size() > 0;
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
                relatedModelResponse -> relatedModelResponse.getManagementType().equals(ManagementType.TABLE_ORIENTED))
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
            checkRefreshRangeWithinCoveredRange(loadingRange, project, table, toBeRefreshSegmentRange);
        }

        Segments<NDataSegment> affetedSegments = new Segments<NDataSegment>();

        for (NDataModel model : models) {
            val dataflow = dfManager.getDataflow(model.getId());
            Segments<NDataSegment> segments = getSegmentsByRange(model.getUuid(), project, start, end);
            if (!dataflow.getStatus().equals(RealizationStatusEnum.LAG_BEHIND)) {
                if (CollectionUtils.isEmpty(segments.getSegments(SegmentStatusEnum.READY))) {
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
            affetedSegments.addAll(segments);
        }
        Preconditions.checkState(CollectionUtils.isNotEmpty(affetedSegments));
        Collections.sort(affetedSegments);
        String affectedStart = affetedSegments.getFirstSegment().getSegRange().getStart().toString();
        String affectedEnd = affetedSegments.getLastSegment().getSegRange().getEnd().toString();
        for (NDataSegment segment : affetedSegments) {
            byteSize += segment.getStorageBytesSize();
        }
        return new RefreshAffectedSegmentsResponse(byteSize, affectedStart, affectedEnd);

    }

    private void checkSegRefreshingInLagBehindModel(Segments<NDataSegment> segments) {
        for (val seg : segments) {
            if (segments.getSegmentStatusToDisplay(seg).equals(SegmentStatusEnumToDisplay.REFRESHING)) {
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
            }
        }
    }

    private void checkRefreshRangeWithinCoveredRange(NDataLoadingRange dataLoadingRange, String project, String table,
            SegmentRange toBeRefreshSegmentRange) {
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new KylinException(INVALID_SEGMENT_RANGE, String.format(MsgPicker.getMsg().getSEGMENT_INVALID_RANGE(),
                    toBeRefreshSegmentRange, coveredReadySegmentRange));
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
            SparkSession ss = SparderEnv.getSparkSession();
            String flatTableSql = JoinedFlatTable.generateSelectDataStatement(dataModel, false);
            String pushdownSql = QueryUtil.massagePushDownSql(flatTableSql, dataModel.getProject(), "default", false);
            ss.sql(pushdownSql);
        } catch (Exception e) {
            Pattern pattern = Pattern.compile("cannot resolve '(.*?)' given input columns");
            Matcher matcher = pattern.matcher(e.getMessage().replaceAll("`", ""));
            if (matcher.find()) {
                String column = matcher.group(1);
                String table = column.contains(".") ? column.split("\\.")[0] : dataModel.getRootFactTableName();
                String error = String.format(MsgPicker.getMsg().getTABLENOTFOUND(), dataModel.getAlias(), column,
                        table);
                throw new KylinException(TABLE_NOT_EXIST, error);
            } else {
                String errorMsg = String.format("model [%s], %s", dataModel.getAlias(), String.format(
                        MsgPicker.getMsg().getDEFAULT_REASON(), null != e.getMessage() ? e.getMessage() : "null"));
                throw new KylinException(FAILED_EXECUTE_MODEL_SQL, errorMsg);
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

    public void batchCreateModel(String project, List<ModelRequest> modelRequests) {
        aclEvaluate.checkProjectWritePermission(project);
        checkDuplicateAliasInModelRequests(modelRequests);
        for (ModelRequest modelRequest : modelRequests) {
            validatePartitionDateColumn(modelRequest);

            modelRequest.setProject(project);
            modelRequest.setSimplifiedDimensions(modelRequest.getDimensions());

            List<NDataModel.Measure> allMeasures = modelRequest.getAllMeasures();
            List<SimplifiedMeasure> simplifiedMeasures = Lists.newArrayList();
            for (NDataModel.Measure measure : allMeasures) {
                SimplifiedMeasure simplifiedMeasure = SimplifiedMeasure.fromMeasure(measure);
                simplifiedMeasures.add(simplifiedMeasure);
            }
            modelRequest.setSimplifiedMeasures(simplifiedMeasures);
            doCheckBeforeModelSave(project, modelRequest);
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (ModelRequest modelRequest : modelRequests) {
                if (modelRequest.getIndexPlan() != null) {
                    saveModelAndIndexInMem(modelRequest, modelRequest.getIndexPlan(), modelRequest.getProject());
                }
            }
            return null;
        }, project);
    }

    public NDataModel createModel(String project, ModelRequest modelRequest) throws Exception {
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
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }

        AbstractContext proposeContext = new ModelSelectContextOfSemiMode(KylinConfig.getInstanceFromEnv(), project,
                sqls.toArray(new String[0]));
        NSmartMaster smartMaster = new NSmartMaster(proposeContext);
        smartMaster.executePropose();
        return smartMaster.getRecommendedModels();
    }

    public boolean couldAnsweredByExistedModel(String project, List<String> sqls) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return true;
        }

        return CollectionUtils.isNotEmpty(couldAnsweredByExistedModels(project, sqls));
    }

    public NRecomendationListResponse suggestModel(String project, List<String> sqls, boolean reuseExistedModel) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return null;
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        AbstractSemiAutoContext proposeContext = reuseExistedModel
                ? new ModelReuseContextOfSemiMode(kylinConfig, project, sqls.toArray(new String[0]), true)
                : new ModelCreateContextOfSemiMode(kylinConfig, project, sqls.toArray(new String[0]));
        NSmartMaster smartMaster = new NSmartMaster(proposeContext);
        smartMaster.runSuggestModel();
        return constructModelRecommendListResponse(proposeContext.getRecommendationMap(), smartMaster.getContext());
    }

    private NRecomendationListResponse constructModelRecommendListResponse(
            Map<NDataModel, OptimizeRecommendation> recommendationsMap, AbstractContext context) {
        List<NRecomendationListResponse.NRecomendedDataModelResponse> newDataModelResponseList = Lists.newArrayList();
        List<NRecomendationListResponse.NRecomendedDataModelResponse> originDataModelResponseList = Lists
                .newArrayList();

        context.getModelContexts().stream().filter(modelContext -> !modelContext.isTargetModelMissing())
                .collect(groupingBy(modelContext -> modelContext.getTargetModel().getUuid()))
                .forEach((modelId, modelContextList) -> {

                    constructModelRecommendResponse(modelContextList, recommendationsMap, originDataModelResponseList,
                            newDataModelResponseList);

                });
        return new NRecomendationListResponse(originDataModelResponseList, newDataModelResponseList);
    }

    private void constructModelRecommendResponse(List<AbstractContext.NModelContext> modelContextList,
            Map<NDataModel, OptimizeRecommendation> recommendationsMap,
            List<NRecomendationListResponse.NRecomendedDataModelResponse> originModelList,
            List<NRecomendationListResponse.NRecomendedDataModelResponse> newModelList) {

        val sqls = modelContextList.stream()
                .flatMap(modelContext -> modelContext.getModelTree().getOlapContexts().stream())
                .map(olapContext -> olapContext.sql).collect(Collectors.toSet());
        val modelContext = modelContextList.stream().filter(modelCtx -> !modelCtx.isSnapshotSelected()).findAny()
                .orElse(modelContextList.get(0));

        val response = new NRecomendationListResponse.NRecomendedDataModelResponse(modelContext.getTargetModel());
        response.setDimensions(modelContext.getTargetModel().getAllNamedColumns().stream()
                .filter(dim -> dim.isDimension()).collect(Collectors.toList()));
        response.setSqls(Lists.newArrayList(sqls));

        if (recommendationsMap.get(modelContext.getTargetModel()) != null) {
            response.setRecommendationResponse(
                    new OptRecommendationResponse(recommendationsMap.get(modelContext.getTargetModel()), null));
        } else {
            // build Agg && Table index
            val indexPlan = modelContext.getTargetIndexPlan();
            response.setIndices(indexPlan);
        }

        if (modelContext.getOriginModel() == null) {
            newModelList.add(response);
        } else {
            originModelList.add(response);
        }
    }

    private NDataModel doCheckBeforeModelSave(String project, ModelRequest modelRequest) {

        if (modelRequest.getRecommendation() == null) {
            // new model
            checkAliasExist(modelRequest.getUuid(), modelRequest.getAlias(), project);
        }
        modelRequest.setOwner(AclPermissionUtil.getCurrentUsername());
        checkModelRequest(modelRequest);

        //remove some attributes in modelResponse to fit NDataModel
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);
        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        if (prj.getMaintainModelType().equals(MaintainModelType.AUTO_MAINTAIN)
                || dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new KylinException(FAILED_CREATE_MODEL, MsgPicker.getMsg().getINVALID_CREATE_MODEL());
        }

        preProcessBeforeModelSave(dataModel, project);
        checkFlatTableSql(dataModel);
        return dataModel;
    }

    private void saveModelAndIndexInMem(NDataModel model, IndexPlan indexPlan, String project) {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
            dataModelManager.updateDataModelDesc(model);
        } else {
            dataModelManager.createDataModelDesc(model, model.getOwner());
        }
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val emptyIndex = new IndexPlan();
        emptyIndex.setUuid(model.getUuid());
        indexPlanManager.createIndexPlan(emptyIndex);
        dataflowManager.createDataflow(emptyIndex, model.getOwner());

        updateIndexPlan(project, indexPlan);
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> ModelDropAddListener.onAdd(project, model.getId(), model.getAlias()));
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
        indexPlanManager.createIndexPlan(indexPlan);
        val df = dataflowManager.createDataflow(indexPlan, model.getOwner());
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
        indexPlanManager.updateIndexPlan(indexPlan.getId(), (copyForWrite) -> {
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

    private void checkModelDimensions(ModelRequest request) {
        Set<String> dimensionNames = new HashSet<>();

        for (NDataModel.NamedColumn dimension : request.getSimplifiedDimensions()) {
            // check if the dimension name is valid
            if (!StringUtils.containsOnly(dimension.getName(), VALID_NAME_FOR_MODEL_DIMENSION_MEASURE))
                throw new KylinException(INVALID_NAME,
                        String.format(MsgPicker.getMsg().getINVALID_DIMENSION_NAME(), dimension.getName()));

            // check duplicate dimension names
            if (dimensionNames.contains(dimension.getName()))
                throw new KylinException(DUPLICATE_DIMENSION_NAME,
                        String.format(MsgPicker.getMsg().getDUPLICATE_DIMENSION_NAME(), dimension.getName()));

            dimensionNames.add(dimension.getName());
        }
    }

    private void checkModelMeasures(ModelRequest request) {
        Set<String> measureNames = new HashSet<>();
        Set<SimplifiedMeasure> measures = new HashSet<>();

        for (SimplifiedMeasure measure : request.getSimplifiedMeasures()) {
            // check if the measure name is valid
            if (!StringUtils.containsOnly(measure.getName(), VALID_NAME_FOR_MODEL_DIMENSION_MEASURE))
                throw new KylinException(INVALID_NAME,
                        String.format(MsgPicker.getMsg().getINVALID_MEASURE_NAME(), measure.getName()));

            // check duplicate measure names
            if (measureNames.contains(measure.getName()))
                throw new KylinException(DUPLICATE_MEASURE_NAME,
                        String.format(MsgPicker.getMsg().getDUPLICATE_MEASURE_NAME(), measure.getName()));

            // check duplicate measure definitions
            if (measures.contains(measure))
                throw new KylinException(DUPLICATE_MEASURE_EXPRESSION,
                        String.format(MsgPicker.getMsg().getDUPLICATE_MEASURE_DEFINITION(), measure.getName()));

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
                    throw new KylinException(DUPLICATE_JOIN_CONDITION, String
                            .format(MsgPicker.getMsg().getDUPLICATE_JOIN_CONDITIONS(), primaryKeys[i], foreignKey[i]));

                joinKeys.add(Pair.newPair(primaryKeys[i], foreignKey[i]));
            }
        }
    }

    @Deprecated
    private void proposeAndSaveDateFormatIfNotExist(String project, String modelId) throws Exception {
        val modelManager = getDataModelManager(project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        val partitionDesc = modelDesc.getPartitionDesc();
        if (partitionDesc == null || StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
                || StringUtils.isNotEmpty(partitionDesc.getPartitionDateFormat()))
            return;

        if (StringUtils.isNotEmpty(partitionDesc.getPartitionDateColumn())
                && StringUtils.isNotEmpty(partitionDesc.getPartitionDateFormat())) {
            return;
        }

        String partitionColumn = modelDesc.getPartitionDesc().getPartitionDateColumnRef().getExpressionInSourceDB();

        val date = PushDownUtil.getFormatIfNotExist(modelDesc.getRootFactTableName(), partitionColumn, project);
        val format = DateFormat.proposeDateFormat(date);
        modelManager.updateDataModel(modelId, model -> {
            model.getPartitionDesc().setPartitionDateFormat(format);
        });
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

    private void saveDateFormatIfNotExist(String project, String modelId, String format) throws Exception {
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
        val modelManager = getDataModelManager(project);

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
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getMODEL_SEGMENT_CAN_NOT_REMOVE(), dataModel.getAlias()));
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsExist(model, project, ids);
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

    public JobInfoResponse fixSegmentHoles(String project, String modelId, List<SegmentTimeRequest> segmentHoles)
            throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        checkModelAndIndexManually(project, modelId);
        String format = probeDateFormatIfNotExist(project, modelDesc);
        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            List<JobInfoResponse.JobInfo> jobInfos = Lists.newArrayList();
            for (SegmentTimeRequest hole : segmentHoles) {
                jobInfos.add(constructIncrementBuild(project, modelId, hole.getStart(), hole.getEnd(), format));
            }
            return jobInfos;
        }, project);

        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;

    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end)
            throws Exception {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (modelDesc.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateColumn())) {
            return fullBuildSegmentsManually(project, modelId);
        } else {
            return incrementBuildSegmentsManually(project, modelId, start, end, modelDesc.getPartitionDesc(),
                    Lists.newArrayList());
        }
    }

    public JobInfoResponse fullBuildSegmentsManually(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(() -> constructFullBuild(project, modelId), project);
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> constructFullBuild(String project, String modelId) {
        checkModelAndIndexManually(project, modelId);
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
                    SegmentRange.TimePartitionedSegmentRange.createInfinite());
            return Lists.newArrayList(new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(),
                    getEventManager(project).postAddSegmentEvents(newSegment, modelId, getUsername())));
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(2);
        res.addAll(refreshSegmentById(modelId, project,
                Lists.newArrayList(getDataflowManager(project).getDataflow(modelId).getSegments().get(0).getId())
                        .toArray(new String[0])));
        res.add(new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                getEventManager(project).postAddCuboidEvents(modelId, getUsername())));
        return res;
    }

    private JobInfoResponse.JobInfo constructIncrementBuild(String project, String modelId, String start, String end,
            String partionColFormat) throws Exception {
        NDataModel modelDescInTransaction = getDataModelManager(project).getDataModelDesc(modelId);
        EventManager eventManager = getEventManager(project);
        TableDesc table = getTableManager(project).getTableDesc(modelDescInTransaction.getRootFactTableName());
        val df = getDataflowManager(project).getDataflow(modelId);
        if (modelDescInTransaction.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDescInTransaction.getPartitionDesc().getPartitionDateColumn())) {
            throw new IllegalArgumentException("Can not add a new segment on full build model.");
        }
        Preconditions.checkArgument(!PushDownUtil.needPushdown(start, end), "Load data must set start and end date");
        val segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);
        checkSegmentToBuildOverlapsBuilt(project, modelId, segmentRangeToBuild);
        saveDateFormatIfNotExist(project, modelId, partionColFormat);
        NDataSegment newSegment = getDataflowManager(project).appendSegment(df, segmentRangeToBuild);
        return new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(),
                eventManager.postAddSegmentEvents(newSegment, modelId, getUsername()));
    }

    public JobInfoResponse incrementBuildSegmentsManually(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, List<SegmentTimeRequest> segmentHoles) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
        val modelManager = getDataModelManager(project);
        if (partitionDesc == null || StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())) {
            throw new KylinException(EMPTY_PARTITION_COLUMN, "Partition column is null.'");
        }

        String startFormat = DateFormat.getFormatTimeStamp(start, partitionDesc.getPartitionDateFormat()).toString();
        String endFormat = DateFormat.getFormatTimeStamp(end, partitionDesc.getPartitionDateFormat()).toString();

        NDataModel copyModel = modelManager.copyForWrite(modelManager.getDataModelDesc(modelId));
        copyModel.setPartitionDesc(partitionDesc);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), project).getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(),
                project);
        String format = probeDateFormatIfNotExist(project, copyModel);
        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(() -> innerIncrementBuild(project, modelId, startFormat, endFormat,
                        partitionDesc, format, segmentHoles), project);
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> innerIncrementBuild(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, String format, List<SegmentTimeRequest> segmentHoles) throws Exception {
        checkModelAndIndexManually(project, modelId);
        if (CollectionUtils.isEmpty(segmentHoles)) {
            segmentHoles = Lists.newArrayList();
        }
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (modelDesc.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDesc.getPartitionDesc().getPartitionDateColumn())
                || !modelDesc.getPartitionDesc().equals(partitionDesc)) {
            aclEvaluate.checkProjectWritePermission(project);
            val request = convertToRequest(modelDesc);
            request.setPartitionDesc(partitionDesc);
            request.setProject(project);
            updateDataModelSemantic(project, request);
            segmentHoles.clear();
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(segmentHoles.size() + 2);
        for (SegmentTimeRequest hole : segmentHoles) {
            res.add(constructIncrementBuild(project, modelId, hole.getStart(), hole.getEnd(), format));
        }
        res.add(constructIncrementBuild(project, modelId, start, end, format));
        res.add(new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                getEventManager(project).postAddCuboidEvents(modelId, getUsername())));
        return res;
    }

    ModelRequest convertToRequest(NDataModel modelDesc) throws IOException {
        val request = new ModelRequest(JsonUtil.deepCopy(modelDesc, NDataModel.class));
        request.setSimplifiedMeasures(modelDesc.getEffectiveMeasures().values().stream()
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(modelDesc.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toList()));
        request.setComputedColumnDescs(modelDesc.getComputedColumnDescs());
        return request;
    }

    @Transaction(project = 0)
    public void updatePartitionColumn(String project, String modelId, PartitionDesc partitionDesc) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        val dataflowManager = getDataflowManager(project);
        val df = dataflowManager.getDataflow(modelId);
        val model = df.getModel();
        if (partitionDesc == null && model.getPartitionDesc() == null && df.getFirstSegment() == null) {
            dataflowManager.fillDfManually(df,
                    Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
        }
        if (!Objects.equals(partitionDesc, model.getPartitionDesc())) {
            val request = convertToRequest(model);
            request.setProject(project);
            request.setPartitionDesc(partitionDesc);
            updateDataModelSemantic(project, request);
        }
    }

    private void checkModelAndIndexManually(String project, String modelId) {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (!modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT_MANUALLY(), modelDesc.getAlias()));
        }

        val indexPlan = getIndexPlan(modelId, project);
        if (indexPlan == null || indexPlan.getAllLayouts().isEmpty()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getCAN_NOT_BUILD_SEGMENT());
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
        if (CollectionUtils.isEmpty(segments)) {
            return;
        } else {
            for (NDataSegment existedSegment : segments) {
                if (existedSegment.getSegRange().overlaps(segmentRangeToBuild)) {
                    throw new KylinException(SEGMENT_RANGE_OVERLAP,
                            String.format(MsgPicker.getMsg().getSEGMENT_RANGE_OVERLAP(),
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
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new KylinException(INVALID_MODEL_NAME, String.format(msg.getINVALID_MODEL_NAME(), modelAlias));
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
     * check if the computed column expressions are valid ( in hive)
     * <p>
     * ccInCheck is optional, if provided, other cc in the model will skip hive check
     */
    public ComputedColumnDesc checkComputedColumn(final NDataModel dataModelDesc, String project, String ccInCheck) {
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

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {
            checkCCName(cc.getColumnName());

            if (!StringUtils.isEmpty(ccInCheck) && !StringUtils.equalsIgnoreCase(cc.getFullName(), ccInCheck))
                continue;

            //replace computed columns with basic columns
            ComputedColumnDesc.simpleParserCheck(cc.getExpression(), dataModelDesc.getAliasMap().keySet());
            String innerExpression = KapQueryUtil.massageComputedColumn(dataModelDesc, project, cc);
            cc.setInnerExpression(innerExpression);

            //check by data source, this could be slow
            long ts = System.currentTimeMillis();
            ComputedColumnEvalUtil.evaluateExprAndType(dataModelDesc, cc);
            logger.debug("Spent {} ms to visit data source to validate computed column expression: {}",
                    (System.currentTimeMillis() - ts), cc.getExpression());
            return cc;
        }
        throw new IllegalStateException("No computed column match: " + ccInCheck);
    }

    static void checkCCName(String name) {
        if (PushDownConverterKeyWords.CALCITE.contains(name.toUpperCase())
                || PushDownConverterKeyWords.HIVE.contains(name.toUpperCase())) {
            throw new KylinException(INVALID_NAME,
                    String.format(MsgPicker.getMsg().getINVALID_COMPUTER_COLUMN_NAME(), name));
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
                if (!tblColRef.getColumnDesc().isComputedColumn()) {
                    if (ccColumnNames.contains(tblColRef.getName())) {
                        ambiguousCCNameSet.add(tblColRef.getName());
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(ambiguousCCNameSet)) {
            StringBuilder error = new StringBuilder();
            ambiguousCCNameSet.forEach(name -> {
                error.append(String.format(MsgPicker.getMsg().getCHECK_CC_AMBIGUITY(), name));
                error.append("\r\n");
            });
            throw new KylinException(DUPLICATE_COMPUTER_COLUMN_NAME, error.toString());
        }
    }

    public void preProcessBeforeModelSave(NDataModel model, String project) {
        model.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);

        String originFilterCondition = model.getFilterCondition();
        if (StringUtils.isNotEmpty(originFilterCondition)) {
            String newFilterCondition = KapQueryUtil.massageExpression(model, project, originFilterCondition);
            String filterConditionAddTableName = addTableNameIfNotExist(newFilterCondition, model);
            model.setFilterCondition(filterConditionAddTableName);
        }

        checkCCNameAmbiguity(model);

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = KapQueryUtil.massageComputedColumn(model, project, ccDesc);
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
            throw new KylinException(MODEL_NOT_EXIST, String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
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
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getMODEL_SEGMENT_CAN_NOT_REMOVE(), dataModel.getAlias()));
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsExist(model, project, ids);
        checkSegmentsStatus(model, project, ids, SegmentStatusEnumToDisplay.LOCKED);

        val indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Set<String> idsToDelete = Sets.newHashSet();
        for (String id : ids) {
            if (dataflow.getSegment(id) != null) {
                idsToDelete.add(id);
            } else {
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getSEG_NOT_FOUND(), id, dataflow.getModelAlias()));
            }
        }
        segmentHelper.removeSegment(project, dataflow.getUuid(), idsToDelete);
        cleanIndexPlanWhenNoSegments(project, dataflow.getUuid());
    }

    private void checkSegmentsExist(String model, String project, String[] ids) {
        Preconditions.checkNotNull(model);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(ids);

        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());

        List<String> notExistIds = Stream.of(ids).filter(segmentId -> null == dataflow.getSegment(segmentId))
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(notExistIds)) {
            throw new KylinException(SEGMENT_NOT_EXIST,
                    String.format("Can not find the Segments by ids [%s]", StringUtils.join(notExistIds, ",")));
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
        String message = status.equals(SegmentStatusEnumToDisplay.LOCKED) ? MsgPicker.getMsg().getSEGMENT_LOCKED()
                : MsgPicker.getMsg().getSEGMENT_STATUS(status.name());
        for (String id : ids) {
            if (segments.getSegmentStatusToDisplay(dataflow.getSegment(id)).equals(status)) {
                throw new KylinException(PERMISSION_DENIED, String.format(message, id));
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
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_REMOVE_SEGMENT());
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
                        String.format(MsgPicker.getMsg().getSEGMENT_CONTAINS_GAPS(), segmentList.get(i).getId(),
                                segmentList.get(i + 1).getId()));
            }
        }
    }

    @Transaction(project = 1)
    public JobInfoResponse.JobInfo mergeSegmentsManually(String modelId, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);

        val dfManager = getDataflowManager(project);
        val eventManager = getEventManager(project);
        val indexPlan = getIndexPlan(modelId, project);
        val df = dfManager.getDataflow(indexPlan.getUuid());

        checkSegmentsExist(modelId, project, ids);
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
                        String.format(MsgPicker.getMsg().getSEG_NOT_FOUND(), id, df.getModelAlias()));
            }

            if (!segment.getStatus().equals(SegmentStatusEnum.READY)) {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_MERGE_SEGMENT());
            }

            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();

            if (segmentStart < start)
                start = segmentStart;

            if (segmentEnd > end)
                end = segmentEnd;
        }

        NDataSegment mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(start, end), true);

        val mergeEvent = new MergeSegmentEvent();
        mergeEvent.setModelId(modelId);
        mergeEvent.setSegmentId(mergeSeg.getId());
        mergeEvent.setJobId(UUID.randomUUID().toString());
        mergeEvent.setOwner(getUsername());
        eventManager.post(mergeEvent);

        return new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), mergeEvent.getJobId());
    }

    @Transaction(project = 1)
    public List<JobInfoResponse.JobInfo> refreshSegmentById(String modelId, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);
        checkSegmentsExist(modelId, project, ids);
        checkSegmentsStatus(modelId, project, ids, SegmentStatusEnumToDisplay.LOADING,
                SegmentStatusEnumToDisplay.REFRESHING, SegmentStatusEnumToDisplay.MERGING,
                SegmentStatusEnumToDisplay.LOCKED);

        List<JobInfoResponse.JobInfo> jobIds = new ArrayList<>();
        NDataflowManager dfMgr = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());

        for (String id : ids) {
            NDataSegment segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getSEG_NOT_FOUND(), id, df.getModelAlias()));
            }

            NDataSegment newSeg = dfMgr.refreshSegment(df, segment.getSegRange());

            val event = new RefreshSegmentEvent();
            event.setSegmentId(newSeg.getId());
            event.setModelId(modelId);
            event.setOwner(getUsername());
            event.setJobId(UUID.randomUUID().toString());
            eventManager.post(event);

            jobIds.add(new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_REFRESH.toString(), event.getJobId()));
        }
        return jobIds;
    }

    @Transaction(project = 0)
    public void updateDataModelSemantic(String project, ModelRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelRequest(request);
        validatePartitionDateColumn(request);

        val modelId = request.getUuid();
        val modelManager = getDataModelManager(project);
        val originModel = modelManager.getDataModelDesc(modelId);

        val copyModel = modelManager.copyForWrite(originModel);
        semanticUpdater.updateModelColumns(copyModel, request, true);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), request.getProject())
                .getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(),
                project);

        preProcessBeforeModelSave(copyModel, project);
        modelManager.updateDataModelDesc(copyModel);

        var newModel = modelManager.getDataModelDesc(modelId);

        checkIndexColumnExist(project, modelId, originModel);

        checkFlatTableSql(newModel);
        semanticUpdater.handleSemanticUpdate(project, modelId, originModel, request.getStart(), request.getEnd());

        val projectInstance = NProjectManager.getInstance(getConfig()).getProject(project);
        if (projectInstance.isSemiAutoMode()) {
            val recommendationManager = OptimizeRecommendationManager.getInstance(getConfig(), project);
            recommendationManager.cleanInEffective(modelId);
        }
    }

    private void checkIndexColumnExist(String project, String modelId, NDataModel originModel) {
        val indePlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indePlanManager.getIndexPlan(modelId);
        var newModel = getDataModelManager(project).getDataModelDesc(modelId);

        // check agg group contains removed dimensions
        val rule = indexPlan.getRuleBasedIndex();
        if (rule != null) {
            if (!newModel.getEffectiveDimensions().keySet().containsAll(rule.getDimensions())) {
                val allDimensions = rule.getDimensions();
                val dimensionNames = allDimensions.stream()
                        .filter(id -> !newModel.getEffectiveDimensions().containsKey(id))
                        .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
                throw new IllegalStateException(String.format(MsgPicker.getMsg().getAGGINDEX_DIMENSION_NOTFOUND(),
                        indexPlan.getModel().getUuid(), StringUtils.join(dimensionNames, ",")));
            }

            for (NAggregationGroup agg : rule.getAggregationGroups()) {
                if (!newModel.getEffectiveMeasures().keySet().containsAll(Sets.newHashSet(agg.getMeasures()))) {
                    val measureNames = Arrays.stream(agg.getMeasures())
                            .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                            .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                    throw new IllegalStateException(String.format(MsgPicker.getMsg().getAGGINDEX_MEASURE_NOTFOUND(),
                            indexPlan.getModel().getUuid(), StringUtils.join(measureNames, ",")));
                }
            }
        }

        //check table index contains removed columns
        val tableIndexColumns = indexPlan.getIndexes().stream().filter(IndexEntity::isTableIndex)
                .map(IndexEntity::getDimensions).flatMap(List::stream).collect(Collectors.toSet());
        val allNamedColumns = newModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .map(NDataModel.NamedColumn::getId).collect(Collectors.toList());
        if (!allNamedColumns.containsAll(tableIndexColumns)) {
            val columnNames = tableIndexColumns.stream().filter(x -> !allNamedColumns.contains(x))
                    .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());
            throw new IllegalStateException(String.format(MsgPicker.getMsg().getTABLEINDEX_COLUMN_NOTFOUND(),
                    indexPlan.getModel().getUuid(), StringUtils.join(columnNames, ",")));
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
                throw new IllegalStateException(String.format(MsgPicker.getMsg().getAGGINDEX_DIMENSION_NOTFOUND(),
                        indexPlan.getModel().getUuid(), StringUtils.join(dimensionNames, ",")));
            }

            if (!newModel.getEffectiveMeasures().keySet().containsAll(aggIndex.getMeasures())) {
                val measureNames = aggIndex.getMeasures().stream()
                        .filter(measureId -> !newModel.getEffectiveMeasures().containsKey(measureId))
                        .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());
                throw new IllegalStateException(String.format(MsgPicker.getMsg().getAGGINDEX_MEASURE_NOTFOUND(),
                        indexPlan.getModel().getUuid(), StringUtils.join(measureNames, ",")));

            }
        }
    }

    public void updateBrokenModel(String project, ModelRequest modelRequest, Set<Integer> columnIds) {
        val modelManager = getDataModelManager(project);
        val origin = modelManager.getDataModelDesc(modelRequest.getUuid());
        val copyModel = modelManager.copyForWrite(origin);
        semanticUpdater.updateModelColumns(copyModel, modelRequest, true);
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

        if (prj.getMaintainModelType().equals(MaintainModelType.AUTO_MAINTAIN)
                || broken.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
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
            semanticUpdater.updateModelColumns(broken, modelRequest, true);
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
                throw new KylinException(PERMISSION_DENIED, String.format(
                        MsgPicker.getMsg().getINVALID_SET_TABLE_INC_LOADING(), tableName, modelDesc.getAlias()));
            }
        }
    }

    private void checkProjectWhenModelSelected(String model, List<String> projects) {
        if (!isSelectAll(model) && (CollectionUtils.isEmpty(projects) || projects.size() != 1)) {
            throw new KylinException(DUPLICATE_MODEL_NAME,
                    "Only one project name should be specified while model is specified!");
        }
    }

    private List<ModelInfoResponse> getModelInfoByProject(List<String> projects) {
        List<ModelInfoResponse> modelInfoLists = Lists.newArrayList();

        for (val project : projects) {
            val dfManager = getDataflowManager(project);
            val models = dfManager.listUnderliningDataModels();
            for (val model : models) {
                modelInfoLists.add(getModelInfoByModel(model.getId(), project));
            }
        }
        return modelInfoLists;
    }

    private ModelInfoResponse getModelInfoByModel(String model, String project) {
        val dataflowManager = getDataflowManager(project);
        val modelManager = getDataModelManager(project);
        val modelDesc = modelManager.getDataModelDesc(model);
        if (modelDesc == null) {
            throw new KylinException(MODEL_NOT_EXIST, String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), model));
        }
        val modelInfoResponse = new ModelInfoResponse();
        modelInfoResponse.setProject(project);
        modelInfoResponse.setAlias(modelDesc.getAlias());
        modelInfoResponse.setModel(model);
        val modelSize = dataflowManager.getDataflowStorageSize(model);
        modelInfoResponse.setModelStorageSize(modelSize);
        return modelInfoResponse;
    }

    private boolean isSelectAll(String field) {
        return "*".equals(field);
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
                    }
                    responseList.add(response);
                });
        return responseList;
    }

    @Transaction(project = 0)
    public void updateModelConfig(String project, String modelId, ModelConfigRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
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
            copyForWrite.setOverrideProps(overrideProps);

            // affected by "kylin.cube.aggrgroup.is-base-cuboid-always-valid" config
            if (copyForWrite.getRuleBasedIndex() != null) {
                val newRule = JsonUtil.deepCopyQuietly(copyForWrite.getRuleBasedIndex(), NRuleBasedIndex.class);
                newRule.setLastModifiedTime(System.currentTimeMillis());
                newRule.setLayoutIdMapping(Lists.newArrayList());
                copyForWrite.setRuleBasedIndex(newRule);
            }
        });
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
    public BuildIndexResponse buildIndicesManually(String modelId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        if (!modelDesc.getManagementType().equals(ManagementType.MODEL_BASED)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getCAN_NOT_BUILD_INDICES_MANUALLY(), modelDesc.getAlias()));
        }

        NDataflow df = getDataflowManager(project).getDataflow(modelId);
        IndexPlan indexPlan = getIndexPlanManager(project).getIndexPlan(modelId);

        val segments = df.getSegments();
        if (segments.isEmpty()) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
        }

        // be process layouts = all layouts - ready layouts
        val lastSeg = segments.getLastSegment();
        Set<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        for (LayoutEntity layout : indexPlan.getAllLayouts()) {
            NDataLayout nc = lastSeg.getLayout(layout.getId());
            if (nc == null) {
                toBeProcessedLayouts.add(layout);
            }
        }

        if (CollectionUtils.isEmpty(toBeProcessedLayouts)) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);
        }

        val eventManager = getEventManager(project);
        String jobId = eventManager.postAddCuboidEvents(modelId, getUsername());

        return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD, jobId);
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

    public void checkFilterCondition(ModelRequest modelRequest) {
        aclEvaluate.checkProjectWritePermission(modelRequest.getProject());
        NDataModel model = semanticUpdater.convertToDataModel(modelRequest);
        NDataModel oldDataModel = getDataModelManager(model.getProject()).getDataModelDesc(model.getUuid());
        if (oldDataModel != null && !oldDataModel.isBroken()) {
            model = getDataModelManager(model.getProject()).copyForWrite(oldDataModel);
            semanticUpdater.updateModelColumns(model, modelRequest, false);
        }
        String originFilterCondition = model.getFilterCondition();
        try {
            if (StringUtils.isNotEmpty(originFilterCondition)) {
                String filterConditionAddTableName = addTableNameIfNotExist(originFilterCondition, model);
                model.setFilterCondition(filterConditionAddTableName);
            }
        } catch (Exception e) {
            throw new KylinException(INVALID_FILTER_CONDITION, e);
        }
    }

    public String addTableNameIfNotExist(final String expr, final NDataModel model) {
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
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel dataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        NDataModelResponse model = new NDataModelResponse(dataModel);
        NModelDescResponse response = new NModelDescResponse();
        response.setUuid(model.getUuid());
        response.setLastModified(model.getLastModified());
        response.setVersion(model.getVersion());
        response.setName(model.getAlias());
        response.setProject(model.getProject());
        response.setDescription(model.getDescription());

        response.setMeasures(model.getMeasures());
        IndexPlan indexPlan = getIndexPlan(response.getUuid(), project);
        if (indexPlan.getRuleBasedIndex() != null) {
            List<AggGroupResponse> aggGroupResponses = indexPlan.getRuleBasedIndex().getAggregationGroups().stream()
                    .map(x -> new AggGroupResponse(indexPlan, x)).collect(Collectors.toList());
            response.setAggregationGroups(aggGroupResponses);
            Set<String> allAggDim = Sets.newHashSet();
            indexPlan.getRuleBasedIndex().getDimensions().stream().map(x -> indexPlan.getEffectiveDimCols().get(x))
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
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        NDataModel oldDataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (oldDataModel == null) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }

        PartitionDesc partitionDesc = modelParatitionDescRequest.getPartitionDesc();
        if (partitionDesc != null) {
            String rootFactTable = oldDataModel.getRootFactTableName().split("\\.")[1];
            if (!partitionDesc.getPartitionDateColumn().toUpperCase().startsWith(rootFactTable + ".")) {
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
        if (AclPermissionUtil.isAdmin() || AclPermissionUtil.isAdminInProject(project)) {
            for (val model : models) {
                NDataModelAclParams aclParams = new NDataModelAclParams();
                aclParams.setUnauthorizedTables(Sets.newHashSet());
                aclParams.setUnauthorizedColumns(Sets.newHashSet());
                if (model instanceof NDataModelResponse) {
                    ((NDataModelResponse) model).setAclParams(aclParams);
                } else if (model instanceof RelatedModelResponse) {
                    ((RelatedModelResponse) model).setAclParams(aclParams);
                }
            }
            return models;
        }
        String username = AclPermissionUtil.getCurrentUsername();
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        var auths = getAclTCRManager(project).getAuthTablesAndColumns(project, username, true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        Set<String> groups = AclPermissionUtil.getCurrentUserGroups();
        for (val group : groups) {
            auths = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }
        List<NDataModel> normalModel = new ArrayList<>();
        List<NDataModel> noVisibleModel = new ArrayList<>();
        for (val model : models) {
            Set<String> tablesNoAcl = Sets.newHashSet();
            Set<String> columnsNoAcl = Sets.newHashSet();
            Set<String> tables = Sets.newHashSet();
            model.getJoinTables().forEach(table -> tables.add(table.getTable()));
            tables.add(model.getRootFactTableName());
            tables.stream().filter(table -> !allAuthTables.contains(table)).forEach(table -> tablesNoAcl.add(table));
            for (String table : tables) {
                if (!allAuthTables.contains(table))
                    continue;
                ColumnDesc[] columnDescs = NTableMetadataManager.getInstance(getConfig(), project).getTableDesc(table)
                        .getColumns();
                Arrays.stream(columnDescs).map(column -> table + "." + column.getName())
                        .filter(column -> !allAuthColumns.contains(column)).forEach(column -> columnsNoAcl.add(column));
            }
            NDataModelAclParams aclParams = new NDataModelAclParams();
            aclParams.setUnauthorizedTables(tablesNoAcl);
            aclParams.setUnauthorizedColumns(columnsNoAcl);
            if (model instanceof NDataModelResponse) {
                ((NDataModelResponse) model).setAclParams(aclParams);
            } else if (model instanceof RelatedModelResponse) {
                ((RelatedModelResponse) model).setAclParams(aclParams);
            }
            (aclParams.isVisible() ? normalModel : noVisibleModel).add(model);
        }
        List<NDataModel> result = new ArrayList<>(normalModel);
        result.addAll(noVisibleModel);
        return result;
    }

    public NDataModel updateReponseAcl(NDataModel model, String project) {
        List<NDataModel> models = updateReponseAcl(Arrays.asList(model), project);
        return models.get(0);
    }

    public void checkDuplicateAliasInModelRequests(Collection<ModelRequest> modelRequests) {
        Map<String, List<ModelRequest>> aliasModelRequestMap = modelRequests.stream()
                .collect(groupingBy(modelRequest -> modelRequest.getAlias().toLowerCase(), Collectors.toList()));
        aliasModelRequestMap.forEach((alias, requests) -> {
            if (requests.size() > 1) {
                throw new KylinException(INVALID_NAME, String.format(MsgPicker.getMsg().getMODEL_ALIAS_DUPLICATED(),
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
                    errorSb.append(MessageFormat.format(MsgPicker.getMsg().getCheckCCType(),
                            computedColumnDesc.getFullName(), ccCopy.getDatatype(), computedColumnDesc.getDatatype()));
                }
            } catch (Exception e) {
                logger.error("Error validating computed column {}, {}", computedColumnDesc, e);
            }
        }

        if (errorSb.length() > 0) {
            throw new RuntimeException(errorSb.toString());
        }
    }

    private static final Set<String> StringTypes = Sets.newHashSet("STRING", "CHAR", "VARCHAR");

    private boolean matchCCDataType(String actual, String expected) {
        if (actual == null) {
            return false;
        }

        actual = actual.toUpperCase();
        expected = expected.toUpperCase();
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
                    String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
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
}