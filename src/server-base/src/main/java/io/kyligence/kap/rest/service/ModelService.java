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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.adhocquery.HivePushDownConverter;
import org.apache.kylin.source.adhocquery.PushDownConverterKeyWords;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
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
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelDropAddListener;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggGroupResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
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
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.query.advisor.AdviceMessage;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    private static final String MODEL = "Model '";

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

    private NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getDataModelManager(project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
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
        if (getProjectManager().getProject(projectName) == null) {
            throw new RuntimeException("project not found");
        }
        NDataModel dataModel = getDataModelManager(projectName).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new RuntimeException("model not found");
        }
        NDataModelResponse cube = new NDataModelResponse(dataModel);
        NCubeDescResponse result = new NCubeDescResponse();

        result.setUuid(cube.getUuid());
        result.setName(cube.getAlias());
        result.setMeasures(
                cube.getMeasures().stream().map(x -> new NCubeDescResponse.Measure3X(x)).collect(Collectors.toList()));

        IndexPlan indexPlan = getIndexPlan(result.getUuid(), projectName);
        if (indexPlan.getRuleBasedIndex() != null) {
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

        List<NDataModelResponse> modelResponseList = getModels(modelAlias, project, false, null, null, LAST_MODIFY,
                true);

        modelResponseList.forEach(modelResponse -> {
            NDataModelOldParams oldParams = new NDataModelOldParams();

            long inputRecordCnt = 0L;
            long inputRecordSizeBytes = 0L;
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

            oldParams.setName(modelResponse.getAlias());
            oldParams.setProjectName(project);
            oldParams.setStreaming(false);
            oldParams.setSizeKB(modelResponse.getStorage() / 1024);
            oldParams.setInputRecordSizeBytes(inputRecordSizeBytes);
            oldParams.setInputRecordCnt(inputRecordCnt);

            modelResponse.setSegments(segments);
            modelResponse.setOldParams(oldParams);
        });

        return modelResponseList;
    }

    public List<NDataModelResponse> getModels(final String modelAlias, final String projectName, boolean exactMatch,
            String owner, String status, String sortBy, boolean reverse) {
        aclEvaluate.checkProjectReadPermission(projectName);
        List<NDataflow> dataflowList = getDataflowManager(projectName).listAllDataflows(true);
        val dfManager = getDataflowManager(projectName);
        val optRecomManager = getOptRecommendationManager(projectName);

        List<NDataModelResponse> filterModels = new ArrayList<>();
        for (NDataflow dataflow : dataflowList) {
            var modelDesc = dataflow.getModel();
            val isBroken = dataflow.checkBrokenWithRelatedInfo();
            if (isBroken) {
                modelDesc = getBrokenModel(projectName, modelDesc.getId());
            }
            boolean isModelNameMatch = isArgMatch(modelAlias, exactMatch, modelDesc.getAlias());
            boolean isModelOwnerMatch = isArgMatch(owner, exactMatch, modelDesc.getOwner());
            if (isModelNameMatch && isModelOwnerMatch) {
                RealizationStatusEnum modelStatus = isBroken ? RealizationStatusEnum.BROKEN
                        : getModelStatus(modelDesc.getUuid(), projectName);
                boolean isModelStatusMatch = StringUtils.isEmpty(status)
                        || (modelStatus != null && modelStatus.name().equalsIgnoreCase(status));

                if (isModelStatusMatch) {
                    NDataModelResponse nDataModelResponse = enrichModelResponse(modelDesc, projectName);
                    nDataModelResponse.setModelBroken(isBroken);
                    nDataModelResponse.setStatus(modelStatus);
                    nDataModelResponse.setStorage(dfManager.getDataflowStorageSize(modelDesc.getUuid()));
                    nDataModelResponse.setSource(dfManager.getDataflowSourceSize(modelDesc.getUuid()));
                    nDataModelResponse.setExpansionrate(
                            computeExpansionRate(nDataModelResponse.getStorage(), nDataModelResponse.getSource()));
                    nDataModelResponse.setUsage(dataflow.getQueryHitCount());
                    nDataModelResponse
                            .setRecommendationsCount(optRecomManager.getRecommendationCount(modelDesc.getId()));
                    filterModels.add(nDataModelResponse);
                }
            }
        }
        if ("expansionrate".equalsIgnoreCase(sortBy)) {
            return sortExpansionRate(reverse, filterModels);
        } else {
            Comparator<NDataModelResponse> comparator = propertyComparator(
                    StringUtils.isEmpty(sortBy) ? LAST_MODIFY : sortBy, !reverse);
            filterModels.sort(comparator);
            return filterModels;
        }
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

    private String computeExpansionRate(long storageBytesSize, long sourceBytesSize) {
        if (storageBytesSize == 0) {
            return "0";
        }
        if (sourceBytesSize == 0) {
            return "-1";
        }
        BigDecimal divide = new BigDecimal(storageBytesSize).divide(new BigDecimal(sourceBytesSize), 4,
                BigDecimal.ROUND_HALF_UP);
        BigDecimal bigDecimal = divide.multiply(new BigDecimal(100)).setScale(2);
        return bigDecimal.toString();
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
        indexPlan.getAllIndexes().stream().filter(e -> e.getId() >= IndexEntity.TABLE_INDEX_START_ID)
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
            throw new BadRequestException("Model alias " + newAlias + " already exists!");
        }
    }

    public boolean checkModelAliasUniqueness(String modelId, String newAlias, String project) {
        List<NDataModel> models = getDataflowManager(project).listUnderliningDataModels();
        for (NDataModel model : models) {
            if ((StringUtils.isNotEmpty(modelId) || !model.getUuid().equals(modelId))
                    && model.getAlias().equals(newAlias)) {
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
            throw new BadRequestException(
                    MODEL + dataModelDesc.getAlias() + "' is table oriented, can not purge the model!!");
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
            throw new BadRequestException("DescBroken model " + modelId + " can not online or offline!");
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
        aclEvaluate.checkProjectOperationPermission(project);
        val dfManager = getDataflowManager(project);
        long byteSize = 0L;
        List<RelatedModelResponse> models = getRelateModels(project, table, "").stream().filter(
                relatedModelResponse -> relatedModelResponse.getManagementType().equals(ManagementType.TABLE_ORIENTED))
                .collect(Collectors.toList());

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
                        throw new BadRequestException("No segments to refresh, please select new range and try again!");
                    }
                }

                if (CollectionUtils.isNotEmpty(segments.getBuildingSegments())) {
                    throw new BadRequestException(
                            "Can not refresh, some segments is building within the range you want to refresh!");
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
                throw new BadRequestException(
                        "Can not refresh, some segments is building within the range you want to refresh!");
            }
        }
    }

    private void checkRefreshRangeWithinCoveredRange(NDataLoadingRange dataLoadingRange, String project, String table,
            SegmentRange toBeRefreshSegmentRange) {
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new IllegalArgumentException("ToBeRefreshSegmentRange " + toBeRefreshSegmentRange
                    + " is out of range the coveredReadySegmentRange of dataLoadingRange, the coveredReadySegmentRange is "
                    + coveredReadySegmentRange);
        }
    }

    @Transaction(project = 0)
    public void refreshSegments(String project, String table, String refreshStart, String refreshEnd,
            String affectedStart, String affectedEnd) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        RefreshAffectedSegmentsResponse response = getRefreshAffectedSegmentsResponse(project, table, refreshStart,
                refreshEnd);
        if (!response.getAffectedStart().equals(affectedStart) || !response.getAffectedEnd().equals(affectedEnd)) {
            throw new BadRequestException("Ready segments range has changed, can not refresh, please try again.");
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
            String pushdownSql = new HivePushDownConverter().convert(flatTableSql, "", "default", false);
            ss.sql(pushdownSql);
        } catch (Exception e) {
            Pattern pattern = Pattern.compile("cannot resolve '(.*?)' given input columns");
            Matcher matcher = pattern.matcher(e.getMessage().replaceAll("`", ""));
            if (matcher.find()) {
                String column = matcher.group(1);
                String table = column.contains(".") ? column.split("\\.")[0] : dataModel.getRootFactTableName();
                String error = String.format(MsgPicker.getMsg().getTABLENOTFOUND(), column, table);
                throw new RuntimeException(error);
            } else {
                String errorMsg = String.format(AdviceMessage.getInstance().getDefaultReason(),
                        null != e.getMessage() ? e.getMessage() : "null");
                throw new RuntimeException(errorMsg);
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

    public void batchCreateModel(String project, List<ModelRequest> modelRequests) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);

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

        UnitOfWork.doInTransactionWithRetry(() -> {
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
        return UnitOfWork.doInTransactionWithRetry(() -> saveModel(project, modelRequest), project);
    }

    public boolean couldAnsweredByExistedModel(String project, List<String> sqls) {
        if (CollectionUtils.isEmpty(sqls))
            return true;

        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project,
                sqls.toArray(new String[0]));
        smartMaster.selectExistedModel();
        return CollectionUtils.isNotEmpty(smartMaster.getRecommendedModels());
    }

    public NRecomendationListResponse suggestModel(String project, List<String> sqls, boolean reuseExistedModel) {
        if (CollectionUtils.isEmpty(sqls)) {
            return null;
        }

        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project,
                sqls.toArray(new String[0]), reuseExistedModel, true);
        val recommendationsMap = reuseExistedModel ? smartMaster.selectAndGenRecommendation()
                : smartMaster.recommendNewModelAndIndex();
        return constructModelRecommendResponse(recommendationsMap, smartMaster.getContext());
    }

    private NRecomendationListResponse constructModelRecommendResponse(
            Map<NDataModel, OptimizeRecommendation> recommendationsMap, NSmartContext context) {
        List<NRecomendationListResponse.NRecomendedDataModelResponse> newDataModelResponseList = Lists.newArrayList();
        List<NRecomendationListResponse.NRecomendedDataModelResponse> originDataModelResponseList = Lists
                .newArrayList();

        for (NSmartContext.NModelContext modelContext : context.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }

            NRecomendationListResponse.NRecomendedDataModelResponse response = new NRecomendationListResponse.NRecomendedDataModelResponse(
                    modelContext.getTargetModel());
            Set<String> acceleratedSqls = Sets.newHashSet();
            modelContext.getModelTree().getOlapContexts().forEach(ctx -> acceleratedSqls.add(ctx.sql));
            response.setDimensions(modelContext.getTargetModel().getAllNamedColumns().stream()
                    .filter(dim -> dim.isDimension()).collect(Collectors.toList()));
            response.setSqls(Lists.newArrayList(acceleratedSqls));

            if (recommendationsMap.get(modelContext.getTargetModel()) != null) {
                response.setRecommendationResponse(
                        new OptRecommendationResponse(recommendationsMap.get(modelContext.getTargetModel()), null));
            } else {
                // build Agg && Table index
                IndexPlan indexPlan = modelContext.getTargetIndexPlan();
                response.setIndices(indexPlan);
            }
            if (modelContext.getOriginModel() == null) {
                newDataModelResponseList.add(response);
            } else {
                originDataModelResponseList.add(response);
            }
        }
        return new NRecomendationListResponse(originDataModelResponseList, newDataModelResponseList);
    }

    private NDataModel doCheckBeforeModelSave(String project, ModelRequest modelRequest) throws Exception {

        if (modelRequest.getRecommendation() == null) {
            // new model
            checkAliasExist(modelRequest.getUuid(), modelRequest.getAlias(), project);
        }
        checkModelRequest(modelRequest);

        //remove some attributes in modelResponse to fit NDataModel
        val prjManager = getProjectManager();
        val prj = prjManager.getProject(project);
        val dataModel = semanticUpdater.convertToDataModel(modelRequest);
        if (prj.getMaintainModelType().equals(MaintainModelType.AUTO_MAINTAIN)
                || dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException("Can not create model manually in SQL acceleration project!");
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
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof UserDetails) {
            request.setOwner(((UserDetails) authentication.getPrincipal()).getUsername());
        }
    }

    private void checkModelDimensions(ModelRequest request) {
        Set<String> dimensionNames = new HashSet<>();

        for (NDataModel.NamedColumn dimension : request.getSimplifiedDimensions()) {
            // check if the dimension name is valid
            if (!StringUtils.containsOnly(dimension.getName(), VALID_NAME_FOR_MODEL_DIMENSION_MEASURE))
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getINVALID_DIMENSION_NAME(), dimension.getName()));

            // check duplicate dimension names
            if (dimensionNames.contains(dimension.getName()))
                throw new IllegalArgumentException(
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
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getINVALID_MEASURE_NAME(), measure.getName()));

            // check duplicate measure names
            if (measureNames.contains(measure.getName()))
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getDUPLICATE_MEASURE_NAME(), measure.getName()));

            // check duplicate measure definitions
            if (measures.contains(measure))
                throw new IllegalArgumentException(
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
                    throw new IllegalArgumentException(String.format(MsgPicker.getMsg().getDUPLICATE_JOIN_CONDITIONS(),
                            primaryKeys[i], foreignKey[i]));

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

    private Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String model)
            throws Exception {
        val modelManager = getDataModelManager(project);
        val modelDesc = modelManager.getDataModelDesc(model);
        val table = modelDesc.getRootFactTableName();

        String partitionColumn = modelDesc.getPartitionDesc().getPartitionDateColumn();
        String dateFormat = modelDesc.getPartitionDesc().getPartitionDateFormat();
        Preconditions.checkArgument(StringUtils.isNotEmpty(dateFormat) && StringUtils.isNotEmpty(partitionColumn));

        val minAndMaxTime = PushDownUtil.getMaxAndMinTimeWithTimeOut(partitionColumn, table, project);

        return new Pair<>(DateFormat.getFormattedDate(minAndMaxTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(minAndMaxTime.getSecond(), dateFormat));
    }

    public void buildSegmentsManually(String project, String modelId, String start, String end) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
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
        String format = probeDateFormatIfNotExist(project, modelDesc);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataModel modelDescInTransaction = getDataModelManager(project).getDataModelDesc(modelId);
            SegmentRange segmentRangeToBuild;

            TableDesc table = getTableManager(project).getTableDesc(modelDescInTransaction.getRootFactTableName());

            val df = getDataflowManager(project).getDataflow(modelId);

            if (modelDescInTransaction.getPartitionDesc() == null
                    || StringUtils.isEmpty(modelDescInTransaction.getPartitionDesc().getPartitionDateColumn())) {
                //if full seg exists,refresh it
                val segs = df.getSegments(SegmentStatusEnum.READY);
                if (segs.size() == 1 && segs.get(0).getSegRange().isInfinite()) {
                    refreshSegmentById(modelId, project,
                            Lists.newArrayList(df.getSegments().get(0).getId()).toArray(new String[0]));
                    return null;
                }
                //build Full seg
                segmentRangeToBuild = SegmentRange.TimePartitionedSegmentRange.createInfinite();
            } else {
                Preconditions.checkArgument(!PushDownUtil.needPushdown(start, end),
                        "Load data must set start and end date");
                segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(start, end);
            }
            saveDateFormatIfNotExist(project, modelId, format);

            checkSegmentToBuildOverlapsBuilt(project, modelId, segmentRangeToBuild);

            NDataSegment newSegment = getDataflowManager(project).appendSegment(df, segmentRangeToBuild);

            EventManager eventManager = getEventManager(project);

            eventManager.postAddSegmentEvents(newSegment, modelId, getUsername());
            eventManager.postAddCuboidEvents(modelId, getUsername());
            return null;
        }, project);
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
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
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
    public ComputedColumnDesc checkComputedColumn(final NDataModel dataModelDesc, String project, String ccInCheck) {

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
            ComputedColumnDesc.simpleParserCheck(ccExpression, dataModelDesc.getAliasMap().keySet());
            cc.setInnerExpression(ccExpression);

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
            throw new IllegalStateException(
                    "The computed column's name:" + name + " is a sql keyword, please choose another name.");
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
        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            String ccExpression = KapQueryUtil.massageComputedColumn(model, project, ccDesc);
            ccDesc.setInnerExpression(ccExpression);
        }
    }

    @Transaction(project = 0)
    public void updateModelDataCheckDesc(String project, String modelId, long checkOptions, long faultThreshold,
            long faultActions) {
        aclEvaluate.checkProjectWritePermission(project);
        final NDataModel dataModel = getDataModelManager(project).getDataModelDesc(modelId);
        if (dataModel == null) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
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
    public void deleteSegmentById(String model, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel dataModel = getDataModelManager(project).getDataModelDesc(model);
        if (dataModel.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException(
                    MODEL + dataModel.getAlias() + "' is table oriented, can not remove segments manually!");
        }
        NDataflowManager dataflowManager = getDataflowManager(project);
        checkSegmentsExist(model, project, ids);
        checkSegmentsLocked(model, project, ids);
        checkDeleteSegmentLegally(model, project, ids);
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
            throw new BadRequestException(
                    String.format("Can not find the Segments by ids [%s]", StringUtils.join(notExistIds, ",")));
        }
    }

    private void checkSegmentsLocked(String model, String project, String[] ids) {
        NDataflowManager dataflowManager = getDataflowManager(project);
        IndexPlan indexPlan = getIndexPlan(model, project);
        NDataflow dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        Segments<NDataSegment> segments = dataflow.getSegments();
        for (String id : ids) {
            if (segments.getSegmentStatusToDisplay(dataflow.getSegment(id)).equals(SegmentStatusEnumToDisplay.LOCKED)) {
                throw new BadRequestException(
                        "Can not remove or refresh segment (ID:" + id + "), because the segment is LOCKED.");
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
    public void mergeSegmentsManually(String modelId, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);
        val dfManager = getDataflowManager(project);
        val eventManager = getEventManager(project);
        val indexPlan = getIndexPlan(modelId, project);
        val df = dfManager.getDataflow(indexPlan.getUuid());

        checkSegmentsExist(modelId, project, ids);
        checkSegmentsLocked(modelId, project, ids);

        long start = Long.MAX_VALUE;
        long end = -1;

        for (String id : ids) {
            val segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format(MsgPicker.getMsg().getSEG_NOT_FOUND(), id, df.getModelAlias()));
            }

            if (!segment.getStatus().equals(SegmentStatusEnum.READY)) {
                throw new BadRequestException("Cannot merge segments which are not ready");
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

        val postMergeEvent = new PostMergeOrRefreshSegmentEvent();
        postMergeEvent.setModelId(modelId);
        postMergeEvent.setSegmentId(mergeSeg.getId());
        postMergeEvent.setJobId(mergeEvent.getJobId());
        postMergeEvent.setOwner(getUsername());
        eventManager.post(postMergeEvent);
    }

    @Transaction(project = 1)
    public void refreshSegmentById(String modelId, String project, String[] ids) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataflowManager dfMgr = getDataflowManager(project);
        EventManager eventManager = getEventManager(project);
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());

        checkSegmentsExist(modelId, project, ids);
        checkSegmentsLocked(modelId, project, ids);

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

            val event2 = new PostMergeOrRefreshSegmentEvent();
            event2.setSegmentId(newSeg.getId());
            event2.setModelId(modelId);
            event2.setOwner(getUsername());
            event2.setJobId(event.getJobId());
            eventManager.post(event2);

        }
    }

    @Transaction(project = 0)
    public void updateDataModelSemantic(String project, ModelRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        checkModelRequest(request);
        validatePartitionDateColumn(request);

        val modelId = request.getUuid();
        val modelManager = getDataModelManager(project);
        val originModel = modelManager.getDataModelDesc(modelId);

        val cubeManager = getIndexPlanManager(project);
        val copyModel = modelManager.copyForWrite(originModel);
        semanticUpdater.updateModelColumns(copyModel, request);
        val allTables = NTableMetadataManager.getInstance(modelManager.getConfig(), request.getProject())
                .getAllTablesMap();
        copyModel.init(modelManager.getConfig(), allTables, getDataflowManager(project).listUnderliningDataModels(),
                project);

        val indexPlan = cubeManager.getIndexPlan(modelId);
        // check agg group contains removed dimensions
        val rule = indexPlan.getRuleBasedIndex();
        if (rule != null) {
            if (!copyModel.getEffectiveDimenionsMap().keySet().containsAll(rule.getDimensions())) {
                val allDimensions = rule.getDimensions();
                val dimensionNames = allDimensions.stream()
                        .filter(id -> !copyModel.getEffectiveDimenionsMap().containsKey(id))
                        .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());

                throw new IllegalStateException("model " + indexPlan.getModel().getUuid()
                        + "'s agg group still contains dimensions " + StringUtils.join(dimensionNames, ","));
            }

            for (NAggregationGroup agg : rule.getAggregationGroups()) {
                if (!copyModel.getEffectiveMeasureMap().keySet().containsAll(Sets.newHashSet(agg.getMeasures()))) {
                    val measureNames = Arrays.stream(agg.getMeasures())
                            .filter(measureId -> !copyModel.getEffectiveMeasureMap().containsKey(measureId))
                            .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());

                    throw new IllegalStateException("model " + indexPlan.getModel().getUuid()
                            + "'s agg group still contains measures " + measureNames);
                }

            }
        }

        preProcessBeforeModelSave(copyModel, project);
        modelManager.updateDataModelDesc(copyModel);

        var newModel = modelManager.getDataModelDesc(modelId);

        checkFlatTableSql(newModel);
        semanticUpdater.handleSemanticUpdate(project, modelId, originModel, request.getStart(), request.getEnd());

        val projectInstance = NProjectManager.getInstance(getConfig()).getProject(project);
        if (projectInstance.isSemiAutoMode()) {
            val recommendationManager = OptimizeRecommendationManager.getInstance(getConfig(), project);
            recommendationManager.cleanInEffective(modelId);
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

        if (prj.getMaintainModelType().equals(MaintainModelType.AUTO_MAINTAIN)
                || broken.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            throw new BadRequestException("Can not repair model manually smart mode!");
        }
        if (modelRequest.getPartitionDesc() != null
                && !modelRequest.getPartitionDesc().equals(broken.getPartitionDesc())) {
            broken.setPartitionDesc(modelRequest.getPartitionDesc());
        }
        broken.setFilterCondition(modelRequest.getFilterCondition());
        broken.setJoinTables(modelRequest.getJoinTables());
        broken.init(getConfig(), getTableManager(project).getAllTablesMap(),
                getDataflowManager(project).listUnderliningDataModels(), project);
        broken.setBrokenReason(NDataModel.BrokenReason.NULL);
        val format = probeDateFormatIfNotExist(project, broken);
        return UnitOfWork.doInTransactionWithRetry(() -> {
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
                throw new BadRequestException(String.format(
                        "Can not set table '%s' incremental loading, as another model '%s' uses it as a lookup table",
                        tableName, modelDesc.getAlias()));
            }
        }
    }

    private void checkProjectWhenModelSelected(String model, List<String> projects) {
        if (!isSelectAll(model) && (CollectionUtils.isEmpty(projects) || projects.size() != 1)) {
            throw new BadRequestException("Only one project name should be specified while model is specified!");
        }
    }

    public List<ModelInfoResponse> getModelInfo(String suite, String model, List<String> projects, long start,
            long end) {
        List<ModelInfoResponse> modelInfoList = Lists.newArrayList();
        checkProjectWhenModelSelected(model, projects);

        val projectManager = getProjectManager();
        if (CollectionUtils.isEmpty(projects)) {
            projects = projectManager.listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toList());
        }
        if (isSelectAll(model)) {
            modelInfoList.addAll(getModelInfoByProject(projects));
        } else {
            modelInfoList.add(getModelInfoByModel(model, projects.get(0)));
        }
        Map<String, QueryTimesResponse> resultMap = Maps.newHashMap();
        for (val project : projects) {
            resultMap.putAll(getModelQueryTimesMap(suite, project, model, start, end));

        }
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
            throw new BadRequestException(MODEL + model + "' does not exist!");
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

    public ExistedDataRangeResponse getLatestDataRange(String project, String modelId) throws Exception {
        Preconditions.checkNotNull(modelId);
        aclEvaluate.checkProjectReadPermission(project);

        val df = getDataflowManager(project).getDataflow(modelId);
        Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, modelId);
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
            throw new BadRequestException(
                    "Table oriented model '" + modelDesc.getAlias() + "' can not build indices manually!");
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
        eventManager.postAddCuboidEvents(modelId, getUsername());

        return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD);
    }

    public PurgeModelAffectedResponse getPurgeModelAffectedResponse(String project, String model) {
        val response = new PurgeModelAffectedResponse();
        val byteSize = getDataflowManager(project).getDataflowStorageSize(model);
        response.setByteSize(byteSize);
        long jobSize = getExecutableManager(project).countByModelAndStatus(model, ExecutableState::isProgressing);
        response.setRelatedJobSize(jobSize);
        return response;
    }

    public void checkFilterCondition(ModelRequest modelRequest) {
        NDataModel dataModel = semanticUpdater.convertToDataModel(modelRequest);
        preProcessBeforeModelSave(dataModel, modelRequest.getProject());
    }

    private class AddTableNameSqlVisitor extends SqlBasicVisitor<Object> {
        private String expr;
        private Map<String, String> colToTable;
        private Set<String> ambiguityCol;
        private Set<String> aliasSet;

        public AddTableNameSqlVisitor(String expr, Map<String, String> colToTable, Set<String> ambiguityCol,
                Set<String> aliasSet) {
            this.expr = expr;
            this.colToTable = colToTable;
            this.ambiguityCol = ambiguityCol;
            this.aliasSet = aliasSet;
        }

        @Override
        public Object visit(SqlIdentifier id) {
            boolean ok = true;
            if (id.names.size() == 1) {
                String column = id.names.get(0).toUpperCase().trim();
                if (!colToTable.containsKey(column) || ambiguityCol.contains(column)) {
                    ok = false;
                } else {
                    id.names = ImmutableList.of(colToTable.get(column), column);
                }
            } else if (id.names.size() == 2) {
                String table = id.names.get(0).toUpperCase().trim();
                String column = id.names.get(1).toUpperCase().trim();
                if (!aliasSet.contains(table) || !colToTable.containsKey(column)) {
                    ok = false;
                }
            } else {
                ok = false;
            }
            if (!ok) {
                throw new IllegalArgumentException(
                        "Unrecognized column: " + id.toString() + " in expression '" + expr + "'.");
            }
            return null;
        }

        @Override
        public Object visit(SqlCall call) {
            if (call instanceof SqlBasicCall) {
                if (call.getOperator() instanceof SqlAsOperator) {
                    throw new IllegalArgumentException(
                            String.format(AdviceMessage.getInstance().getDefaultReason(), "null"));
                }

                if (call.getOperator() instanceof SqlAggFunction) {
                    throw new IllegalArgumentException(
                            String.format(AdviceMessage.getInstance().getDefaultReason(), "null"));
                }
            }
            return call.getOperator().acceptCall(this, call);
        }
    }

    public String addTableNameIfNotExist(final String expr, final NDataModel model) {
        Map<String, String> colToTable = Maps.newHashMap();
        Set<String> ambiguityCol = Sets.newHashSet();
        for (val col : model.getAllNamedColumns()) {
            if (col.getStatus() == NDataModel.ColumnStatus.TOMB) {
                continue;
            }
            String aliasDotColumn = col.getAliasDotColumn();
            String table = aliasDotColumn.split("\\.")[0];
            String column = aliasDotColumn.split("\\.")[1];
            if (colToTable.containsKey(column)) {
                ambiguityCol.add(column);
            } else {
                colToTable.put(column, table);
            }
        }

        Set<String> aliasSet = model.getAliasMap().keySet();
        SqlNode sqlNode = CalciteParser.getExpNode(expr);

        SqlVisitor<Object> sqlVisitor = new AddTableNameSqlVisitor(expr, colToTable, ambiguityCol, aliasSet);

        sqlNode.accept(sqlVisitor);
        return sqlNode
                .toSqlString(new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE)), true)
                .toString();
    }

    public NModelDescResponse getModelDesc(String modelAlias, String project) {
        if (getProjectManager().getProject(project) == null) {
            throw new RuntimeException("project not found");
        }
        NDataModel dataModel = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (dataModel == null) {
            throw new RuntimeException("model not found");
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
}