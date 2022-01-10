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

import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_MEASURE_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB_SAVE_INDEX_SUCCESS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.UpdateImpact;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.model.tool.JoinDescNonEquiCompBean;
import org.apache.kylin.metadata.model.tool.NonEquiJoinConditionVisitor;
import org.apache.kylin.query.exception.QueryErrorCode;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.ModifyTableNameSqlVisitor;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.model.util.scd2.SCD2Exception;
import io.kyligence.kap.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import io.kyligence.kap.metadata.model.util.scd2.SCD2SqlConverter;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinDesc;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelCreateContextOfSemiV2;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ModelSemanticHelper extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelSemanticHelper.class);
    private ExpandableMeasureUtil expandableMeasureUtil =
            new ExpandableMeasureUtil((model, ccDesc) -> {
                String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                        AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(), getCurrentUserGroups()));
                ccDesc.setInnerExpression(ccExpression);
                ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
            });

    public NDataModel deepCopyModel(NDataModel originModel) {
        NDataModel nDataModel;
        try {
            nDataModel = JsonUtil.readValue(JsonUtil.writeValueAsIndentString(originModel), NDataModel.class);
            nDataModel.setJoinTables(SCD2SimplificationConvertUtil.deepCopyJoinTables(originModel.getJoinTables()));
        } catch (IOException e) {
            logger.error("Parse json failed...", e);
            throw new KylinException(CommonErrorCode.FAILED_PARSE_JSON, e);
        }
        return nDataModel;
    }

    public NDataModel convertToDataModel(ModelRequest modelRequest) {
        List<SimplifiedMeasure> simplifiedMeasures = modelRequest.getSimplifiedMeasures();
        NDataModel dataModel;
        try {
            dataModel = JsonUtil.deepCopy(modelRequest, NDataModel.class);
        } catch (IOException e) {
            logger.error("Parse json failed...", e);
            throw new KylinException(CommonErrorCode.FAILED_PARSE_JSON, e);
        }
        dataModel.setUuid(modelRequest.getUuid() != null ? modelRequest.getUuid() : RandomUtil.randomUUIDStr());
        dataModel.setProject(modelRequest.getProject());
        dataModel.setAllMeasures(convertMeasure(simplifiedMeasures));
        dataModel.setAllNamedColumns(convertNamedColumns(modelRequest.getProject(), dataModel, modelRequest));

        dataModel.initJoinDesc(KylinConfig.getInstanceFromEnv(),
                getTableManager(modelRequest.getProject()).getAllTablesMap());
        convertNonEquiJoinCond(dataModel, modelRequest);
        dataModel.setModelType(dataModel.getModelTypeFromTable());
        return dataModel;
    }

    /**
     * expand model request, add hidden internal measures from current model
     * @param modelRequest
     * @return
     */
    public void expandModelRequest(ModelRequest modelRequest) {
        if (modelRequest.getUuid() != null) {
            NDataModel existingModel =
                    NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), modelRequest.getProject())
                            .getDataModelDesc(modelRequest.getUuid());
            if (existingModel.isBroken()) {
                return;
            }
            Set<Integer> internalIds = new HashSet<>();
            for (SimplifiedMeasure measure : modelRequest.getSimplifiedMeasures()) {
                if (existingModel.getEffectiveExpandedMeasures().containsKey(measure.getId())) {
                    internalIds.addAll(existingModel.getEffectiveExpandedMeasures().get(measure.getId()));
                }
            }
            Set<Integer> requestMeasureIds = modelRequest.getSimplifiedMeasures().stream().map(SimplifiedMeasure::getId).collect(Collectors.toSet());
            for (Integer internalId : internalIds) {
                if (!requestMeasureIds.contains(internalId)) {
                    modelRequest.getSimplifiedMeasures().add(SimplifiedMeasure.fromMeasure(existingModel.getEffectiveMeasures().get(internalId)));
                }
            }
        }
    }

    public void deleteExpandableMeasureInternalMeasures(NDataModel model) {
        expandableMeasureUtil.deleteExpandableMeasureInternalMeasures(model);
    }

    /**
     * expand measures (e.g. CORR measure) in current model, may create new CC or new measures
     * @param model
     * @return
     */
    public void expandExpandableMeasure(NDataModel model) {
        expandableMeasureUtil.expandExpandableMeasure(model);
    }

    private void convertNonEquiJoinCond(final NDataModel dataModel, final ModelRequest request) {

        final List<SimplifiedJoinTableDesc> requestJoinTableDescs = request.getSimplifiedJoinTableDescs();
        if (CollectionUtils.isEmpty(requestJoinTableDescs)) {
            return;
        }

        HashSet<JoinDescNonEquiCompBean> scd2NonEquiCondSets = new HashSet<>();

        val projectKylinConfig = getProjectManager().getProject(dataModel.getProject()).getConfig();
        boolean isScd2Enabled = projectKylinConfig.isQueryNonEquiJoinModelEnabled();

        for (int i = 0; i < requestJoinTableDescs.size(); i++) {
            final JoinDesc modelJoinDesc = dataModel.getJoinTables().get(i).getJoin();
            final SimplifiedJoinDesc requestJoinDesc = requestJoinTableDescs.get(i).getSimplifiedJoinDesc();

            if (CollectionUtils.isEmpty(requestJoinDesc.getSimplifiedNonEquiJoinConditions())) {
                continue;
            }

            //1. check scd2 turn on when non-equi join exists
            if (!isScd2Enabled) {
                throw new KylinException(QueryErrorCode.SCD2_SAVE_MODEL_WHEN_DISABLED, "please turn on scd2 config");
            }

            //2. check request equi join condition
            checkRequestNonEquiJoinConds(requestJoinDesc);

            //3. suggest nonEquiModel
            final JoinDesc suggModelJoin = suggNonEquiJoinModel(projectKylinConfig, dataModel.getProject(),
                    modelJoinDesc, requestJoinDesc);
            // restore table alias in non-equi conditions
            final NonEquiJoinCondition nonEquiCondWithAliasRestored = new NonEquiJoinConditionVisitor() {
                @Override
                public NonEquiJoinCondition visitColumn(NonEquiJoinCondition cond) {
                    TableRef originalTableRef;
                    if (cond.getColRef().getTableRef().getTableIdentity()
                            .equals(modelJoinDesc.getPKSide().getTableIdentity())) {
                        originalTableRef = modelJoinDesc.getPKSide();
                    } else {
                        originalTableRef = modelJoinDesc.getFKSide();
                    }

                    return new NonEquiJoinCondition(originalTableRef.getColumn(cond.getColRef().getName()),
                            cond.getDataType());
                }
            }.visit(suggModelJoin.getNonEquiJoinCondition());
            suggModelJoin.setNonEquiJoinCondition(nonEquiCondWithAliasRestored);
            String expr = suggModelJoin.getNonEquiJoinCondition().getExpr();
            expr = expr.replaceAll(suggModelJoin.getPKSide().getAlias(), modelJoinDesc.getPKSide().getAlias());
            expr = expr.replaceAll(suggModelJoin.getFKSide().getAlias(), modelJoinDesc.getFKSide().getAlias());
            suggModelJoin.getNonEquiJoinCondition().setExpr(expr);
            suggModelJoin.setPrimaryTableRef(modelJoinDesc.getPKSide());
            suggModelJoin.setPrimaryTable(modelJoinDesc.getPrimaryTable());
            suggModelJoin.setForeignTableRef(modelJoinDesc.getFKSide());
            suggModelJoin.setForeignTable(modelJoinDesc.getForeignTable());

            //4. update dataModel
            try {

                SCD2NonEquiCondSimplification.INSTANCE.convertToSimplifiedSCD2Cond(suggModelJoin);
                modelJoinDesc.setNonEquiJoinCondition(suggModelJoin.getNonEquiJoinCondition());
                modelJoinDesc.setForeignTable(suggModelJoin.getForeignTable());
                modelJoinDesc.setPrimaryTable(suggModelJoin.getPrimaryTable());
            } catch (SCD2Exception e) {
                logger.error("Update datamodel failed...", e);
                throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR, Throwables.getRootCause(e).getMessage());
            }

            //5. check same join conditions
            if (scd2NonEquiCondSets.contains(new JoinDescNonEquiCompBean(requestJoinDesc))) {
                throw new KylinException(QueryErrorCode.SCD2_DUPLICATE_CONDITION, "duplicate join edge");
            } else {
                scd2NonEquiCondSets.add(new JoinDescNonEquiCompBean(requestJoinDesc));
            }

        }

    }

    private void checkRequestNonEquiJoinConds(final SimplifiedJoinDesc requestJoinDesc) {

        if (!SCD2CondChecker.INSTANCE.checkSCD2EquiJoinCond(requestJoinDesc.getForeignKey(),
                requestJoinDesc.getPrimaryKey())) {
            throw new KylinException(QueryErrorCode.SCD2_EMPTY_EQUI_JOIN, "SCD2 must have one equi join conditon");
        }
        if (!SCD2CondChecker.INSTANCE
                .checkSCD2NonEquiJoinCondPair(requestJoinDesc.getSimplifiedNonEquiJoinConditions())) {
            throw new KylinException(QueryErrorCode.SCD2_DUPLICATE_FK_PK_PAIR, "SCD2 non-equi condition must be pair");
        }
        if (!SCD2CondChecker.INSTANCE.checkFkPkPairUnique(requestJoinDesc)) {
            throw new KylinException(QueryErrorCode.SCD2_DUPLICATE_FK_PK_PAIR, "SCD2 condition must be unqiue");
        }
    }

    private JoinDesc suggNonEquiJoinModel(final KylinConfig kylinConfig, final String project,
            final JoinDesc modelJoinDesc, final SimplifiedJoinDesc requestJoinDesc) {
        String nonEquiSql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(modelJoinDesc,
                requestJoinDesc.getSimplifiedNonEquiJoinConditions());

        BackdoorToggles.addToggle(BackdoorToggles.QUERY_NON_EQUI_JOIN_MODEL_ENABLED, "true");
        AbstractContext context = new ModelCreateContextOfSemiV2(kylinConfig, project, new String[] { nonEquiSql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.executePropose();

        List<AbstractContext.ModelContext> suggModelContexts = smartMaster.getContext().getModelContexts();
        if (CollectionUtils.isEmpty(suggModelContexts) || Objects.isNull(suggModelContexts.get(0).getTargetModel())) {

            AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(nonEquiSql);
            if (Objects.nonNull(accelerateInfo)) {
                log.error("scd2 suggest error, sql:{}", nonEquiSql, accelerateInfo.getFailedCause());
            }
            throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR, "it has illegal join condition");
        }

        if (suggModelContexts.size() != 1) {
            throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR,
                    "scd2 suggest more than one model:" + nonEquiSql);
        }

        return suggModelContexts.get(0).getTargetModel().getJoinTables().get(0).getJoin();
    }

    private List<NDataModel.NamedColumn> convertNamedColumns(String project, NDataModel dataModel,
            ModelRequest modelRequest) {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        List<JoinTableDesc> allTables = Lists.newArrayList();
        val rootFactTable = new JoinTableDesc();
        rootFactTable.setTable(dataModel.getRootFactTableName());
        rootFactTable.setAlias(dataModel.getRootFactTableAlias());
        rootFactTable.setKind(NDataModel.TableKind.FACT);
        allTables.add(rootFactTable);
        allTables.addAll(dataModel.getJoinTables());

        List<NDataModel.NamedColumn> simplifiedColumns = modelRequest.getSimplifiedDimensions();
        Map<String, NDataModel.NamedColumn> dimensionNameMap = Maps.newHashMap();
        for (NDataModel.NamedColumn namedColumn : simplifiedColumns) {
            dimensionNameMap.put(namedColumn.getAliasDotColumn(), namedColumn);
        }
        Map<String, NDataModel.NamedColumn> otherColumnNameMap = Maps.newHashMap();
        for (NDataModel.NamedColumn namedColumn : modelRequest.getOtherColumns()) {
            otherColumnNameMap.put(namedColumn.getAliasDotColumn(), namedColumn);
        }

        int id = 0;
        List<NDataModel.NamedColumn> columns = Lists.newArrayList();
        for (JoinTableDesc joinTable : allTables) {
            val tableDesc = tableManager.getTableDesc(joinTable.getTable());
            boolean isFact = joinTable.getKind() == NDataModel.TableKind.FACT;
            val alias = StringUtils.isEmpty(joinTable.getAlias()) ? tableDesc.getName() : joinTable.getAlias();
            for (ColumnDesc column : modelRequest.getColumnsFetcher().apply(tableDesc, !isFact)) {
                val namedColumn = new NDataModel.NamedColumn();
                namedColumn.setId(id++);
                namedColumn.setName(column.getName());
                namedColumn.setAliasDotColumn(alias + "." + column.getName());
                namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
                val dimension = dimensionNameMap.get(namedColumn.getAliasDotColumn());
                if (dimension != null) {
                    namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                    namedColumn.setName(dimension.getName());
                }
                if (otherColumnNameMap.get(namedColumn.getAliasDotColumn()) != null) {
                    namedColumn.setName(otherColumnNameMap.get(namedColumn.getAliasDotColumn()).getName());
                }
                columns.add(namedColumn);
            }
        }
        Map<String, ComputedColumnDesc> ccMap = dataModel.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getFullName, Function.identity()));
        List<ComputedColumnDesc> orderedCCList = Lists.newArrayList();
        NDataModel originModel = getDataModelManager(project).getDataModelDesc(dataModel.getUuid());
        if (originModel != null && !originModel.isBroken()) {
            originModel.getAllNamedColumns().stream().filter(NamedColumn::isExist)
                    .filter(column -> ccMap.containsKey(column.getAliasDotColumn())) //
                    .forEach(column -> {
                        ComputedColumnDesc cc = ccMap.get(column.getAliasDotColumn());
                        orderedCCList.add(cc);
                        ccMap.remove(column.getAliasDotColumn());
                    });
            orderedCCList.addAll(ccMap.values());
        } else {
            orderedCCList.addAll(dataModel.getComputedColumnDescs());
        }

        for (ComputedColumnDesc computedColumnDesc : orderedCCList) {
            NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
            namedColumn.setId(id++);
            namedColumn.setName(computedColumnDesc.getColumnName());
            namedColumn.setAliasDotColumn(computedColumnDesc.getFullName());
            namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
            val dimension = dimensionNameMap.get(namedColumn.getAliasDotColumn());
            if (dimension != null) {
                namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                namedColumn.setName(dimension.getName());
            }
            columns.add(namedColumn);
        }
        return columns;
    }

    private void updateModelColumnForTableAliasModify(NDataModel model, Map<String, String> matchAlias) {

        for (val kv : matchAlias.entrySet()) {
            String oldAliasName = kv.getKey();
            String newAliasName = kv.getValue();
            if (oldAliasName.equalsIgnoreCase(newAliasName)) {
                continue;
            }

            model.getAllNamedColumns().stream().filter(NamedColumn::isExist)
                    .forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));
            model.getAllMeasures().stream().filter(x -> !x.isTomb())
                    .forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));
            model.getComputedColumnDescs().forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));

            String filterCondition = model.getFilterCondition();
            if (StringUtils.isNotEmpty(filterCondition)) {
                SqlVisitor<Object> modifyAlias = new ModifyTableNameSqlVisitor(oldAliasName, newAliasName);
                SqlNode sqlNode = CalciteParser.getExpNode(filterCondition);
                sqlNode.accept(modifyAlias);
                String newFilterCondition = sqlNode.toSqlString(HiveSqlDialect.DEFAULT).toString();
                model.setFilterCondition(newFilterCondition);
            }
        }
    }

    private Map<String, String> getAliasTransformMap(NDataModel originModel, NDataModel expectModel) {
        Map<String, String> matchAlias = Maps.newHashMap();
        boolean match = originModel.getJoinsGraph().match(expectModel.getJoinsGraph(), matchAlias);
        if (!match) {
            matchAlias.clear();
        }
        return matchAlias;
    }

    private Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toExistMap = allCols -> allCols
            .stream().filter(NDataModel.NamedColumn::isExist)
            .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, Function.identity()));

    private Function<List<NDataModel.Measure>, Map<SimplifiedMeasure, NDataModel.Measure>> toMeasureMap = allCols -> allCols
            .stream().filter(m -> !m.isTomb())
            .collect(Collectors.toMap(SimplifiedMeasure::fromMeasure, Function.identity(), (u, v) -> {
                throw new KylinException(DUPLICATE_MEASURE_EXPRESSION,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDUPLICATE_MEASURE_DEFINITION(), v.getName()));
            }));

    private Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toDimensionMap = allCols -> allCols
            .stream().filter(NDataModel.NamedColumn::isDimension)
            .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, Function.identity()));

    private boolean isValidMeasure(MeasureDesc measure) {
        val funcDesc = measure.getFunction();
        val param = funcDesc.getParameters().get(0);
        if (param.isConstant()) {
            return true;
        }
        val ccDataType = param.getColRef().getType();
        return funcDesc.isDatatypeSuitable(ccDataType);
    }

    private NDataModel updateColumnsInit(NDataModel originModel, ModelRequest request, boolean saveCheck) {
        val expectedModel = convertToDataModel(request);

        String project = request.getProject();
        val allTables = getTableManager(project).getAllTablesMap();
        val initialAllTables = expectedModel.getExtendedTables(allTables);
        expectedModel.init(KylinConfig.getInstanceFromEnv(), initialAllTables);
        Map<String, String> matchAlias = getAliasTransformMap(originModel, expectedModel);
        updateModelColumnForTableAliasModify(expectedModel, matchAlias);

        List<NDataModel> allModels = getDataflowManager(project).listUnderliningDataModels();
        expectedModel.init(KylinConfig.getInstanceFromEnv(), allTables, allModels, project, false, saveCheck);

        originModel.setJoinTables(expectedModel.getJoinTables());
        originModel.setCanvas(expectedModel.getCanvas());
        originModel.setRootFactTableName(expectedModel.getRootFactTableName());
        originModel.setRootFactTableAlias(expectedModel.getRootFactTableAlias());
        originModel.setPartitionDesc(expectedModel.getPartitionDesc());
        originModel.setFilterCondition(expectedModel.getFilterCondition());
        originModel.setMultiPartitionDesc(expectedModel.getMultiPartitionDesc());
        updateModelColumnForTableAliasModify(originModel, matchAlias);

        return expectedModel;
    }

    private boolean equalsIgnoreReturnType(Measure removedOrUpdatedMeasure, Measure newMeasure) {
        val simpleOld = SimplifiedMeasure.fromMeasure(removedOrUpdatedMeasure);
        simpleOld.setReturnType("any");
        val simpleNew = SimplifiedMeasure.fromMeasure(newMeasure);
        simpleNew.setReturnType("any");
        return simpleOld.equals(simpleNew);
    }

    public UpdateImpact updateModelColumns(NDataModel originModel, ModelRequest request) {
        return updateModelColumns(originModel, request, false);
    }

    public UpdateImpact updateModelColumns(NDataModel originModel, ModelRequest request, boolean saveCheck) {
        val expectedModel = updateColumnsInit(originModel, request, saveCheck);
        val updateImpact = new UpdateImpact();
        // handle computed column updates
        List<ComputedColumnDesc> currentComputedColumns = originModel.getComputedColumnDescs();
        List<ComputedColumnDesc> newComputedColumns = expectedModel.getComputedColumnDescs();
        Set<String> removedOrUpdatedComputedColumns = currentComputedColumns.stream()
                .filter(cc -> !newComputedColumns.contains(cc)).map(ComputedColumnDesc::getFullName)
                .collect(Collectors.toSet());
        // move deleted CC's named column to TOMB
        originModel.getAllNamedColumns().stream() //
                .filter(column -> removedOrUpdatedComputedColumns.contains(column.getAliasDotColumn())
                        && column.isExist())
                .forEach(unusedColumn -> {
                    unusedColumn.setStatus(NDataModel.ColumnStatus.TOMB);
                    updateImpact.getRemovedOrUpdatedCCs().add(unusedColumn.getId());
                });
        // move deleted CC's measure to TOMB
        List<Measure> currentMeasures = originModel.getEffectiveMeasures().values().asList();
        currentMeasures.stream().filter(measure -> {
            List<TblColRef> params = measure.getFunction().getColRefs();
            if (CollectionUtils.isEmpty(params)) {
                return false;
            }
            return params.stream().map(TblColRef::getIdentity).anyMatch(removedOrUpdatedComputedColumns::contains);
        }).forEach(unusedMeasure -> {
            unusedMeasure.setTomb(true);
            updateImpact.getInvalidMeasures().add(unusedMeasure.getId());
        });
        originModel.setComputedColumnDescs(expectedModel.getComputedColumnDescs());

        // compare measures
        List<NDataModel.Measure> newMeasures = Lists.newArrayList();
        compareAndUpdateColumns(toMeasureMap.apply(originModel.getAllMeasures()),
                toMeasureMap.apply(expectedModel.getAllMeasures()), newMeasures::add,
                oldMeasure -> oldMeasure.setTomb(true),
                (oldMeasure, newMeasure) -> oldMeasure.setName(newMeasure.getName()));
        updateMeasureStatus(newMeasures, originModel, updateImpact);

        // compare originModel and expectedModel's existing allNamedColumn
        val originExistMap = toExistMap.apply(originModel.getAllNamedColumns());
        val newCols = Lists.<NDataModel.NamedColumn> newArrayList();
        compareAndUpdateColumns(originExistMap, toExistMap.apply(expectedModel.getAllNamedColumns()), newCols::add,
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.TOMB),
                (olCol, newCol) -> olCol.setName(newCol.getName()));
        updateColumnStatus(newCols, originModel, updateImpact);

        // measures invalid due to cc removal
        // should not clear related layouts
        val removedCCs = new HashSet<Integer>();
        removedCCs.addAll(updateImpact.getRemovedOrUpdatedCCs());
        removedCCs.removeAll(updateImpact.getUpdatedCCs());
        updateImpact.getInvalidMeasures().removeIf(measureId -> causedByCCDelete(removedCCs, originModel, measureId));

        // compare originModel and expectedModel's dimensions
        val originDimensionMap = toDimensionMap.apply(originModel.getAllNamedColumns());
        compareAndUpdateColumns(originDimensionMap, toDimensionMap.apply(expectedModel.getAllNamedColumns()),
                newCol -> originExistMap.get(newCol.getAliasDotColumn()).setStatus(NDataModel.ColumnStatus.DIMENSION),
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.EXIST),
                (olCol, newCol) -> olCol.setName(newCol.getName()));

        //Move unused named column to EXIST status
        originModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .filter(column -> request.getSimplifiedDimensions().stream()
                        .noneMatch(dimension -> dimension.getAliasDotColumn().equals(column.getAliasDotColumn())))
                .forEach(c -> c.setStatus(NDataModel.ColumnStatus.EXIST));

        return updateImpact;
    }

    /**
     *  one measure in expectedModel but not in originModel then add one
     *  one in expectedModel, is also a TOMB one in originModel, set status to not TOMB
     * @param newMeasures
     * @param originModel
     * @param updateImpact
     */
    private void updateMeasureStatus(List<Measure> newMeasures, NDataModel originModel, UpdateImpact updateImpact) {
        int maxMeasureId = originModel.getAllMeasures().stream().map(NDataModel.Measure::getId).mapToInt(i -> i).max()
                .orElse(NDataModel.MEASURE_ID_BASE - 1);
        newMeasures.sort(Comparator.comparing(Measure::getId));
        for (NDataModel.Measure measure : newMeasures) {
            Integer modifiedMeasureId = updateImpact.getInvalidMeasures().stream() //
                    .filter(measureId -> equalsIgnoreReturnType(originModel.getTombMeasureById(measureId), measure))
                    .findFirst().orElse(null);
            if (modifiedMeasureId != null) {
                // measure affected by cc modification
                // check if measure is still valid due to cc modification
                if (!isValidMeasure(measure)) {
                    // measure removed due to cc modification
                    continue;
                }
                // measure added/updated is valid, removed from InvalidMeasure
                updateImpact.getInvalidMeasures().remove(modifiedMeasureId);
                val funcDesc = measure.getFunction();
                val ccDataType = funcDesc.getParameters().get(0).getColRef().getType();
                val proposeReturnType = FunctionDesc.proposeReturnType(funcDesc.getExpression(), ccDataType.toString());
                val originReturnType = originModel.getTombMeasureById(modifiedMeasureId).getFunction().getReturnType();
                if (!originReturnType.equals(proposeReturnType)) {
                    // measure return type change, assign new id
                    val newFuncDesc = FunctionDesc.newInstance(funcDesc.getExpression(), funcDesc.getParameters(),
                            proposeReturnType);
                    measure.setFunction(newFuncDesc);
                    maxMeasureId++;
                    measure.setId(maxMeasureId);
                    originModel.getAllMeasures().add(measure);
                    updateImpact.getReplacedMeasures().put(modifiedMeasureId, maxMeasureId);
                } else {
                    // measure return type not change, id not change
                    originModel.getTombMeasureById(modifiedMeasureId).setTomb(false);
                    updateImpact.getUpdatedMeasures().add(modifiedMeasureId);
                }
            } else {
                // new added measure
                if (isValidMeasure(measure)) {
                    maxMeasureId++;
                    measure.setId(maxMeasureId);
                    originModel.getAllMeasures().add(measure);
                } else {
                    updateImpact.getInvalidRequestMeasures().add(measure.getId());
                }
            }
        }
    }

    /**
     * one in expectedModel, is also a TOMB one in originModel, set status as the expected's
     * @param newCols
     * @param originModel
     * @param updateImpact
     */
    private void updateColumnStatus(List<NDataModel.NamedColumn> newCols, NDataModel originModel,
            UpdateImpact updateImpact) {
        int maxId = originModel.getAllNamedColumns().stream().map(NamedColumn::getId).mapToInt(i -> i).max().orElse(-1);
        for (NDataModel.NamedColumn newCol : newCols) {
            val modifiedColId = updateImpact.getRemovedOrUpdatedCCs().stream() //
                    .filter(modifiedId -> newCol.getAliasDotColumn()
                            .equals(originModel.getTombColumnNameById(modifiedId)))
                    .findFirst().orElse(null);
            if (modifiedColId != null) {
                // column affected by cc modification
                val modifiedColumn = originModel.getAllNamedColumns().stream().filter(c -> c.getId() == modifiedColId)
                        .findFirst().orElse(null);
                if (modifiedColumn != null) {
                    modifiedColumn.setStatus(newCol.getStatus());
                    updateImpact.getUpdatedCCs().add(modifiedColId);
                }
            } else {
                maxId++;
                newCol.setId(maxId);
                originModel.getAllNamedColumns().add(newCol);
            }
        }
    }

    /**
     * if a measure becomes invalid because of cc delete,
     * the measure and related aggGroups/layouts should remains
     * @param removedCCs
     * @param originModel
     * @param measureId
     * @return
     */
    private boolean causedByCCDelete(Set<Integer> removedCCs, NDataModel originModel, int measureId) {
        for (int ccId : removedCCs) {
            val colName = originModel.getTombColumnNameById(ccId);
            val funcParams = originModel.getTombMeasureById(measureId).getFunction().getParameters();
            for (val funcParam : funcParams) {
                val funcColName = funcParam.getColRef().getIdentity();
                if (StringUtils.equalsIgnoreCase(funcColName, colName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private <K, T> void compareAndUpdateColumns(Map<K, T> origin, Map<K, T> target, Consumer<T> onlyInTarget,
            Consumer<T> onlyInOrigin, BiConsumer<T, T> inBoth) {
        for (Map.Entry<K, T> entry : target.entrySet()) {
            // change name does not matter
            val matched = origin.get(entry.getKey());
            if (matched == null) {
                onlyInTarget.accept(entry.getValue());
            } else {
                inBoth.accept(matched, entry.getValue());
            }
        }
        for (Map.Entry<K, T> entry : origin.entrySet()) {
            val matched = target.get(entry.getKey());
            if (matched == null) {
                onlyInOrigin.accept(entry.getValue());
            }
        }

    }

    private List<NDataModel.Measure> convertMeasure(List<SimplifiedMeasure> simplifiedMeasures) {
        List<NDataModel.Measure> measures = new ArrayList<>();
        boolean hasCountAll = false;
        int id = NDataModel.MEASURE_ID_BASE;
        if (simplifiedMeasures == null) {
            simplifiedMeasures = Lists.newArrayList();
        }
        for (SimplifiedMeasure simplifiedMeasure : simplifiedMeasures) {
            val measure = simplifiedMeasure.toMeasure();
            measure.setId(id);
            measures.add(measure);
            val functionDesc = measure.getFunction();
            if (functionDesc.isCount() && !functionDesc.isCountOnColumn()) {
                hasCountAll = true;
            }
            id++;
        }
        if (!hasCountAll) {
            FunctionDesc functionDesc = new FunctionDesc();
            ParameterDesc parameterDesc = new ParameterDesc();
            parameterDesc.setType("constant");
            parameterDesc.setValue("1");
            functionDesc.setParameters(Lists.newArrayList(parameterDesc));
            functionDesc.setExpression("COUNT");
            functionDesc.setReturnType("bigint");
            NDataModel.Measure measure = CubeUtils.newMeasure(functionDesc, "COUNT_ALL", id);
            measures.add(measure);
        }
        return measures;
    }

    public void handleSemanticUpdate(String project, String model, NDataModel originModel, String start, String end) {
        handleSemanticUpdate(project, model, originModel, start, end, false);
    }

    public void handleSemanticUpdate(String project, String model, NDataModel originModel, String start, String end,
            boolean saveOnly) {
        val needBuild = doHandleSemanticUpdate(project, model, originModel, start, end);
        if (!saveOnly && needBuild) {
            buildForModel(project, model);
        }
    }

    public boolean doHandleSemanticUpdate(String project, String model, NDataModel originModel, String start,
                                          String end){
        return doHandleSemanticUpdate(project, model, originModel, start, end, true).getFirst();
    }

    public Pair<Boolean, Boolean> doHandleSemanticUpdate(String project, String model, NDataModel originModel, String start,
                                                         String end, boolean needCleanSecondStorage) {
        val config = KylinConfig.getInstanceFromEnv();
        val indePlanManager = NIndexPlanManager.getInstance(config, project);
        val modelMgr = NDataModelManager.getInstance(config, project);
        val optRecManagerV2 = OptRecManagerV2.getInstance(project);

        val indexPlan = indePlanManager.getIndexPlan(model);
        val newModel = modelMgr.getDataModelDesc(model);

        if (isSignificantChange(originModel, newModel)) {
            log.info("model {} reload data from datasource", originModel.getAlias());
            val savedIndexPlan = handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasures().keySet(),
                    indePlanManager);
            removeUselessDimensions(savedIndexPlan, newModel.getEffectiveDimensions().keySet(), false, config);
            modelMgr.updateDataModel(newModel.getUuid(),
                    copyForWrite -> copyForWrite.setSemanticVersion(copyForWrite.getSemanticVersion() + 1));
            boolean isClean = handleReloadData(newModel, originModel, project, start, end, needCleanSecondStorage);
            optRecManagerV2.discardAll(model);
            return Pair.newPair(true, isClean);
        }

        // measure changed: does not matter to auto created cuboids' data, need refresh rule based cuboids
        if (isMeasureChange(originModel, newModel)) {
            handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasures().keySet(), indePlanManager);
        }
        // dimension deleted: previous step is remove dimensions in rule,
        //   so we only remove the auto created cuboids
        if (isDimNotOnlyAdd(originModel, newModel)) {
            removeUselessDimensions(indexPlan, newModel.getEffectiveDimensions().keySet(), true, config);
        }

        boolean result = hasRulebaseLayoutChange(indexPlan.getRuleBasedIndex(),
                indePlanManager.getIndexPlan(indexPlan.getId()).getRuleBasedIndex());
        return Pair.newPair(result, false);
    }

    public boolean isDimNotOnlyAdd(NDataModel originModel, NDataModel newModel) {
        return !newModel.getEffectiveDimensions().keySet().containsAll(originModel.getEffectiveDimensions().keySet());
    }

    public boolean isMeasureChange(NDataModel originModel, NDataModel newModel) {
        return !CollectionUtils.isEqualCollection(newModel.getEffectiveMeasures().keySet(),
                originModel.getEffectiveMeasures().keySet());
    }

    public boolean isFilterConditonNotChange(String oldFilterCondition, String newFilterCondition) {
        oldFilterCondition = oldFilterCondition == null ? "" : oldFilterCondition;
        newFilterCondition = newFilterCondition == null ? "" : newFilterCondition;
        return StringUtils.trim(oldFilterCondition).equals(StringUtils.trim(newFilterCondition));
    }

    public static boolean isMultiPartitionDescSame(MultiPartitionDesc oldPartitionDesc,
            MultiPartitionDesc newPartitionDesc) {
        String oldColumns = oldPartitionDesc == null ? "" : StringUtils.join(oldPartitionDesc.getColumns(), ",");
        String newColumns = newPartitionDesc == null ? "" : StringUtils.join(newPartitionDesc.getColumns(), ",");
        return oldColumns.equals(newColumns);
    }

    public static boolean isAntiFlattenableSame(List<JoinTableDesc> oldJoinTables, List<JoinTableDesc> newJoinTables) {
        Map<JoinDesc, JoinTableDesc> newJoinMap = newJoinTables.stream()
                .collect(Collectors.toMap(JoinTableDesc::getJoin, Function.identity()));
        boolean sameAntiFlattenable = true;
        for (JoinTableDesc oldJoinTable : oldJoinTables) {
            JoinDesc join = oldJoinTable.getJoin();
            if (newJoinMap.containsKey(join)) {
                JoinTableDesc newJoinTable = newJoinMap.get(join);
                if (oldJoinTable.hasDifferentAntiFlattenable(newJoinTable)) {
                    sameAntiFlattenable = false;
                    break;
                }
            }
        }
        return sameAntiFlattenable;
    }

    // if partitionDesc, mpCol, joinTable, FilterCondition changed, we need reload data from datasource
    private boolean isSignificantChange(NDataModel originModel, NDataModel newModel) {
        return isDifferent(originModel.getPartitionDesc(), newModel.getPartitionDesc())
                || !Objects.equals(originModel.getRootFactTable(), newModel.getRootFactTable())
                || !originModel.getJoinsGraph().match(newModel.getJoinsGraph(), Maps.newHashMap())
                || !isFilterConditonNotChange(originModel.getFilterCondition(), newModel.getFilterCondition())
                || !isMultiPartitionDescSame(originModel.getMultiPartitionDesc(), newModel.getMultiPartitionDesc())
                || !isAntiFlattenableSame(originModel.getJoinTables(), newModel.getJoinTables());
    }

    private boolean isDifferent(PartitionDesc p1, PartitionDesc p2) {
        boolean isP1Null = p1 == null || p1.isEmpty();
        boolean isP2Null = p2 == null || p2.isEmpty();
        if (isP1Null && isP2Null) {
            return false;
        }
        return !Objects.equals(p1, p2);
    }

    private IndexPlan handleMeasuresChanged(IndexPlan indexPlan, Set<Integer> measures,
            NIndexPlanManager indexPlanManager) {
        return indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream()
                    .filter(index -> measures.containsAll(index.getMeasures())).collect(Collectors.toList()));
            if (copyForWrite.getRuleBasedIndex() == null) {
                return;
            }
            val newRule = JsonUtil.deepCopyQuietly(copyForWrite.getRuleBasedIndex(), RuleBasedIndex.class);
            newRule.setLayoutIdMapping(Lists.newArrayList());

            if (newRule.getAggregationGroups() != null) {
                for (NAggregationGroup aggGroup : newRule.getAggregationGroups()) {
                    val aggMeasures = Sets.newHashSet(aggGroup.getMeasures());
                    aggGroup.setMeasures(Sets.intersection(aggMeasures, measures).toArray(new Integer[0]));
                }
            }

            copyForWrite.setRuleBasedIndex(newRule);
        });
    }

    private void removeUselessDimensions(IndexPlan indexPlan, Set<Integer> availableDimensions, boolean onlyDataflow,
            KylinConfig config) {
        val dataflowManager = NDataflowManager.getInstance(config, indexPlan.getProject());
        val deprecatedLayoutIds = indexPlan.getIndexes().stream().filter(index -> !index.isTableIndex())
                .filter(index -> !availableDimensions.containsAll(index.getDimensions()))
                .flatMap(index -> index.getLayouts().stream().map(LayoutEntity::getId)).collect(Collectors.toSet());
        val toBeDeletedLayoutIds = indexPlan.getToBeDeletedIndexes().stream().filter(index -> !index.isTableIndex())
                .filter(index -> !availableDimensions.containsAll(index.getDimensions()))
                .flatMap(index -> index.getLayouts().stream().map(LayoutEntity::getId)).collect(Collectors.toSet());
        deprecatedLayoutIds.addAll(toBeDeletedLayoutIds);
        if (deprecatedLayoutIds.isEmpty()) {
            return;
        }
        if (onlyDataflow) {
            val df = dataflowManager.getDataflow(indexPlan.getUuid());
            dataflowManager.removeLayouts(df, deprecatedLayoutIds);
            if (CollectionUtils.isNotEmpty(toBeDeletedLayoutIds)) {
                val indexPlanManager = NIndexPlanManager.getInstance(config, indexPlan.getProject());
                indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                    copyForWrite.removeLayouts(deprecatedLayoutIds, true, true);
                });
            }
        } else {
            val indexPlanManager = NIndexPlanManager.getInstance(config, indexPlan.getProject());
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                copyForWrite.removeLayouts(deprecatedLayoutIds, true, true);
                copyForWrite.removeLayouts(deprecatedLayoutIds, true, true);
            });
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelId, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelId).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    private boolean handleReloadData(NDataModel newModel, NDataModel oriModel, String project, String start, String end,
                                     boolean needCleanSecondStorage) {
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        var df = dataflowManager.getDataflow(newModel.getUuid());
        val segments = df.getFlatSegments();

        dataflowManager.updateDataflow(df.getUuid(), copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        boolean isClean = cleanModelWithSecondStorage(newModel.getUuid(), project, needCleanSecondStorage);

        String modelId = newModel.getUuid();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        if (newModel.isMultiPartitionModel() || oriModel.isMultiPartitionModel()) {
            boolean isPartitionChange = !isMultiPartitionDescSame(oriModel.getMultiPartitionDesc(),
                    newModel.getMultiPartitionDesc())
                    || !Objects.equals(oriModel.getPartitionDesc(), newModel.getPartitionDesc());
            if (isPartitionChange && newModel.isMultiPartitionModel()) {
                modelManager.updateDataModel(modelId, copyForWrite -> {
                    copyForWrite.setMultiPartitionDesc(
                            new MultiPartitionDesc(newModel.getMultiPartitionDesc().getColumns()));
                });
            }
        } else {
            if (!Objects.equals(oriModel.getPartitionDesc(), newModel.getPartitionDesc())) {

                // from having partition to no partition
                if (newModel.getPartitionDesc() == null) {
                    dataflowManager.fillDfManually(df,
                            Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
                    // change partition column and from no partition to having partition
                } else if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
                    dataflowManager.fillDfManually(df,
                            Lists.newArrayList(getSegmentRangeByModel(project, modelId, start, end)));
                }
            } else {
                List<SegmentRange> segmentRanges = Lists.newArrayList();
                segments.forEach(segment -> segmentRanges.add(segment.getSegRange()));
                dataflowManager.fillDfManually(df, segmentRanges);
            }
        }
        return isClean;
    }

    private boolean cleanModelWithSecondStorage(String modelId, String project, boolean needCleanSecondStorage) {
        if (SecondStorageUtil.isModelEnable(project, modelId)) {
            SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);
            return updater.onUpdate(project, modelId, needCleanSecondStorage);
        }
        return false;
    }

    public BuildIndexResponse handleIndexPlanUpdateRule(String project, String model, RuleBasedIndex oldRule,
            RuleBasedIndex newRule, boolean forceFireEvent) {
        log.debug("handle indexPlan udpate rule {} {}", project, model);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(model);
        val readySegs = df.getSegments();
        if (readySegs.isEmpty()) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
        }
        val jobManager = JobManager.getInstance(kylinConfig, project);

        // new cuboid
        if (hasRulebaseLayoutChange(oldRule, newRule) || forceFireEvent) {
            String jobId = jobManager.addIndexJob(new JobParam(model, getUsername()));

            val buildIndexResponse = new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD, jobId);

            if (Objects.isNull(jobId)) {
                buildIndexResponse.setWarnCodeWithSupplier(FAILED_CREATE_JOB_SAVE_INDEX_SUCCESS);
            }

            return buildIndexResponse;
        }

        return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);

    }

    private boolean hasRulebaseLayoutChange(RuleBasedIndex oldRule, RuleBasedIndex newRule) {
        val originLayouts = oldRule == null ? Sets.<LayoutEntity> newHashSet() : oldRule.genCuboidLayouts();
        val targetLayouts = newRule == null ? Sets.<LayoutEntity> newHashSet() : newRule.genCuboidLayouts();
        val difference = Sets.difference(targetLayouts, originLayouts);
        return difference.size() > 0;
    }

    public IndexPlan addRuleBasedIndexBlackListLayouts(IndexPlan indexPlan, Collection<Long> blackListLayoutIds) {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), indexPlan.getProject());
        return indexPlanManager.updateIndexPlan(indexPlan.getId(), indexPlanCopy -> {
            indexPlanCopy.addRuleBasedBlackList(blackListLayoutIds);
        });
    }

    public void buildForModel(String project, String modelId) {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getIndexPlan(modelId);
        if (CollectionUtils.isNotEmpty(indexPlan.getAllLayoutIds(false))) {
            JobManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .addIndexJob(new JobParam(modelId, getUsername()));
        }
    }

}
