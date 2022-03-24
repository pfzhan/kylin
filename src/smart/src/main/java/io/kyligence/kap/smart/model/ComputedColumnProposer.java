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

package io.kyligence.kap.smart.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.TableColRefWithRel;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.AbstractContext.ModelContext;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnProposer extends AbstractModelProposer {

    ComputedColumnProposer(ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void execute(NDataModel dataModel) {
        log.trace("Propose computed column for model({})", dataModel.getId());
        long startTime = System.currentTimeMillis();

        // pre-init to construct join-tree
        initModel(dataModel);
        Set<String> ccSuggestions = collectLatentCCSuggestions(modelContext, dataModel);
        transferStatusOfNeedUpdateCC(ccSuggestions);
        List<ComputedColumnDesc> newValidCCList = transferToComputedColumn(dataModel, ccSuggestions);
        evaluateTypeOfComputedColumns(dataModel, newValidCCList);
        cleanInvalidComputedColumnsInModel(dataModel);
        collectCCRecommendations(newValidCCList);

        log.info("Propose ComputedColumns successfully completed in {} s. Valid ComputedColumns on model({}) are: {}.",
                (System.currentTimeMillis() - startTime) / 1000, dataModel.getId(), dataModel.getComputedColumnNames());
    }

    private Set<String> collectLatentCCSuggestions(ModelContext modelContext, NDataModel dataModel) {
        Set<String> ccSuggestions = Sets.newHashSet();
        ModelTree modelTree = modelContext.getModelTree();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matchJoins(dataModel, ctx);
            ctx.fixModel(dataModel, matchingAlias);
            Set<TblColRef> innerColumns = collectInnerColumns(ctx);
            innerColumns.removeIf(col -> !col.isInnerColumn());
            ccSuggestions.addAll(translateToSuggestions(innerColumns, matchingAlias));
            ctx.unfixModel();
        }

        log.info("Proposed computed column candidates {} for model [{}] successfully", ccSuggestions,
                dataModel.getId());
        return ccSuggestions;
    }

    private void transferStatusOfNeedUpdateCC(Set<String> latentCCSuggestions) {
        if (!latentCCSuggestions.isEmpty()) {
            modelContext.setNeedUpdateCC(true);
        }
    }

    private List<ComputedColumnDesc> transferToComputedColumn(NDataModel dataModel, Set<String> ccSuggestions) {
        List<ComputedColumnDesc> validCCs = Lists.newArrayList();
        if (ccSuggestions.isEmpty()) {
            return validCCs;
        }

        Map<String, String> ccInnerExpToUniqueFlag = modelContext.getUniqueContentToFlag();
        List<NDataModel> otherModels = getOtherModels(dataModel);
        int index = 0;
        long currentTs = System.currentTimeMillis();
        for (String ccSuggestion : ccSuggestions) {
            ++index;
            ComputedColumnDesc ccDesc = modelContext.getUsedCC().get(ccSuggestion);
            // In general, cc expressions in the SQL statements should have been replaced in transformers,
            // however, it could not be replaced when meets some corner cases(#11411). As a result, it will
            // lead to add the same CC more than once and fail to accelerate current sql statements.
            if (ccDesc != null) {
                continue;
            }
            ccDesc = new ComputedColumnDesc();
            ccDesc.setTableIdentity(dataModel.getRootFactTable().getTableIdentity());
            ccDesc.setTableAlias(dataModel.getRootFactTableAlias());
            ccDesc.setComment("Auto suggested from: " + ccSuggestion);
            ccDesc.setDatatype("ANY"); // resolve data type later
            ccDesc.setExpression(ccSuggestion);
            String newCCName = ComputedColumnUtil.shareCCNameAcrossModel(ccDesc, dataModel, otherModels);
            if (newCCName != null) {
                ccDesc.setColumnName(newCCName);
            } else {
                String name = ComputedColumnUtil.newAutoCCName(currentTs, index);
                ccDesc.setColumnName(name);
            }

            String innerExpression = KapQueryUtil.massageComputedColumn(dataModel, project, ccDesc, null);
            ccDesc.setInnerExpression(innerExpression);
            if (ccInnerExpToUniqueFlag.containsKey(innerExpression)) {
                ccDesc.setUuid(ccInnerExpToUniqueFlag.get(innerExpression));
            } else {
                ccDesc.setUuid(String.format(Locale.ROOT, "tmp_%s_%s", currentTs, index));
            }
            dataModel.getComputedColumnDescs().add(ccDesc);

            if (ComputedColumnEvalUtil.resolveCCName(ccDesc, dataModel, otherModels)) {
                validCCs.add(ccDesc);
                modelContext.getUsedCC().put(ccDesc.getExpression(), ccDesc);
            } else {
                // For example: If a malformed computed column expression of
                // `CASE(IN($3, 'Auction', 'FP-GTC'), 'Auction', $3)`
                // has been added to the model, we should discard it, otherwise, all suggestions after this round
                // cannot resolve ccName for model initialization failed.
                dataModel.getComputedColumnDescs().remove(ccDesc);
                log.debug("Malformed computed column {} has been removed from the model {}.", //
                        ccDesc, dataModel.getUuid());
            }
        }
        return validCCs;
    }

    /**
     * There are three kind of CC need remove:
     * 1. invalid CC: something wrong happened in resolving name
     * 2. unsupported CC: something wrong happened in inferring type
     * 3. the type of CC is ANY: something unlikely thing happened in inferring type
     */
    private void cleanInvalidComputedColumnsInModel(NDataModel dataModel) {
        dataModel.getComputedColumnDescs().removeIf(cc -> cc.getDatatype().equals("ANY"));
    }

    private Set<TblColRef> collectInnerColumns(OLAPContext context) {
        Set<TblColRef> usedCols = Sets.newHashSet();
        usedCols.addAll(context.allColumns);

        context.aggregations.stream() // collect inner columns from agg metrics
                .filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters()))
                .forEach(agg -> usedCols.addAll(agg.getColRefs()));
        // collect inner columns from group keys
        usedCols.addAll(getGroupByInnerColumns(context));
        // collect inner columns from filter keys
        if (modelContext.getProposeContext().getSmartConfig().enableComputedColumnOnFilterKeySuggestion()) {
            usedCols.addAll(getFilterInnerColumns(context));
        }
        return usedCols;
    }

    private void collectCCRecommendations(List<ComputedColumnDesc> computedColumns) {
        if (!modelContext.getProposeContext().needCollectRecommendations()
                || CollectionUtils.isEmpty(computedColumns)) {
            return;
        }

        computedColumns.forEach(cc -> {
            CCRecItemV2 item = new CCRecItemV2();
            item.setCc(cc);
            item.setCreateTime(System.currentTimeMillis());
            item.setUuid(cc.getUuid());
            item.setUniqueContent(cc.getInnerExpression());
            modelContext.getCcRecItemMap().putIfAbsent(item.getUuid(), item);
        });
    }

    protected Set<String> translateToSuggestions(Set<TblColRef> innerColumns, Map<String, String> matchingAlias) {
        if (isAnyCCExpDependsOnLookupTable(innerColumns)) {
            return Sets.newHashSet();
        }
        Set<String> candidates = Sets.newHashSet();
        for (TblColRef col : innerColumns) {
            String parserDesc = col.getParserDescription();
            parserDesc = matchingAlias.entrySet().stream()
                    .map(entry -> (Function<String, String>) s -> s.replaceAll(entry.getKey(), entry.getValue()))
                    .reduce(Function.identity(), Function::andThen).apply(parserDesc);
            log.trace(parserDesc);
            candidates.add(parserDesc);
        }
        return candidates;
    }

    private boolean isAnyCCExpDependsOnLookupTable(Set<TblColRef> innerColumns) {
        boolean dependsOnLookupTable = false;
        for (TblColRef col : innerColumns) {
            if (modelContext.getChecker().isCCDependsLookupTable(col)) {
                dependsOnLookupTable = true;
                break;
            }
        }
        return dependsOnLookupTable;
    }

    private Collection<TblColRef> getFilterInnerColumns(OLAPContext context) {
        Collection<TblColRef> resultSet = new HashSet<>();
        for (TblColRef innerColRef : context.getInnerFilterColumns()) {
            Set<TblColRef> filterSourceColumns = innerColRef.getSourceColumns();
            if (!innerColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(filterSourceColumns, modelContext.getProposeContext().getSmartConfig()
                            .getComputedColumnOnFilterKeySuggestionMinCardinality())) {

                // if the inner filter column contains columns from group keys
                // and the inner filter column also appears in the select clause,
                // the the CC replacement will produce a wrong aggregation form.
                // eg.
                // BEFORE: select expr(a) from tbl where expr(a) > 100 group by a
                // AFTER: select CC_1 from tbl where CC_1 > 100 group by a
                // Thus we add a simple check here to ensure that inner filter column
                // does not contain columns from group by clause
                // see: https://github.com/Kyligence/KAP/issues/14072
                // remove this once issue #14072 is fixed
                filterSourceColumns.retainAll(context.getGroupByColumns());
                if (filterSourceColumns.isEmpty()) {
                    resultSet.add(innerColRef);
                }
            }
        }
        return resultSet;
    }

    private Collection<TblColRef> getGroupByInnerColumns(OLAPContext context) {
        Collection<TblColRef> resultSet = new HashSet<>();
        for (TableColRefWithRel groupByColRefWithRel : context.getInnerGroupByColumns()) {
            TblColRef groupByColRef = groupByColRefWithRel.getTblColRef();
            Set<TblColRef> groupSourceColumns = groupByColRef.getSourceColumns();
            if (!groupByColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(groupSourceColumns, modelContext.getProposeContext().getSmartConfig()
                            .getComputedColumnOnGroupKeySuggestionMinCardinality())) {
                resultSet.add(groupByColRef);
            }
        }
        return resultSet;
    }

    /**
     * check and ensure that the cardinality of input cols is greater than or equal to the minCardinality
     * If the cardinality of a column is missing, return config "computed-column.suggestion.enabled-if-no-sampling"
     * @param colRefs TblColRef set
     * @param minCardinality minCardinality
     */
    private boolean checkColumnsMinCardinality(Collection<TblColRef> colRefs, long minCardinality) {
        for (TblColRef colRef : colRefs) {
            long colCardinality = getColumnCardinality(colRef);
            if (colCardinality == -1) {
                return this.getModelContext().getProposeContext().getSmartConfig().needProposeCcIfNoSampling();
            }
            if (colCardinality >= minCardinality) {
                return true;
            }
            minCardinality = minCardinality / colCardinality;
        }
        return false;
    }

    private long getColumnCardinality(TblColRef colRef) {
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        TableExtDesc.ColumnStats columnStats = TableExtDesc.ColumnStats.getColumnStats(nTableMetadataManager, colRef);
        return columnStats == null ? -1 : columnStats.getCardinality();
    }

    private void evaluateTypeOfComputedColumns(NDataModel dataModel, List<ComputedColumnDesc> validCCs) {
        if (modelContext.getProposeContext().isSkipEvaluateCC() || validCCs.isEmpty()) {
            return;
        }
        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(dataModel, validCCs);
    }

    private List<NDataModel> getOtherModels(NDataModel dataModel) {
        List<NDataModel> otherModels = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                .listAllModels().stream() //
                .filter(m -> !m.getUuid().equals(dataModel.getUuid())) //
                .collect(Collectors.toList());
        otherModels.addAll(//
                getModelContext().getProposeContext() //
                        .getModelContexts().stream() //
                        .filter(modelContext -> modelContext != getModelContext()) //
                        .map(ModelContext::getTargetModel) //
                        .filter(Objects::nonNull) //
                        .collect(Collectors.toList()) //
        );
        return otherModels;
    }

    public static class ComputedColumnProposerOfModelReuseContext extends ComputedColumnProposer {

        ComputedColumnProposerOfModelReuseContext(ModelContext modelContext) {
            super(modelContext);
        }

        @Override
        protected Set<String> translateToSuggestions(Set<TblColRef> usedCols, Map<String, String> matchingAlias) {
            Set<String> candidates = super.translateToSuggestions(usedCols, matchingAlias);
            for (TblColRef col : usedCols) {
                // if create totally new model, it won't reuse CC. So it need to be create its own new ComputedCols.
                if (col.getColumnDesc().isComputedColumn()) {
                    try {
                        candidates.add(CalciteParser.transformDoubleQuote(col.getExpressionInSourceDB()));
                    } catch (Exception e) {
                        log.warn("fail to acquire formatted cc from expr {}", col.getExpressionInSourceDB());
                    }
                }
            }
            return candidates;
        }
    }
}