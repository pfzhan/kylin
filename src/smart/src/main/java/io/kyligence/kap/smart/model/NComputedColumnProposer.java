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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.TableColRefWIthRel;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;

public class NComputedColumnProposer extends NAbstractModelProposer {

    NComputedColumnProposer(NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(NDataModel nDataModel) {
        logger.trace("Propose computed column for model [{}]", nDataModel.getId());

        List<NDataModel> otherModels = NDataflowManager.getInstance(kylinConfig, project) //
                .listUnderliningDataModels().stream() //
                .filter(m -> !m.getUuid().equals(nDataModel.getUuid())) //
                .collect(Collectors.toList());
        otherModels.addAll(getModelContext().getSmartContext() //
                .getModelContexts().stream() //
                .filter(ctx -> ctx != getModelContext()) //
                .map(NModelContext::getTargetModel) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList()) //
        );

        // pre-init to construct join-tree
        initModel(nDataModel);
        Set<String> ccSuggestions = collectComputedColumnSuggestion(modelContext, nDataModel);
        logger.info("Proposed computed column candidates {} for model [{}] successfully", ccSuggestions,
                nDataModel.getId());
        if (ccSuggestions.isEmpty()) {
            return;
        }

        TableRef rootTable = nDataModel.getRootFactTable();
        List<ComputedColumnDesc> validCCs = Lists.newArrayList();
        for (String ccSuggestion : ccSuggestions) {
            ComputedColumnDesc ccDesc = modelContext.getUsedCC().get(ccSuggestion);

            // In general, cc expressions in the SQL statements should have been replaced in transformers,
            // however, it could not be replaced when meets some corner cases(#11411). As a result, it will
            // lead to add the same CC more than once and fail to accelerate current sql statements.
            if (ccDesc != null) {
                continue;
            }

            ccDesc = new ComputedColumnDesc();
            ccDesc.setColumnName(ComputedColumnEvalUtil.DEFAULT_CC_NAME);
            ccDesc.setTableIdentity(rootTable.getTableIdentity());
            ccDesc.setTableAlias(nDataModel.getRootFactTableAlias());
            ccDesc.setComment("Auto suggested from: " + ccSuggestion);
            ccDesc.setDatatype("ANY"); // resolve data type later
            ccDesc.setExpression(ccSuggestion);
            ccDesc.setInnerExpression(KapQueryUtil.massageComputedColumn(nDataModel, project, ccDesc));
            nDataModel.getComputedColumnDescs().add(ccDesc);

            if (ComputedColumnEvalUtil.resolveCCName(ccDesc, nDataModel, otherModels, kylinConfig, project)) {
                validCCs.add(ccDesc);
                modelContext.getUsedCC().put(ccDesc.getExpression(), ccDesc);
            } else {
                // For example: If a malformed computed column expression of
                // `CASE(IN($3, 'Auction', 'FP-GTC'), 'Auction', $3)`
                // has been added to the model, we should discard it, otherwise, all suggestions after this round
                // cannot resolve ccName for model initialization failed.
                nDataModel.getComputedColumnDescs().remove(ccDesc);
                logger.debug("Malformed computed column {} has been removed from the model {}.", //
                        ccDesc, nDataModel.getUuid());
            }
        }

        if (!modelContext.getSmartContext().isSkipEvaluateCC() && !validCCs.isEmpty()) {
            ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, validCCs);
        }

        // there are three kind of CC need remove:
        // 1. invalid CC: something wrong happened in resolving name
        // 2. unsupported CC: something wrong happened in inferring type
        // 3. the type of CC is ANY: something unlikely thing happened in inferring type
        nDataModel.getComputedColumnDescs().removeIf(cc -> cc.getDatatype().equals("ANY"));
        logger.info("There are valid computed columns {} for model [{}] after validation",
                nDataModel.getComputedColumnNames(), nDataModel.getId());
    }

    private Set<String> collectComputedColumnSuggestion(NModelContext modelContext, NDataModel nDataModel) {
        Set<String> ccSuggestions = Sets.newHashSet();

        ModelTree modelTree = modelContext.getModelTree();

        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matchJoins(nDataModel, ctx);
            ctx.fixModel(nDataModel, matchingAlias);
            ccSuggestions.addAll(collectInnerColumnCandidate(ctx, matchingAlias));
            ctx.unfixModel();
        }

        return ccSuggestions;
    }

    private Set<String> collectInnerColumnCandidate(OLAPContext context, Map<String, String> matchingAlias) {
        Set<TblColRef> usedCols = Sets.newHashSet();
        Set<String> candidates = Sets.newHashSet();
        usedCols.addAll(context.allColumns);

        context.aggregations.stream() // collect inner columns from agg metrics
                .filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters()))
                .forEach(agg -> usedCols.addAll(agg.getColRefs()));
        // collect inner columns from group keys
        usedCols.addAll(getGroupByInnerColumns(context));
        // collect inner columns from filter keys
        if (modelContext.getSmartContext().getSmartConfig().enableComputedColumnOnFilterKeySuggestion()) {
            usedCols.addAll(getFilterInnerColumns(context));
        }

        for (TblColRef col : usedCols) {
            if (col.isInnerColumn()) {
                String parserDesc = col.getParserDescription();
                parserDesc = matchingAlias.entrySet().stream()
                        .map(entry -> (Function<String, String>) s -> s.replaceAll(entry.getKey(), entry.getValue()))
                        .reduce(Function.identity(), Function::andThen).apply(parserDesc);
                logger.trace(parserDesc);
                candidates.add(parserDesc);
            }
        }
        return candidates;
    }

    private Collection<TblColRef> getFilterInnerColumns(OLAPContext context) {
        Collection<TblColRef> resultSet = new HashSet<>();
        for (TblColRef innerColRef : context.getInnerFilterColumns()) {
            Set<TblColRef> filterSourceColumns = innerColRef.getSourceColumns();
            if (!innerColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(filterSourceColumns, modelContext.getSmartContext().getSmartConfig()
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
        for (TableColRefWIthRel groupByColRefWithRel : context.getInnerGroupByColumns()) {
            TblColRef groupByColRef = groupByColRefWithRel.getTblColRef();
            Set<TblColRef> groupSourceColumns = groupByColRef.getSourceColumns();
            if (!groupByColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(groupSourceColumns, modelContext.getSmartContext().getSmartConfig()
                            .getComputedColumnOnGroupKeySuggestionMinCardinality())) {
                resultSet.add(groupByColRef);
            }
        }
        return resultSet;
    }

    /**
     * check and ensure that the cardinality of input cols is greater than or equal to the minCardinality
     * If the cardinality of a column is missing, return true
     * @param colRefs
     * @param minCardinality
     * @return
     */
    private boolean checkColumnsMinCardinality(Collection<TblColRef> colRefs, long minCardinality) {
        for (TblColRef colRef : colRefs) {
            long colCardinality = getColumnCardinality(colRef);
            if (colCardinality == -1) {
                return true;
            }
            if (colCardinality >= minCardinality) {
                return true;
            }
            minCardinality = minCardinality / colCardinality;
        }
        return false;
    }

    private long getColumnCardinality(TblColRef colRef) {
        NTableMetadataManager nTableMetadataManager = modelContext.getSmartContext().getTableMetadataManager();
        TableExtDesc.ColumnStats columnStats = TableExtDesc.ColumnStats.getColumnStats(nTableMetadataManager, colRef);
        return columnStats == null ? -1 : columnStats.getCardinality();
    }
}