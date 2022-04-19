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

package io.kyligence.kap.query.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.TableColRefWithRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.CollectionUtil;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.query.relnode.KapAggregateRel;
import lombok.val;
import lombok.var;

public class ComputedColumnRewriter {

    private static final Logger logger = LoggerFactory.getLogger(ComputedColumnRewriter.class);

    private ComputedColumnRewriter() {
    }

    public static void rewriteCcInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        rewriteAggInnerCol(context, model, matchInfo);
        rewriteTopNInnerCol(context, model, matchInfo);
        rewriteGroupByInnerCol(context, model, matchInfo);
        rewriteFilterInnerCol(context, model, matchInfo);
    }

    private static void rewriteAggInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (!KapConfig.getInstanceFromEnv().isAggComputedColumnRewriteEnabled()
                || CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        context.aggregations.stream().filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters())).forEach(agg -> {
            List<ParameterDesc> parameters = Lists.newArrayList();
            for (ParameterDesc parameter : agg.getParameters()) {
                if (!parameter.getColRef().isInnerColumn()) {
                    parameters.add(parameter);
                    continue;
                }

                TblColRef translatedInnerCol = rewriteInnerColumnToTblColRef(parameter.getColRef().getParserDescription(), model, matchInfo);
                if (translatedInnerCol != null) {
                    parameters.add(ParameterDesc.newInstance(translatedInnerCol));
                    context.allColumns.add(translatedInnerCol);
                }
            }

            if (!parameters.isEmpty()) {
                agg.setParameters(parameters);
            }
        });
    }

    private static TblColRef rewriteInnerColumnToTblColRef(String innerExpression, NDataModel model, QueryAliasMatchInfo matchInfo) {
        SqlNode innerColExpr;
        try {
            innerColExpr = CalciteParser.getExpNode(innerExpression);
        } catch (IllegalStateException e) {
            logger.warn("Failed to resolve CC expr for {}", innerExpression, e);
            return null;
        }

        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            SqlNode ccExpressionNode = CalciteParser.getExpNode(cc.getExpression());
            if (ExpressionComparator.isNodeEqual(innerColExpr, ccExpressionNode, matchInfo,
                    new AliasDeduceImpl(matchInfo))) {
                var ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                        model.getRootFactTable().getTableDesc());
                logger.info("Replacing CC expr [{},{}]", cc.getColumnName(), cc.getExpression());
                return new TblColRef(model.getRootFactTable(), ccCols[0]);
            }
        }

        return null;
    }

    private static void rewriteTopNInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs()))
            return;

        context.getSortColumns().stream().filter(TblColRef::isInnerColumn).forEach(column -> {
            if (CollectionUtils.isEmpty(column.getOperands()))
                return;

            val translatedOperands = Lists.<TblColRef> newArrayList();
            for (TblColRef tblColRef : column.getOperands()) {
                val innerExpression = tblColRef.getParserDescription();
                if (innerExpression == null)
                    continue;

                val translatedInnerCol = rewriteInnerColumnToTblColRef(innerExpression, model, matchInfo);
                if (translatedInnerCol != null)
                    translatedOperands.add(translatedInnerCol);
            }

            column.setOperands(translatedOperands);
        });
    }

    private static void rewriteGroupByInnerCol(OLAPContext ctx, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        // collect all aggRel with candidate CC group keys
        Map<KapAggregateRel, Map<TblColRef, TblColRef>> relColReplacementMapping = new HashMap<>();
        for (TableColRefWithRel tableColRefWIthRel : ctx.getInnerGroupByColumns()) {
            SqlNode innerColExpr;
            try {
                innerColExpr = CalciteParser.getExpNode(tableColRefWIthRel.getTblColRef().getParserDescription());
            } catch (IllegalStateException e) {
                logger.warn("Failed to resolve CC expr for {}", tableColRefWIthRel.getTblColRef().getParserDescription(), e);
                continue;
            }
            for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
                SqlNode ccExpressionNode = CalciteParser.getExpNode(cc.getExpression());
                if (ExpressionComparator.isNodeEqual(innerColExpr, ccExpressionNode, matchInfo,
                        new AliasDeduceImpl(matchInfo))) {
                    var ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                            model.getRootFactTable().getTableDesc());

                    CollectionUtil.find(
                            ctx.firstTableScan.getColumnRowType().getAllColumns(),
                            colRef -> colRef.getColumnDesc().equals(ccCols[0])
                    ).ifPresent(ccColRef -> {
                                relColReplacementMapping.putIfAbsent(
                                        tableColRefWIthRel.getRelNodeAs(KapAggregateRel.class), new HashMap<>());
                                relColReplacementMapping.get(tableColRefWIthRel.getRelNodeAs(KapAggregateRel.class))
                                        .put(tableColRefWIthRel.getTblColRef(), ccColRef);
                                logger.info("Replacing CC expr [{},{}] in group key {}",
                                        cc.getColumnName(), cc.getExpression(), tableColRefWIthRel.getTblColRef());
                            });
                }
            }
        }

        // rebuild aggRel group keys with CC cols
        for (Map.Entry<KapAggregateRel, Map<TblColRef, TblColRef>> kapAggregateRelMapEntry : relColReplacementMapping.entrySet()) {
            KapAggregateRel aggRel = kapAggregateRelMapEntry.getKey();
            Map<TblColRef, TblColRef> colReplacementMapping = kapAggregateRelMapEntry.getValue();
            aggRel.reBuildGroups(colReplacementMapping);
        }
    }

    private static void rewriteFilterInnerCol(OLAPContext ctx, NDataModel model, QueryAliasMatchInfo matchInfo) {
        //empty
    }
}
