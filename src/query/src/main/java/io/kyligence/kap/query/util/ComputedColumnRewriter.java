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

import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.var;

public class ComputedColumnRewriter {

    private ComputedColumnRewriter() {
    }

    public static void rewriteCcInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        rewriteAggInnerCol(context, model, matchInfo);
        rewriteGroupByInnerCol(context, model, matchInfo);
        rewriteFilterInnerCol(context, model, matchInfo);
    }

    private static void rewriteAggInnerCol(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (!KapConfig.getInstanceFromEnv().isAggComputedColumnRewriteEnabled()
                || CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        context.aggregations.stream().filter(agg -> agg.getParameters().size() != 0).forEach(agg -> {
            List<ParameterDesc> parameters = Lists.newArrayList();
            for (ParameterDesc parameter : agg.getParameters()) {
                if (!parameter.getColRef().isInnerColumn()) {
                    parameters.add(parameter);
                    continue;
                }

                for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
                    SqlNode ccExpressionNode = CalciteParser.getExpNode(cc.getExpression());
                    SqlNode innerColExpr = CalciteParser.getExpNode(parameter.getColRef().getParserDescription());
                    if (ExpressionComparator.isNodeEqual(innerColExpr, ccExpressionNode, matchInfo,
                            new AliasDeduceImpl(matchInfo))) {
                        var ccCols = ComputedColumnUtil.createComputedColumns(Lists.newArrayList(cc),
                                model.getRootFactTable().getTableDesc());
                        parameters.add(ParameterDesc.newInstance(new TblColRef(model.getRootFactTable(), ccCols[0])));
                    }
                }
            }

            if (!parameters.isEmpty()) {
                agg.setParameters(parameters);
            }
        });
    }

    private static void rewriteGroupByInnerCol(OLAPContext ctx, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }
        //todo
    }

    private static void rewriteFilterInnerCol(OLAPContext ctx, NDataModel model, QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }
        //todo
    }
}
