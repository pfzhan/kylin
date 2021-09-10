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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;

public class SqlDigestComputedColumnRewriter extends ComputedColumnRewriter {

    public static SQLDigest getSqlDigest(OLAPContext context, NDataModel model, QueryAliasMatchInfo matchInfo) {
        SQLDigest originSqlDigest = context.getSQLDigest();
        HashSet<TblColRef> newAllColumns = Sets.newHashSet(originSqlDigest.allColumns);
        List<TblColRef> newSortColumns = originSqlDigest.sortColumns.stream().map(TblColRef::copy)
                .collect(Collectors.toList());
        List<FunctionDesc> newAggregations = new ArrayList<>();
        Map<FunctionDesc, FunctionDesc> aggToOriginMap = Maps.newHashMap();
        for (FunctionDesc agg : originSqlDigest.aggregations) {
            FunctionDesc functionDesc = FunctionDesc.newInstance(agg.getExpression(), agg.getParameters(), agg.getReturnType());
            newAggregations.add(functionDesc);
            aggToOriginMap.put(functionDesc, agg);
        }

        SQLDigest sqlDigest = new SQLDigest(originSqlDigest.factTable, newAllColumns,
                Lists.newLinkedList(originSqlDigest.joinDescs), Lists.newArrayList(originSqlDigest.groupbyColumns),
                Sets.newHashSet(originSqlDigest.subqueryJoinParticipants),
                Sets.newHashSet(originSqlDigest.metricColumns), Lists.newArrayList(newAggregations),
                Sets.newLinkedHashSet(originSqlDigest.filterColumns), Lists.newArrayList(newSortColumns),
                Lists.newArrayList(originSqlDigest.sortOrders), originSqlDigest.limit,
                originSqlDigest.limitPrecedesAggr, Sets.newHashSet(originSqlDigest.involvedMeasure));
        sqlDigest.aggToOriginMap = aggToOriginMap;

        rewriteAggInnerCol(sqlDigest.aggregations, sqlDigest.allColumns, model, matchInfo);
        rewriteTopNInnerCol(sqlDigest.sortColumns, model, matchInfo);
        rewriteSqlDigestGroups(sqlDigest, context, model, matchInfo);
        return sqlDigest;
    }

    private static void rewriteSqlDigestGroups(SQLDigest sqlDigest, OLAPContext ctx, NDataModel model,
            QueryAliasMatchInfo matchInfo) {
        if (CollectionUtils.isEmpty(model.getComputedColumnDescs())) {
            return;
        }

        collectAggRelToCCGroupKeys(ctx, model, matchInfo).forEach((aggRel, colReplacementMapping) -> {
            ColumnRowType inputColumnRowType = ((OLAPRel) aggRel.getInput()).getColumnRowType();
            Set<TblColRef> groups = new HashSet<>();
            for (int i = aggRel.getGroupSet().nextSetBit(0); i >= 0; i = aggRel.getGroupSet().nextSetBit(i + 1)) {
                TblColRef originalColumn = inputColumnRowType.getColumnByIndex(i);
                Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(i);

                if (colReplacementMapping.containsKey(originalColumn)) {
                    groups.add(colReplacementMapping.get(originalColumn));
                } else {
                    groups.addAll(sourceColumns);
                }
            }
            sqlDigest.groupbyColumns = groups.stream()
                    .filter(col -> !col.isInnerColumn() && ctx.belongToContextTables(col)).collect(Collectors.toList());
        });
    }
}
