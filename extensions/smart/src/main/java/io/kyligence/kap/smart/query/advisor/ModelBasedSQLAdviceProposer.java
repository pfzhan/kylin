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
package io.kyligence.kap.smart.query.advisor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationCheck;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ModelBasedSQLAdviceProposer extends AbstractSQLAdviceProposer {
    private DataModelDesc dataModelDesc;

    public ModelBasedSQLAdviceProposer(DataModelDesc dataModelDesc) {
        this.dataModelDesc = dataModelDesc;
    }

    @Override
    public SQLAdvice propose(RealizationCheck.IncapableReason incapableReason, OLAPContext context) {
        switch (incapableReason.getIncapableType()) {
        case MODEL_UNMATCHED_JOIN:
            return getSqlJoinAdvisor(context.joinsTree, dataModelDesc.getJoinsTree());
        case MODEL_BAD_JOIN_SEQUENCE:
            return SQLAdvice.build(String.format(msg.getMODEL_BAD_JOIN_SEQUENCE_REASON(), dataModelDesc.getName()),
                    String.format(msg.getMODEL_BAD_JOIN_SEQUENCE_SUGGEST(), formatJoins(context.joins),
                            dataModelDesc.getName()));
        case CUBE_NOT_CONTAIN_ALL_DIMENSION:
            String notFoundDimensionMsg = formatTblColRefs(incapableReason.getNotFoundDimensions());
            return SQLAdvice.build(
                    String.format(msg.getMODEL_NOT_CONTAIN_ALL_DIMENSIONS_REASON(), notFoundDimensionMsg,
                            dataModelDesc.getName()),
                    String.format(msg.getMODEL_NOT_CONTAIN_ALL_DIMENSIONS_SUGGEST(), notFoundDimensionMsg,
                            dataModelDesc.getName()));
        case CUBE_NOT_CONTAIN_ALL_MEASURE:
            String notFoundMeasureMsg = formatFunctionDescs(incapableReason.getNotFoundMeasures());
            return SQLAdvice.build(
                    String.format(msg.getMODEL_NOT_CONTAIN_ALL_MEASURES_REASON(), notFoundMeasureMsg,
                            dataModelDesc.getName()),
                    String.format(msg.getMODEL_NOT_CONTAIN_ALL_MEASURES_SUGGEST(), notFoundMeasureMsg,
                            dataModelDesc.getName()));
        case CUBE_UNMATCHED_DIMENSION:
            String message = formatTblColRefs(incapableReason.getUnmatchedDimensions());
            return SQLAdvice.build(
                    String.format(msg.getMODEL_UNMATCHED_DIMENSIONS_REASON(), message, dataModelDesc.getName()),
                    String.format(msg.getMODEL_UNMATCHED_DIMENSIONS_SUGGEST(), message, dataModelDesc.getName()));
        case CUBE_NOT_CONTAIN_TABLE:
            return getTableNotFoundSqlAdvisor(incapableReason, context);
        case MODEL_FACT_TABLE_NOT_FOUND:
            String tableName = context.firstTableScan.getOlapTable().getTableName();
            if (!dataModelDesc.isLookupTable(tableName)) {
                return SQLAdvice.build(
                        String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_REASON(), tableName, dataModelDesc.getName()),
                        String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_SUGGEST(), tableName, dataModelDesc.getName()));
            } else {
                return null;
            }
        case MODEL_OTHER_MODEL_INCAPABLE:
            return SQLAdvice.build(msg.getMODEL_OTHER_MODEL_INCAPABLE_REASON(),
                    String.format(msg.getMODEL_OTHER_MODEL_INCAPABLE_SUGGEST(),
                            context.firstTableScan.getTableRef().getTableDesc().getIdentity()));
        default:
            return null;
        }
    }

    private SQLAdvice getFactUnmatchedSqlAdvisor(String factTbl) {
        return SQLAdvice.build(
                String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_REASON(), factTbl, dataModelDesc.getName()),
                String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_SUGGEST(), factTbl, dataModelDesc.getName()));
    }

    private SQLAdvice getTableNotFoundSqlAdvisor(RealizationCheck.IncapableReason incapableReason, OLAPContext ctx) {
        String message = formatTables(incapableReason.getNotFoundTables());
        return SQLAdvice.build(
                String.format(msg.getMODEL_NOT_CONTAIN_ALL_TABLES_REASON(), message, dataModelDesc.getName()),
                String.format(msg.getMODEL_NOT_CONTAIN_ALL_TABLES_SUGGEST(), message, dataModelDesc.getName()));
    }

    private SQLAdvice getSqlJoinAdvisor(JoinsTree contextJoinTree, JoinsTree modelJoinTree) {
        List<JoinsTree.Chain> unmatchedChains = contextJoinTree.unmatchedChain(modelJoinTree,
                Collections.<String, String> emptyMap());
        for (JoinsTree.Chain chain : unmatchedChains) {
            if (chain.getJoin() == null) {
                // root fact not matches
                return getFactUnmatchedSqlAdvisor(chain.getTable().getTableIdentity());
            } else {
                Map<JoinsTree.Chain, JoinsTree.Chain> unmatched = Maps.newHashMap();
                JoinUnmatchedType joinUnmatchedType = getJoinUnmatchedType(chain, modelJoinTree, unmatched);
                if (joinUnmatchedType != null) {
                    return getSqlJoinAdvice(joinUnmatchedType, unmatched);
                }
            }
        }
        return null;
    }

    private SQLAdvice getSqlJoinAdvice(JoinUnmatchedType joinUnmatchedType,
            Map<JoinsTree.Chain, JoinsTree.Chain> unmatched) {
        List<Map.Entry<JoinsTree.Chain, JoinsTree.Chain>> entries = Lists.newArrayList(unmatched.entrySet());
        String contextJoin = format(entries.get(0).getKey());
        String modelJoin = format(entries.get(0).getValue());
        switch (joinUnmatchedType) {
        case TABLE_NOT_FOUND:
            String tableName = entries.get(0).getKey().getTable().getTableName();
            return SQLAdvice.build(
                    String.format(msg.getMODEL_JOIN_TABLE_NOT_FOUND_REASON(), tableName, dataModelDesc.getName()),
                    String.format(msg.getMODEL_JOIN_TABLE_NOT_FOUND_SUGGEST(), tableName, dataModelDesc.getName()));
        case JOIN_TYPE_UNMATCHED:
            return SQLAdvice.build(
                    String.format(msg.getMODEL_JOIN_TYPE_UNMATCHED_REASON(), contextJoin, modelJoin,
                            dataModelDesc.getName()),
                    String.format(msg.getMODEL_JOIN_TYPE_UNMATCHED_SUGGEST(), dataModelDesc.getName()));
        case JOIN_CONDITION_UNMATCHED:
            return SQLAdvice.build(
                    String.format(msg.getMODEL_JOIN_CONDITION_UNMATCHED_REASON(), contextJoin, modelJoin,
                            dataModelDesc.getName()),
                    String.format(msg.getMODEL_JOIN_CONDITION_UNMATCHED_SUGGEST(), dataModelDesc.getName()));
        case JOIN_TYPE_AND_CONDITION_UNMATCHED:
            return SQLAdvice.build(
                    String.format(msg.getMODEL_JOIN_TYPE_CONDITION_UNMATCHED_REASON(), contextJoin, modelJoin,
                            dataModelDesc.getName()),
                    String.format(msg.getMODEL_JOIN_TYPE_CONDITION_UNMATCHED_SUGGEST(), dataModelDesc.getName()));
        default:
            return null;
        }
    }

    private String format(JoinsTree.Chain chain) {
        if (chain == null) {
            return "";
        }
        String tableName = chain.getTable().getTableName();
        String joinTable = chain.getFkSide().getTable().getTableName();
        List<String> joinConditions = Lists.newArrayList();
        String[] primaryKeys = chain.getJoin().getPrimaryKey();
        String[] foreignKeys = chain.getJoin().getForeignKey();
        for (int i = 0; i < primaryKeys.length; i++) {
            if (primaryKeys[i].startsWith(tableName)) {
                joinConditions.add(String.format("%s=%s", primaryKeys[i], foreignKeys[i]));
            } else {
                joinConditions.add(String.format("%s.%s=%s.%s", tableName, primaryKeys[i], joinTable, foreignKeys[i]));
            }
        }
        return String.format("%s JOIN: %s", chain.getJoin().getType().toUpperCase(),
                StringUtils.join(joinConditions, ", "));
    }

    private JoinUnmatchedType getJoinUnmatchedType(JoinsTree.Chain chain, JoinsTree joinsTree,
            Map<JoinsTree.Chain, JoinsTree.Chain> unmatched) {
        Map<String, JoinsTree.Chain> tableChains = joinsTree.getTableChains();
        for (JoinsTree.Chain otherChain : tableChains.values()) {
            if (otherChain.getJoin() != null
                    & chain.getTable().getTableName().equals(otherChain.getTable().getTableName())
                    && chain.getFkSide().getTable().getTableName()
                            .equals(otherChain.getFkSide().getTable().getTableName())) {
                return getJoinUnmatchedType(chain, otherChain, unmatched);
            }
        }
        unmatched.put(chain, null);
        return JoinUnmatchedType.TABLE_NOT_FOUND;
    }

    private JoinUnmatchedType getJoinUnmatchedType(JoinsTree.Chain chain, JoinsTree.Chain otherChain,
            Map<JoinsTree.Chain, JoinsTree.Chain> unmatched) {
        //join
        if (chain.getJoin() == null && otherChain.getJoin() == null) {
            return null;
        }

        if ((chain.getJoin() == null && otherChain.getJoin() != null)
                || (chain.getJoin() != null && otherChain.getJoin() == null)) {
            unmatched.put(chain, otherChain);
            return JoinUnmatchedType.JOIN_TYPE_AND_CONDITION_UNMATCHED;
        }

        boolean typeUnmatched = !chain.getJoin().getType().equalsIgnoreCase(otherChain.getJoin().getType());
        boolean conditionUnmatched = !columnDescEquals(chain.getJoin().getForeignKeyColumns(),
                otherChain.getJoin().getForeignKeyColumns())
                || !columnDescEquals(chain.getJoin().getPrimaryKeyColumns(),
                        otherChain.getJoin().getPrimaryKeyColumns());
        if (typeUnmatched && conditionUnmatched) {
            unmatched.put(chain, otherChain);
            return JoinUnmatchedType.JOIN_TYPE_AND_CONDITION_UNMATCHED;
        }
        if (typeUnmatched) {
            unmatched.put(chain, otherChain);
            return JoinUnmatchedType.JOIN_TYPE_UNMATCHED;
        }
        if (conditionUnmatched) {
            unmatched.put(chain, otherChain);
            return JoinUnmatchedType.JOIN_CONDITION_UNMATCHED;
        }

        if (chain.getFkSide() == null && otherChain.getFkSide() == null) {
            return null;
        }

        if ((chain.getFkSide() == null && otherChain.getFkSide() != null)
                || (chain.getFkSide() != null && otherChain.getFkSide() == null)) {
            unmatched.put(chain, otherChain);
            return JoinUnmatchedType.JOIN_TYPE_AND_CONDITION_UNMATCHED;
        }

        return getJoinUnmatchedType(chain.getFkSide(), otherChain.getFkSide(), unmatched);
    }

    private boolean columnDescEquals(TblColRef[] a, TblColRef[] b) {
        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (a[i].getColumnDesc().equals(b[i].getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    static enum JoinUnmatchedType {
        TABLE_NOT_FOUND, JOIN_TYPE_UNMATCHED, JOIN_CONDITION_UNMATCHED, JOIN_TYPE_AND_CONDITION_UNMATCHED
    }
}
