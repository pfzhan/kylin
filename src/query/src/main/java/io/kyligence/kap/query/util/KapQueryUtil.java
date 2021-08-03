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

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.KeywordDefaultDirtyHack;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KapQueryUtil {

    public static final String DEFAULT_SCHEMA = "DEFAULT";

    private KapQueryUtil() {
    }

    public static String massageExpression(NDataModel model, String project, String expression,
            QueryContext.AclInfo aclInfo, boolean massageToPushdown) {
        String tempConst = "'" + UUID.randomUUID().toString() + "'";
        StringBuilder forCC = new StringBuilder();
        forCC.append("select ");
        forCC.append(expression);
        forCC.append(" ,").append(tempConst);
        forCC.append(" ");
        appendJoinStatement(model, forCC, false);

        String ccSql = KeywordDefaultDirtyHack.transform(forCC.toString());
        try {
            // massage nested CC for drafted model
            Map<String, NDataModel> modelMap = Maps.newHashMap();
            modelMap.put(model.getUuid(), model);
            ccSql = RestoreFromComputedColumn.convertWithGivenModels(ccSql, project, DEFAULT_SCHEMA, modelMap);
            QueryParams queryParams = new QueryParams(project, ccSql, DEFAULT_SCHEMA, false);
            queryParams.setKylinConfig(QueryUtil.getKylinConfig(project));
            queryParams.setAclInfo(aclInfo);

            if (massageToPushdown) {
                ccSql = QueryUtil.massagePushDownSql(queryParams);
            }
        } catch (Exception e) {
            log.warn("Failed to massage SQL expression [{}] with input model {}", ccSql, model.getUuid(), e);
        }

        return ccSql.substring("select ".length(), ccSql.indexOf(tempConst) - 2).trim();
    }

    public static String massageExpression(NDataModel model, String project, String expression,
            QueryContext.AclInfo aclInfo) {
        return massageExpression(model, project, expression, aclInfo, true);
    }

    public static String massageComputedColumn(NDataModel model, String project, ComputedColumnDesc cc,
            QueryContext.AclInfo aclInfo) {
        return massageExpression(model, project, cc.getExpression(), aclInfo);
    }

    public static void appendJoinStatement(NDataModel model, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = Sets.newHashSet();

        TableRef rootTable = model.getRootFactTable();
        sql.append(String.format(Locale.ROOT, "FROM \"%s\".\"%s\" as \"%s\"", rootTable.getTableDesc().getDatabase(),
                rootTable.getTableDesc().getName(), rootTable.getAlias()));
        sql.append(sep);

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            TableRef dimTable = lookupDesc.getTableRef();
            if (join == null || StringUtils.isEmpty(join.getType()) || dimTableCache.contains(dimTable)) {
                continue;
            }

            TblColRef[] pk = join.getPrimaryKeyColumns();
            TblColRef[] fk = join.getForeignKeyColumns();
            if (pk.length != fk.length) {
                throw new IllegalArgumentException("Invalid join condition of lookup table:" + lookupDesc);
            }
            String joinType = join.getType().toUpperCase(Locale.ROOT);

            sql.append(String.format(Locale.ROOT, "%s JOIN \"%s\".\"%s\" as \"%s\"", //
                    joinType, dimTable.getTableDesc().getDatabase(), dimTable.getTableDesc().getName(),
                    dimTable.getAlias()));
            sql.append(sep);
            sql.append("ON ");

            if (pk.length == 0 && join.getNonEquiJoinCondition() != null) {
                sql.append(join.getNonEquiJoinCondition().getExpr());
                dimTableCache.add(dimTable);
                continue;
            }

            for (int i = 0; i < pk.length; i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(String.format(Locale.ROOT, "%s = %s", fk[i].getExpressionInSourceDBWithDoubleQuote(),
                        pk[i].getExpressionInSourceDBWithDoubleQuote()));
            }
            sql.append(sep);

            dimTableCache.add(dimTable);
        }
    }

    public static SqlSelect extractSqlSelect(SqlCall selectOrOrderby) {
        SqlSelect sqlSelect = null;

        if (selectOrOrderby instanceof SqlSelect) {
            sqlSelect = (SqlSelect) selectOrOrderby;
        } else if (selectOrOrderby instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = ((SqlOrderBy) selectOrOrderby);
            if (sqlOrderBy.query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) sqlOrderBy.query;
            }
        }

        return sqlSelect;
    }

    public static boolean isJoinOnlyOneAggChild(KapJoinRel joinRel) {
        RelNode joinLeftChild;
        RelNode joinRightChild;
        final RelNode joinLeft = joinRel.getLeft();
        final RelNode joinRight = joinRel.getRight();
        if (joinLeft instanceof RelSubset && joinRight instanceof RelSubset) {
            final RelSubset joinLeftChildSub = (RelSubset) joinLeft;
            final RelSubset joinRightChildSub = (RelSubset) joinRight;
            joinLeftChild = Util.first(joinLeftChildSub.getBest(), joinLeftChildSub.getOriginal());
            joinRightChild = Util.first(joinRightChildSub.getBest(), joinRightChildSub.getOriginal());

        } else if (joinLeft instanceof HepRelVertex && joinRight instanceof HepRelVertex) {
            joinLeftChild = ((HepRelVertex) joinLeft).getCurrentRel();
            joinRightChild = ((HepRelVertex) joinRight).getCurrentRel();
        } else {
            return false;
        }

        if (!(joinLeftChild instanceof Aggregate) && !(joinRightChild instanceof Aggregate)) {
            return false;
        }
        if (joinLeftChild instanceof Aggregate && joinRightChild instanceof Aggregate) {
            return false;
        }
        return true;
    }

    public static boolean isCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        return SqlKind.CAST == rexNode.getKind();
    }

    public static boolean isPlainTableColumn(int colIdx, RelNode relNode) {
        if (relNode instanceof HepRelVertex) {
            relNode = ((HepRelVertex) relNode).getCurrentRel();
        }
        if (relNode instanceof TableScan) {
            return true;
        } else if (relNode instanceof Join) {
            Join join = (Join) relNode;
            int offset = 0;
            for (RelNode input : join.getInputs()) {
                if (colIdx >= offset && colIdx < input.getRowType().getFieldCount()) {
                    return isPlainTableColumn(colIdx - offset, input);
                }
                offset += input.getRowType().getFieldCount();
            }
        } else if (relNode instanceof Project) {
            RexNode inputRex = ((Project) relNode).getProjects().get(colIdx);
            if (inputRex instanceof RexInputRef) {
                return isPlainTableColumn(((RexInputRef) inputRex).getIndex(), ((Project) relNode).getInput());
            }
        } else if (relNode instanceof Filter) {
            return isPlainTableColumn(colIdx, relNode.getInput(0));
        }
        return false;
    }

    public static boolean containCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        if (SqlKind.CAST == rexNode.getKind()) {
            RexNode operand = ((RexCall) rexNode).getOperands().get(0);
            if (operand instanceof RexCall && operand.getKind() != SqlKind.CASE) {
                return false;
            }
            return true;
        }

        return false;
    }

    public static boolean isNotNullLiteral(RexNode node) {
        return !isNullLiteral(node);
    }

    public static boolean isNullLiteral(RexNode node) {
        return node instanceof RexLiteral && ((RexLiteral) node).isNull();
    }
}
