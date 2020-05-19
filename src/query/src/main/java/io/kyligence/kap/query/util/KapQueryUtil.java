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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.KeywordDefaultDirtyHack;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KapQueryUtil {

    private KapQueryUtil() {
    }

    public static String massageExpression(NDataModel model, String project, String expression) {
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
            ccSql = RestoreFromComputedColumn.convertWithGivenModels(ccSql, project, "DEFAULT", modelMap);
            KylinConfig config = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv().exportToProperties());
            config.setProperty("kylin.query.pushdown.converter-class-names", removeTableViewPrependerConverter(config.getPushDownConverterClassNames()));
            ccSql = QueryUtil.massagePushDownSql(config, ccSql, project, "DEFAULT", false);
        } catch (Exception e) {
            log.warn("Failed to massage SQL expression [{}] with input model {}", ccSql, model.getUuid(), e);
        }

        return ccSql.substring("select ".length(), ccSql.indexOf(tempConst) - 1).trim();
    }

    private static String removeTableViewPrependerConverter(String[] pushdownConverters) {
        StringBuilder sb = new StringBuilder();
        for (String converterName : pushdownConverters) {
            if (converterName.equalsIgnoreCase("io.kyligence.kap.query.security.TableViewPrepender"))
                continue;

            sb.append(converterName);
            sb.append(",");
        }

        if (sb.length() > 1) {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    public static String massageComputedColumn(NDataModel model, String project, ComputedColumnDesc cc) {
        return massageExpression(model, project, cc.getExpression());
    }

    public static void appendJoinStatement(NDataModel model, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = Sets.newHashSet();

        TableRef rootTable = model.getRootFactTable();
        sql.append(String.format("FROM \"%s\".\"%s\" as \"%s\"", rootTable.getTableDesc().getDatabase(),
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
            String joinType = join.getType().toUpperCase();

            sql.append(String.format("%s JOIN \"%s\".\"%s\" as \"%s\"", //
                    joinType, dimTable.getTableDesc().getDatabase(), dimTable.getTableDesc().getName(),
                    dimTable.getAlias()));
            sql.append(sep);
            sql.append("ON ");
            for (int i = 0; i < pk.length; i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(String.format("%s = %s", fk[i].getExpressionInSourceDB(), pk[i].getExpressionInSourceDB()));
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
}
