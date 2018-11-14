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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.KeywordDefaultDirtyHack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;

public class KapQueryUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KapQueryUtil.class);

    public static String massageComputedColumn(NDataModel model, String project, ComputedColumnDesc cc) {
        String tempConst = "'" + UUID.randomUUID().toString() + "'";
        StringBuilder forCC = new StringBuilder();
        forCC.append("select ");
        forCC.append(cc.getExpression());
        forCC.append(" ,").append(tempConst);
        forCC.append(" ");
        appendJoinStatement(model, forCC, false);

        String ccSql = KeywordDefaultDirtyHack.transform(forCC.toString());
        try {
            // massage nested CC for drafted model
            Map<String, NDataModel> modelMap = Maps.newHashMap();
            modelMap.put(model.getName(), model);
            ccSql = RestoreFromComputedColumn.convertWithGivenModels(ccSql, project, "DEFAULT",
                    modelMap);
        } catch (Exception e) {
            LOGGER.warn("Failed to massage SQL expression [{}] with input model {}", ccSql, model.getName(), e);
        }

        return ccSql.substring("select ".length(), ccSql.indexOf(tempConst) - 1);
    }

    public static void appendJoinStatement(NDataModel model, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = Sets.newHashSet();

        TableRef rootTable = model.getRootFactTable();
        sql.append("FROM " + model.getRootFactTable().getTableIdentity() + " as "
                + rootTable.getAlias() + " " + sep);

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
            sql.append(joinType + " JOIN " + dimTable.getTableIdentity() + " as " + dimTable.getAlias() + sep);
            sql.append("ON ");
            for (int i = 0; i < pk.length; i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(fk[i].getExpressionInSourceDB() + " = " + pk[i].getExpressionInSourceDB());
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
