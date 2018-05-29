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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;

class QueryAliasMatchInfo {
    // bimap between query table alias and model table alias
    private BiMap<String, String> aliasMapping;
    // each alias's ColumnRowType
    private LinkedHashMap<String, ColumnRowType> queryAlias;

    QueryAliasMatchInfo(BiMap<String, String> aliasMapping, LinkedHashMap<String, ColumnRowType> queryAlias) {
        this.aliasMapping = aliasMapping;
        this.queryAlias = queryAlias;
    }

    /**
     * only return null if it's a column on subquery
     */
    static TblColRef resolveTblColRef(SqlIdentifier sqlIdentifier, LinkedHashMap<String, ColumnRowType> queryAlias) {
        TblColRef ret = null;
        if (sqlIdentifier.names.size() == 2) {
            //alias.col
            String alias = sqlIdentifier.names.get(0);
            String col = sqlIdentifier.names.get(1);
            ColumnRowType columnRowType = queryAlias.get(alias);

            if (columnRowType == null) {
                throw new IllegalStateException("Alias " + alias + " is not defined");
            }

            if (columnRowType == QueryAliasMatcher.SUBQUERY_TAG) {
                return null;
            }

            ret = columnRowType.getColumnByName(col);
        } else if (sqlIdentifier.names.size() == 1) {
            //only col
            String col = sqlIdentifier.names.get(0);
            ret = resolveTblColRef(queryAlias, col);
        }

        if (ret == null) {
            throw new IllegalStateException(
                    "The join condition column " + sqlIdentifier.toString() + " cannot be resolved");
        }
        return ret;
    }

    static TblColRef resolveTblColRef(LinkedHashMap<String, ColumnRowType> queryAlias, String col) {
        List<String> potentialAlias = Lists.newArrayList();
        for (Map.Entry<String, ColumnRowType> entry : queryAlias.entrySet()) {
            if (entry.getValue() != QueryAliasMatcher.SUBQUERY_TAG && entry.getValue().getColumnByName(col) != null) {
                potentialAlias.add(entry.getKey());
            }
        }
        if (potentialAlias.size() == 1) {
            ColumnRowType columnRowType = queryAlias.get(potentialAlias.get(0));
            return columnRowType.getColumnByName(col);
        } else if (potentialAlias.size() > 1) {
            throw new IllegalStateException(
                    "The column " + col + " is found on multiple alias: " + StringUtils.join(potentialAlias, ","));
        } else {
            throw new IllegalStateException("The column " + col + " can't be found");
        }
    }

    BiMap<String, String> getAliasMapping() {
        return aliasMapping;
    }

    LinkedHashMap<String, ColumnRowType> getQueryAlias() {
        return queryAlias;
    }

}
