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

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class ModelViewSqlNodeComparator extends ExpressionComparator.SqlNodeComparator {

    private final NDataModel model;

    public ModelViewSqlNodeComparator(NDataModel model) {
        this.model = model;
    }

    @Override
    protected boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier) {
        if (querySqlIdentifier.isStar()) {
            return exprSqlIdentifier.isStar();
        } else if (exprSqlIdentifier.isStar()) {
            return false;
        }

        try {
            // 1. the query col table is not really matter here
            // as all columns are supposed to be selected from the single view table underneath
            // 2. and col renaming is not supported now in cc converting
            // so we use the col name in the query directly
            String queryCol = null;
            if (querySqlIdentifier.names.size() == 1) {
                queryCol = querySqlIdentifier.names.get(0);
            } else if (querySqlIdentifier.names.size() == 2) {
                queryCol = querySqlIdentifier.names.get(1);
            }

            NDataModel.NamedColumn modelCol = model.getColumnByColumnNameInModel(queryCol);
            String modelColTableAlias = modelCol.getAliasDotColumn().split("\\.")[0];
            String modelColName = modelCol.getAliasDotColumn().split("\\.")[1];

            return StringUtils.equalsIgnoreCase(modelColTableAlias, exprSqlIdentifier.names.get(0))
                    && StringUtils.equalsIgnoreCase(modelColName, exprSqlIdentifier.names.get(1));
        } catch (NullPointerException | IllegalStateException e) {
            log.trace("met exception when doing expressions[{}, {}] comparison", querySqlIdentifier,
                    exprSqlIdentifier, e);
            return false;
        }
    }
}
