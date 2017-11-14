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

package io.kyligence.kap.common.persistence;

import java.util.Properties;

public class JDBCSqlQueryFormat {
    private Properties sqlQueries;

    public JDBCSqlQueryFormat(Properties props) {
        this.sqlQueries = props;
    }

    private String getSqlFromProperties(String key) {
        String sql = sqlQueries.getProperty(key);
        if (sql == null)
            throw new RuntimeException(String.format("Property '%s' not found", key));
        return sql;
    }

    public String getCreateIfNeedSql() {
        return getSqlFromProperties("format.sql.create-if-need");
    }

    public String getKeyEqualsSql() {
        return getSqlFromProperties("format.sql.key-equals");
    }

    public String getDeletePstatSql() {
        return getSqlFromProperties("format.sql.delete-pstat");
    }

    public String getListResourceSql() {
        return getSqlFromProperties("format.sql.list-resource");
    }

    public String getAllResourceSql() {
        return getSqlFromProperties("format.sql.all-resource");
    }

    public String getReplaceSql() {
        return getSqlFromProperties("format.sql.replace");
    }

    public String getInsertSql() {
        return getSqlFromProperties("format.sql.insert");
    }

    public String getReplaceSqlWithoutContent() {
        return getSqlFromProperties("format.sql.replace-without-content");
    }

    public String getInsertSqlWithoutContent() {
        return getSqlFromProperties("format.sql.insert-without-content");
    }

    public String getUpdateSqlWithoutContent() {
        return getSqlFromProperties("format.sql.update-without-content");
    }

    public String getUpdateContentSql() {
        return getSqlFromProperties("format.sql.update-content");
    }
}
