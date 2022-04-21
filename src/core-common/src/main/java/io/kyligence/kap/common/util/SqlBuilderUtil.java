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
package io.kyligence.kap.common.util;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

public class SqlBuilderUtil {
    private SqlBuilderUtil() {
    }

    public static class SparkTable extends SqlObject {
        String databaseName;
        String tableName;

        public SparkTable(String databaseName, String tableName) {
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        public SparkTable(String fullName) {
            String[] names = fullName.split("\\.");
            Preconditions.checkArgument(names.length == 1 || names.length == 2);
            if (names.length == 1) {
                this.databaseName = "";
                this.tableName = names[0];
            } else if (names.length == 2) {
                this.databaseName = names[0];
                this.tableName = names[1];
            }
        }

        @Override
        protected void collectSchemaObjects(ValidationContext vContext) {
            //don't need validate
        }

        @Override
        public void appendTo(AppendableExt a) throws IOException {
            String s;
            if (!databaseName.isEmpty()) {
                s = "`" + databaseName + "`" + "." + "`" + tableName + "`";
            } else {
                s = "`" + tableName + "`";
            }
            a.append(s);
        }
    }

    public static class SparkColumn extends SqlObject {
        String tableName;
        String columnName;

        public SparkColumn(String fullName) {
            String[] names = fullName.split("\\.");
            Preconditions.checkArgument(names.length == 1 || names.length == 2);
            if (names.length == 1) {
                this.tableName = "";
                this.columnName = names[0];
            } else if (names.length == 2) {
                this.tableName = names[0];
                this.columnName = names[1];
            }
        }

        public SparkColumn(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        protected void collectSchemaObjects(ValidationContext vContext) {
            //don't need validate
        }

        @Override
        public void appendTo(AppendableExt a) throws IOException {
            String s;
            if (!tableName.isEmpty()) {
                s = "`" + tableName + "`" + "." + "`" + columnName + "`";
            } else {
                s = "`" + columnName + "`";
            }
            a.append(s);
        }
    }

}
