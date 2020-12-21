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
package io.kyligence.kap.source.jdbc;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.IReadableTable;

public class JdbcTable implements IReadableTable {
    private final JdbcConnector dataSource;
    private final String database;
    private final String tableName;
    private final TableDesc tableDesc;

    public JdbcTable(JdbcConnector dataSource, TableDesc tableDesc) {
        this.dataSource = dataSource;
        this.tableDesc = tableDesc;
        this.database = tableDesc.getDatabase();
        this.tableName = tableDesc.getName();
    }

    @Override
    public TableReader getReader() throws IOException {
        return new JdbcTableReader(dataSource, tableDesc);
    }

    @Override
    public TableSignature getSignature() {
        String path = String.format(Locale.ROOT, "%s.%s", database, tableName);
        return new TableSignature(path, 0, System.currentTimeMillis());
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public String toString() {
        return "database=[" + database + "], table=[" + tableName + "]";
    }
}
