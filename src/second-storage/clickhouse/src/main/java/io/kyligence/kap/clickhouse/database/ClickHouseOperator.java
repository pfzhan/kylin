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

package io.kyligence.kap.clickhouse.database;

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.secondstorage.database.DatabaseOperator;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import lombok.SneakyThrows;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseOperator implements DatabaseOperator {
    private ClickHouse clickHouse;
    private final ClickHouseRender render = new ClickHouseRender();

    public ClickHouseOperator(final String jdbcUrl) {
        try {
            this.clickHouse = new ClickHouse(jdbcUrl);
        } catch (SQLException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @SneakyThrows
    @Override
    public List<String> listDatabases() {
        ShowDatabases showDatabases = new ShowDatabases();
        return clickHouse.query(showDatabases.toSql(render), resultSet -> {
            try {
                return resultSet.getString(1);
            } catch (SQLException e) {
                return ExceptionUtils.rethrow(e);
            }
        });
    }

    @SneakyThrows
    @Override
    public List<String> listTables(final String database) {
        ShowTables showTables = ShowTables.createShowTables(database);
        return clickHouse.query(showTables.toSql(render), resultSet -> {
            try {
                return resultSet.getString(1);
            } catch (SQLException e) {
                return ExceptionUtils.rethrow(e);
            }
        });
    }

    @SneakyThrows
    @Override
    public void dropTable(final String database, final String table) {
        DropTable dropTable = DropTable.dropTable(database, table);
        clickHouse.apply(dropTable.toSql(render));
    }

    @Override
    public void close() throws IOException {
        this.clickHouse.close();
    }
}
