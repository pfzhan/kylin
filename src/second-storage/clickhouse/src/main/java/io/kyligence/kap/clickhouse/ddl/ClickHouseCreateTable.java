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
package io.kyligence.kap.clickhouse.ddl;

import io.kyligence.kap.secondstorage.ddl.CreateTable;

import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ClickHouseCreateTable extends CreateTable<ClickHouseCreateTable> {

    // clickhouse special
    private String engine;
    private TableIdentifier likeTable;
    private String partitionBy;
    private final List<String> orderBy;
    private int deduplicationWindow = 0;

    public ClickHouseCreateTable(TableIdentifier table, boolean ifNotExists) {
        super(table, ifNotExists);
        this.likeTable = null;
        this.engine = null;
        this.orderBy = new ArrayList<>();
    }

    public ClickHouseCreateTable engine(String engine) {
        this.engine = engine;
        return this;
    }
    public String engine() {
        return engine;
    }

    public ClickHouseCreateTable deduplicationWindow(int window) {
        this.deduplicationWindow = window;
        return this;
    }

    public int getDeduplicationWindow() {
        return deduplicationWindow;
    }

    public ClickHouseCreateTable partitionBy(String column) {
        this.partitionBy = column;
        return this;
    }

    public String partitionBy() {
        return this.partitionBy;
    }

    public ClickHouseCreateTable likeTable(String database, String table) {
        this.likeTable = TableIdentifier.table(database, table);
        return this;
    }
    public TableIdentifier likeTable() {
        return likeTable;
    }
    public boolean createTableWithColumns() {
        return likeTable == null;
    }

    public final ClickHouseCreateTable orderBy(String... fields) {
        orderBy.addAll(Arrays.asList(fields));
        return this;
    }

    public final List<String> orderBy() {
        return orderBy;
    }


    public static ClickHouseCreateTable createCKTable(String database, String table) {
        return new ClickHouseCreateTable(TableIdentifier.table(database, table), false);
    }

    public static ClickHouseCreateTable createCKTableIgnoreExist(String database, String table) {
        return new ClickHouseCreateTable(TableIdentifier.table(database, table), true);
    }
}
