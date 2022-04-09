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

import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.visitor.DefaultSQLRender;

public class ClickHouseRender extends DefaultSQLRender {

    @Override
    public void visit(CreateTable<?> createTable) {
        ClickHouseCreateTable query = (ClickHouseCreateTable) createTable;

        if (query.createTableWithColumns()) {
            super.visit(query);
        } else {
            createTablePrefix(query);
            result.append(' ')
                    .append(DefaultSQLRender.KeyWord.AS);
            acceptOrVisitValue(query.likeTable());
        }
        if (query.engine() !=null) {
            result.append(' ')
                    .append(KeyWord.ENGINE)
                    .append(" = ")
                    .append(query.engine());
        }
        if (query.partitionBy() != null) {
            result.append(' ')
                    .append(KeyWord.PARTITION_BY)
                    .append(' ').append("`").append(query.partitionBy()).append("`");
        }
        if (query.createTableWithColumns()) {
            result.append(' ')
                    .append(DefaultSQLRender.KeyWord.ORDER_BY)
                    .append(' ')
                    .append(KeyWord.TUPLE)
                    .append('(')
                    .append(String.join(",", query.orderBy()))
                    .append(')');
        }
        if (query.getDeduplicationWindow() > 0) {
            result.append(' ')
                    .append(KeyWord.SETTINGS)
                    .append(' ')
                    .append(KeyWord.NON_REPLICATED_DEDUPLICATION_WINDOW)
                    .append(" = ")
                    .append(query.getDeduplicationWindow());
        }
    }

    @Override
    public void visit(AlterTable alterTable) {
        result.append(DefaultSQLRender.KeyWord.ALTER).append(' ')
                .append(DefaultSQLRender.KeyWord.TABLE);
        acceptOrVisitValue(alterTable.getTable());
        result.append(' ');
        if (alterTable.isFreeze()) {
            result.append(' ').append(KeyWord.FREEZE);
        } else if (alterTable.getManipulatePartition() != null) {
            acceptOrVisitValue(alterTable.getManipulatePartition());
        } else if (alterTable.getAttachPart() != null) {
            result.append(' ').append(KeyWord.ATTACH_PART).append(' ')
                    .append('\'').append(alterTable.getAttachPart()).append('\'');
        }
    }

    @Override
    public void visit(AlterTable.ManipulatePartition manipulatePartition) {
        result.append(manipulatePartition.getPartitionOperation().getOperation())
                .append(' ')
                .append(KeyWord.PARTITION)
                .append(' ')
                .append('\'').append(manipulatePartition.getPartition()).append("'");
        if (manipulatePartition.getDestTable() != null) {
            result.append(' ').append(DefaultSQLRender.KeyWord.TO)
                    .append(' ')
                    .append(DefaultSQLRender.KeyWord.TABLE);
            acceptOrVisitValue(manipulatePartition.getDestTable());
        }
    }

    private static class KeyWord {
        public static final String PARTITION_BY = "PARTITION BY";
        private static final String ENGINE = "ENGINE";
        private static final String TUPLE = "tuple";
        private static final String PARTITION = "PARTITION";
        private static final String FREEZE = "FREEZE";
        private static final String ATTACH_PART = "ATTACH PART";
        private static final String SETTINGS = "SETTINGS";
        private static final String NON_REPLICATED_DEDUPLICATION_WINDOW = "non_replicated_deduplication_window";

        private KeyWord() {
        }
    }
}
