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

package io.kyligence.kap.secondstorage.ddl;

import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;
import io.kyligence.kap.secondstorage.ddl.visitor.Renderable;

public class AlterTable extends DDL<AlterTable> {
    private final TableIdentifier table;
    private ManipulatePartition manipulatePartition = null;
    private boolean freeze = false;
    private String attachPart = null;


    public AlterTable(TableIdentifier table, ManipulatePartition manipulatePartition) {
        this.table = table;
        this.manipulatePartition = manipulatePartition;
    }

    public AlterTable(TableIdentifier table, boolean freeze) {
        this.table = table;
        this.freeze = freeze;
    }

    public AlterTable(TableIdentifier table, String attachPart) {
        this.table = table;
        this.attachPart = attachPart;
    }

    public TableIdentifier getTable() {
        return table;
    }

    public ManipulatePartition getManipulatePartition() {
        return manipulatePartition;
    }

    public boolean isFreeze() {
        return freeze;
    }

    public String getAttachPart() {
        return attachPart;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    public enum PartitionOperation {
        MOVE {
            @Override
            public String getOperation() {
                return "MOVE";
            }
        },
        DROP {
            @Override
            public String getOperation() {
                return "DROP";
            }
        };

        public abstract String getOperation();
    }

    public static class ManipulatePartition implements Renderable {
        private final String partition;
        private final TableIdentifier destTable;
        private final PartitionOperation partitionOperation;

        public ManipulatePartition(String partition, TableIdentifier destTable, PartitionOperation partitionOperation) {
            this.partition = partition;
            this.destTable = destTable;
            this.partitionOperation = partitionOperation;
        }

        public ManipulatePartition(String partition, PartitionOperation partitionOperation) {
            this.partition = partition;
            this.destTable = null;
            this.partitionOperation = partitionOperation;
        }

        public String getPartition() {
            return partition;
        }

        public TableIdentifier getDestTable() {
            return destTable;
        }

        public PartitionOperation getPartitionOperation() {
            return partitionOperation;
        }

        @Override
        public void accept(RenderVisitor visitor) {
            visitor.visit(this);
        }
    }
}
