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
package io.kyligence.kap.secondstorage.ddl.visitor;


import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.DropDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.ExistsDatabase;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.RenameTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.ShowCreateDatabase;
import io.kyligence.kap.secondstorage.ddl.ShowCreateTable;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;

public interface RenderVisitor {
    void visit(ColumnWithType column);
    void visit(TableIdentifier tableIdentifier);
    void visit(RenameTable renameTable);
    void visit(CreateTable<?> createTable);
    void visit(CreateDatabase createDatabase);
    void visit(ShowCreateDatabase showCreateDatabase);
    void visit(DropTable dropTable);
    void visit(DropDatabase dropDatabase);
    void visit(InsertInto insert);
    void visit(Select insert);
    void visit(GroupBy groupBy);
    void visit(AlterTable alterTable);
    void visit(ShowCreateTable showCreateTable);
    void visit(ExistsDatabase existsDatabase);
    void visit(ExistsTable existsTable);
    void visit(ShowDatabases showDatabases);
    void visit(ShowTables showTables);

    void visitValue(Object pram);

    void visit(AlterTable.ManipulatePartition movePartition);
}
