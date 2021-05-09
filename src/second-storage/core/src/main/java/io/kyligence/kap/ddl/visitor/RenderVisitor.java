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
package io.kyligence.kap.ddl.visitor;


import io.kyligence.kap.ddl.AlterTable;
import io.kyligence.kap.ddl.CreateDatabase;
import io.kyligence.kap.ddl.CreateTable;
import io.kyligence.kap.ddl.DropTable;
import io.kyligence.kap.ddl.InsertInto;
import io.kyligence.kap.ddl.RenameTable;
import io.kyligence.kap.ddl.Select;
import io.kyligence.kap.ddl.exp.ColumnWithType;
import io.kyligence.kap.ddl.exp.TableIdentifier;

public interface RenderVisitor {
    void visit(ColumnWithType column);
    void visit(TableIdentifier tableIdentifier);
    void visit(RenameTable renameTable);
    void visit(CreateTable<?> createTable);
    void visit(CreateDatabase createDatabase);
    void visit(DropTable dropTable);
    void visit(InsertInto insert);
    void visit(Select insert);
    void visit(AlterTable alterTable);

    void visitValue(Object pram);

    void visit(AlterTable.ManipulatePartition movePartition);
}
