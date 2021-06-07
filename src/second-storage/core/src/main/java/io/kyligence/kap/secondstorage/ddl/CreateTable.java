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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CreateTable<T extends CreateTable<T> > extends DDL<T> {

    private static class Default extends CreateTable<Default> {
        public Default(TableIdentifier table, boolean ifNotExists) {
            super(table, ifNotExists);
        }
    }

    private final boolean ifNotExists;
    private final TableIdentifier table;
    private final List<ColumnWithType> columns;


    public CreateTable(TableIdentifier table, boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
        this.table = table;
        this.columns = new ArrayList<>();
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public TableIdentifier table() {
        return table;
    }

    public List<ColumnWithType> getColumns() {
        return columns;
    }

    @SuppressWarnings("unchecked")
    public T columns(Collection<? extends ColumnWithType> fields) {
        columns.addAll(fields);
        return (T) this;
    }
    public final T columns(ColumnWithType... fields) {
        return columns(Arrays.asList(fields));
    }

    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    public static CreateTable<Default> create(String database, String table) {
        return new Default(TableIdentifier.table(database, table), true);
    }
}
