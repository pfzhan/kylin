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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;

import java.util.ArrayList;
import java.util.List;

public class Select extends DDL<Select> {
    private final TableIdentifier fromTable;

    private final List<ColumnWithAlias> columns = new ArrayList<>();
    private String condition;
    private GroupBy groupby;

    public Select(TableIdentifier table) {
        fromTable = table;
    }

    public Select column(ColumnWithAlias column) {
        columns.add(column);
        return this;
    }

    public Select where(String condition) {
        this.condition = condition;
        return this;
    }

    public Select groupby(GroupBy groupby) {
        this.groupby = groupby;
        return this;
    }

    public void columns(List<ColumnWithAlias> columns) {
        this.columns.addAll(columns);
    }

    public List<ColumnWithAlias> columns() {
        return columns;
    }

    public TableIdentifier from() {
        return fromTable;
    }

    public String where() {
        return condition;
    }

    public GroupBy groupby() {
        return groupby;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }
}