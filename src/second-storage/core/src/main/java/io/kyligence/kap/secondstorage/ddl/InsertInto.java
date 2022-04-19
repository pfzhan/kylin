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
import org.apache.commons.collections.map.ListOrderedMap;

import java.sql.SQLException;

public class InsertInto extends DDL<InsertInto> {

    private final TableIdentifier table;
    private Select select;
    private final ListOrderedMap columnsValues = new ListOrderedMap();

    public InsertInto(TableIdentifier table) {
        this.table = table;
        this.select = null;
    }

    public TableIdentifier table() {
        return table;
    }

    public InsertInto from(String database, String table) {
        select = new Select(TableIdentifier.table(database, table));
        return this;
    }

    public InsertInto set(final String column, final Object value) {
//        if (columnsValues.containsKey(column))
//            throw new QueryGrammarException("Column '" + column
//                    + "' has already been set.");
        columnsValues.put(column, value);
        return this;
    }

    public InsertInto set(final String column, final Object value,
                           final Object defaultValueIfNull) {
        if (value == null)
            return set(column, defaultValueIfNull);
        else
            return set(column, value);
    }

    public Select from() {
        return select;
    }

    public ListOrderedMap getColumnsValues() {
        return columnsValues;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    // throw SQLException for test
    public static InsertInto insertInto(String database, String table) throws SQLException {
        return new InsertInto(TableIdentifier.table(database, table));
    }
}
