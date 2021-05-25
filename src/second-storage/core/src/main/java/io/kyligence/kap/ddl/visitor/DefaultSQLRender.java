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
import io.kyligence.kap.ddl.DropDatabase;
import io.kyligence.kap.ddl.DropTable;
import io.kyligence.kap.ddl.InsertInto;
import io.kyligence.kap.ddl.RenameTable;
import io.kyligence.kap.ddl.Select;
import io.kyligence.kap.ddl.ShowCreateTable;
import io.kyligence.kap.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.ddl.exp.ColumnWithType;
import io.kyligence.kap.ddl.exp.GroupBy;
import io.kyligence.kap.ddl.exp.TableIdentifier;
import org.apache.commons.collections.map.ListOrderedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.join;

public class DefaultSQLRender implements BaseRender {

    protected static final String OPEN_BRACKET = "(";
    protected static final String CLOSE_BRACKET = ")";

    @Override
    public void visit(Select select) {
        result.append(KeyWord.SELECT);
        if (select.columns().isEmpty()) {
            result.append(" * ");
        } else {
            result.append(' ');
            List<String> columns = new ArrayList<>(select.columns().size());
            for (ColumnWithAlias column : select.columns()) {
                columns.add(buildColumnWithAlias(column));
            }
            result.append(String.join(",", columns));
            result.append(' ');
        }

        result.append(KeyWord.FROM);
        acceptOrVisitValue(select.from());
        if (select.where() != null) {
            result.append(' ').append(KeyWord.WHERE).append(' ').append(select.where());
        }
        if (select.groupby() != null) {
            result.append(' ');
            this.visit(select.groupby());
        }
    }

    private String buildColumnWithAlias(ColumnWithAlias column) {
        StringBuilder columnString = new StringBuilder();
        if (column.isDistinct()) {
            columnString.append(KeyWord.DISTINCT);
        }
        if (column.getExpr() != null) {
            columnString.append(' ').append(column.getExpr());
        } else {
            columnString.append(" `").append(column.getName()).append("`");
        }
        if (column.getAlias() != null) {
            columnString.append(" AS ").append(column.getAlias());
        }
        return columnString.toString();
    }

    @Override
    public void visit(GroupBy groupBy) {
        result.append(KeyWord.GROUP_BY);
        result.append(' ');
        List<String> columns = groupBy.columns().stream().map(col -> "`" + col.getName() + "`").collect(Collectors.toList());
        result.append(String.join(",", columns));
    }

    protected final StringBuilder result = new StringBuilder();

    // dialect
    protected String quoteColumn(String col) {
        return "`" + col + "`";
    }

    private String nullableColumn(String type) {
        return KeyWord.NULLABLE + OPEN_BRACKET + type + CLOSE_BRACKET;
    }

    public StringBuilder getResult() {
        return result;
    }

    public void reset() {
        result.setLength(0);
    }

    @Override
    public void visit(ColumnWithType column) {
        if (column.quote()) {
            result.append(quoteColumn(column.name()));
        } else {
            result.append(column.name());
        }
        String type = column.type();
        if (column.nullable()) {
            type = nullableColumn(type);
        }
        result.append(' ').append(type);
    }

    @Override
    public void visit(TableIdentifier tableIdentifier) {
        result.append(' ').append(tableIdentifier.table());
    }

    @Override
    public void visit(RenameTable renameTable) {
        result.append(KeyWord.RENAME).append(' ').append(KeyWord.TABLE);
        acceptOrVisitValue(renameTable.source());
        result.append(' ').append(KeyWord.TO);
        acceptOrVisitValue(renameTable.to());
    }

    protected void createTablePrefix(CreateTable<?> query) {
        result.append(KeyWord.CREATE).append(' ').append(KeyWord.TABLE);
        if (query.isIfNotExists()) {
            result.append(' ').append(KeyWord.IF_NOT_EXISTS);
        }
        acceptOrVisitValue(query.table());
    }

    @Override
    public void visit(CreateTable<?> query) {
        createTablePrefix(query);

        result.append('(');
        boolean firstColumn = true;
        for (final ColumnWithType column : query.getColumns()) {
            if (firstColumn)
                firstColumn = false;
            else
                result.append(",");
            acceptOrVisitValue(column);
        }
        result.append(')');
    }

    @Override
    public void visit(CreateDatabase createDatabase) {
        result.append(KeyWord.CREATE).append(' ').append(KeyWord.DATABASE);
        if (createDatabase.isIfNotExists()) {
            result.append(' ').append(KeyWord.IF_NOT_EXISTS);
        }
        result.append(' ').append(createDatabase.getDatabase());
    }

    @Override
    public void visit(DropTable dropTable) {
        result.append(KeyWord.DROP).append(' ').append(KeyWord.TABLE);
        if (dropTable.isIfExists()) {
            result.append(' ').append(KeyWord.IF_EXISTS);
        }
        acceptOrVisitValue(dropTable.table());
    }

    @Override
    public void visit(DropDatabase dropDatabase) {
        result.append(KeyWord.DROP).append(' ').append(KeyWord.DATABASE);
        if (dropDatabase.isIfExists()) {
            result.append(' ').append(KeyWord.IF_EXISTS);
        }
        result.append(' ').append(dropDatabase.db());
    }

    @Override
    public void visit(InsertInto insert) {
        result.append(KeyWord.INSERT);
        acceptOrVisitValue(insert.table());
        result.append(' ');
        if (insert.from() != null) {
            acceptOrVisitValue(insert.from());
        } else {
            result.append(OPEN_BRACKET);
            final ListOrderedMap columnValues = insert.getColumnsValues();
            result.append(join(columnValues.keyList(), ", "));
            result.append(CLOSE_BRACKET).append(" ");
            result.append(KeyWord.VALUES).append(" ").append(OPEN_BRACKET);
            boolean firstClause = true;
            for (final Object column : columnValues.keyList()) {
                if (firstClause) {
                    firstClause = false;
                } else {
                    result.append(", ");
                }
                acceptOrVisitValue(columnValues.get(column));
            }
            result.append(CLOSE_BRACKET);
        }
    }

    @Override
    public void visit(ShowCreateTable showCreateTable) {
        result.append(KeyWord.SHOW_CREATE_TABLE).append(' ').append(showCreateTable.getTableIdentifier().table());
    }

    @Override
    public void visit(AlterTable alterTable) {
        throw new UnsupportedOperationException("Alter table doesn't support by default render");
    }

    protected static class KeyWord {
        private KeyWord() {}
        public static final String CREATE = "CREATE";
        public static final String DATABASE = "DATABASE";
        public static final String TABLE = "TABLE";
        public static final String DISTINCT = "DISTINCT";
        public static final String ALTER = "ALTER";
        public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE";
        public static final String WHERE = "WHERE";
        public static final String GROUP_BY = "GROUP BY";

        public static final String DROP = "DROP";
        public static final String IF_EXISTS = "if exists";
        public static final String IF_NOT_EXISTS = "if not exists";

        public static final String INSERT = "INSERT INTO";
        public static final String SELECT = "SELECT";
        public static final String FROM = "FROM";

        public static final String AS = "AS";
        public static final String ORDER_BY = "ORDER BY";

        public static final String RENAME = "RENAME";
        public static final String TO = "TO";

        private static final String VALUES = "VALUES";
        private static final String NULLABLE = "Nullable";
    }

    @Override
    public void visit(AlterTable.ManipulatePartition movePartition) {
        throw new UnsupportedOperationException("Move Partition doesn't support by default render");
    }

    @Override
    public void visitValue(Object pram) {
        if (pram instanceof String) {
            result.append("'").append(pram).append("'");
        } else {
            result.append(pram);
        }
    }
}
