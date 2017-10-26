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

package io.kyligence.kap.metadata.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ComputedColumnDesc implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ComputedColumnDesc.class);

    @JsonProperty
    private String tableIdentity; // the alias in the model where the computed column belong to 
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String tableAlias;
    @JsonProperty
    private String columnName; // the new col name
    @JsonProperty
    private String expression;
    @JsonProperty
    private String innerExpression; // QueryUtil massaged expression
    @JsonProperty
    private String datatype;
    @JsonProperty
    private String comment;

    public void init(Map<String, TableRef> aliasMap, String rootFactTableName) {
        Set<String> aliasSet = aliasMap.keySet();

        Preconditions.checkNotNull(tableIdentity, "tableIdentity is null");
        Preconditions.checkNotNull(columnName, "columnName is null");
        Preconditions.checkNotNull(expression, "expression is null");
        Preconditions.checkNotNull(datatype, "datatype is null");

        if (tableAlias == null) // refer to comment of handleLegacyCC()
            tableAlias = tableIdentity.substring(tableIdentity.indexOf(".") + 1);

        Preconditions.checkState(tableIdentity.equals(tableIdentity.trim()),
                "tableIdentity of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(tableAlias.equals(tableAlias.trim()),
                "tableAlias of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(columnName.equals(columnName.trim()),
                "columnName of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(datatype.equals(datatype.trim()),
                "datatype of ComputedColumnDesc has heading/tailing whitespace");

        tableIdentity = tableIdentity.toUpperCase();
        tableAlias = tableAlias.toUpperCase();
        columnName = columnName.toUpperCase();

        if (!tableIdentity.contains(rootFactTableName) || !tableAlias.equals(rootFactTableName)) {
            throw new IllegalArgumentException("Computed column has to be defined on fact table");
        }

        for (TableRef tableRef : aliasMap.values()) {
            if (!rootFactTableName.equals(tableRef.getAlias())) {
                for (TblColRef tblColRef : tableRef.getColumns()) {
                    if (this.columnName.equals(tblColRef.getName())) {
                        throw new IllegalArgumentException(
                                "Computed column name " + columnName + " is already found on table "
                                        + tableRef.getTableIdentity() + ", use a different computed column name");
                    }
                }
            }
        }

        if ("true".equals(System.getProperty("needCheckCC"))) { //conditional execute this because of the calcite dependency is to available every where
            try {
                simpleParserCheck(expression, aliasSet);
            } catch (Exception e) {
                String legacyHandled = handleLegacyCC(expression, rootFactTableName, aliasSet);
                if (legacyHandled != null) {
                    expression = legacyHandled;
                } else {
                    throw e;
                }
            }
        }
    }

    private String handleLegacyCC(String expr, String rootFact, Set<String> aliasSet) {
        try {
            CalciteParser.ensureNoAliasInExpr(expr);
            String ret = CalciteParser.insertAliasInExpr(expr, rootFact);
            simpleParserCheck(ret, aliasSet);
            return ret;
        } catch (Exception e) {
            logger.error("failed to handle legacy CC " + expr);
            return null;
        }
    }

    public void simpleParserCheck(final String expr, final Set<String> aliasSet) {
        SqlNode sqlNode = CalciteParser.getExpNode(expr);

        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() != 2 || !aliasSet.contains(id.names.get(0))) {
                    throw new IllegalArgumentException("Column Identifier in the computed column " + expr
                            + "expression should comply to ALIAS.COLUMN ");
                }
                return null;
            }

            @Override
            public Object visit(SqlCall call) {
                if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                    throw new IllegalArgumentException(
                            "Computed column expression " + expr + " should not contain AS ");
                }
                return call.getOperator().acceptCall(this, call);
            }
        };

        sqlNode.accept(sqlVisitor);
    }

    public String getFullName() {
        return tableIdentity + "." + columnName;
    }

    public String getTableIdentity() {
        return tableIdentity;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getExpression() {
        return expression;
    }
    
    public void setInnerExpression(String innerExpression) {
        this.innerExpression = innerExpression;
    }
    
    public String getInnerExpression() {
        if (StringUtils.isEmpty(innerExpression)) {
            return expression;
        }
        return innerExpression;
    }

    public String getDatatype() {
        return datatype;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ComputedColumnDesc that = (ComputedColumnDesc) o;

        if (!tableIdentity.equals(that.tableIdentity))
            return false;
        if (!StringUtils.equals(tableAlias, that.tableAlias))
            return false;
        if (!columnName.equals(that.columnName))
            return false;
        if (!expression.equals(that.expression))
            return false;
        return datatype.equals(that.datatype);
    }

    @Override
    public int hashCode() {
        int result = tableIdentity.hashCode();
        if (tableAlias != null)
            result = 31 * result + tableAlias.hashCode();
        result = 31 * result + columnName.hashCode();
        result = 31 * result + expression.hashCode();
        result = 31 * result + datatype.hashCode();
        return result;
    }
}
