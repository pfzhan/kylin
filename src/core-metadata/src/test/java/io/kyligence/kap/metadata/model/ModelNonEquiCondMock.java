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

import java.util.HashMap;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;


public class ModelNonEquiCondMock {

    private HashMap<String, TableRef> tableRefCache = new HashMap<>();

    public void clearTableRefCache() {
        tableRefCache.clear();
    }

    public NonEquiJoinCondition mockTblColRefCond(String tableCol, SqlTypeName colType) {
        int idxTableEnd = tableCol.indexOf('.');
        TblColRef colRef = mockTblColRef(tableCol.substring(0, idxTableEnd), tableCol.substring(idxTableEnd+1));
        return mockTblColRefCond(colRef, colType);
    }

    public TableRef mockTblRef(String table) {
        if (!tableRefCache.containsKey(table)) {
            TableDesc tableDesc = new TableDesc();
            tableDesc.setName("DUMMY." + table);
            tableDesc.setColumns(new ColumnDesc[0]);
            tableRefCache.put(table, TblColRef.tableForUnknownModel(table, tableDesc));
        }
        return tableRefCache.get(table);
    }

    public TblColRef mockTblColRef(String table, String col) {
        return mockTblRef(table).makeFakeColumn(col);
    }

    public NonEquiJoinCondition colConstantCompareCond(SqlKind op, String col1, SqlTypeName col1Type, String constantValue, SqlTypeName constantType) {
        return new NonEquiJoinCondition(
                null,
                op,
                new NonEquiJoinCondition[]{mockTblColRefCond(col1, col1Type), mockConstantCond(constantValue, constantType)},
                mockDataType(SqlTypeName.BOOLEAN)
        );
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, SqlTypeName col1Type, String col2, SqlTypeName col2Type) {
        return new NonEquiJoinCondition(
                null,
                op,
                new NonEquiJoinCondition[]{mockTblColRefCond(col1, col1Type), mockTblColRefCond(col2, col2Type)},
                mockDataType(SqlTypeName.BOOLEAN)
        );
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, String col2, SqlTypeName typeName) {
        return colCompareCond(op, col1, typeName, col2, typeName);
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, String col2) {
        return colCompareCond(op, col1, col2, SqlTypeName.CHAR);
    }

    public static NonEquiJoinCondition composite(SqlKind op, NonEquiJoinCondition... conds) {
        return new NonEquiJoinCondition(null, op, conds, mockDataType(SqlTypeName.BOOLEAN));
    }

    public static NonEquiJoinCondition mockTblColRefCond(TblColRef tableCol, SqlTypeName colType) {
        return new NonEquiJoinCondition(tableCol, mockDataType(colType));
    }

    public static NonEquiJoinCondition mockConstantCond(String constantValue, SqlTypeName constantType) {
        return new NonEquiJoinCondition(constantValue, mockDataType(constantType));
    }

    public static DataType mockDataType(SqlTypeName sqlTypeName) {
        DataType dataType = new DataType();
        dataType.setTypeName(sqlTypeName);
        return dataType;
    }
}
