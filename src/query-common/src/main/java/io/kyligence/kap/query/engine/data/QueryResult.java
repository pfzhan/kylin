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

package io.kyligence.kap.query.engine.data;

import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.exec.ExecuteResult;

public class QueryResult {

    private Iterable<List<String>> rows;
    private List<List<String>> rowsList; // save the rows iteratored
    private int size;

    private List<StructField> columns;
    private List<SelectedColumnMeta> columnMetas;

    public QueryResult() {
        this(new LinkedList<>(), 0, new LinkedList<>());
    }

    public QueryResult(ExecuteResult result, List<StructField> columns) {
        this.rows = result.getRows();
        this.size = result.getSize();
        this.columns = columns;
    }

    public QueryResult(Iterable<List<String>> rows, int size, List<StructField> columns) {
        this.rows = rows;
        this.columns = columns;
        this.size = size;
    }

    public QueryResult(Iterable<List<String>> rows, int size, List<StructField> columns, List<SelectedColumnMeta> columnMetas) {
        this.rows = rows;
        this.size = size;
        this.columns = columns;
        this.columnMetas = columnMetas;
    }

    /**
     * This method is generally supposed to be used for testing only
     * @return
     * @deprecated
     */
    @Deprecated
    public List<List<String>> getRows() {
        if (rowsList == null) {
            rowsList = ImmutableList.copyOf(rows);
        }
        return rowsList;
    }

    public Iterable<List<String>> getRowsIterable() {
        return rows;
    }

    public int getSize() {
        return size;
    }

    public List<StructField> getColumns() {
        return columns;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        if (columnMetas != null) {
            return columnMetas;
        }
        columnMetas = new LinkedList<>();
        int columnCount = this.columns.size();
        List<StructField> fieldList = this.columns;

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).isNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true,
                    fieldList.get(i).getPrecision(), fieldList.get(i).getName(), fieldList.get(i).getName(), null, null,
                    null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                    fieldList.get(i).getDataTypeName(), false, false, false));
        }
        return columnMetas;
    }
}
