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

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.query.StructField;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;

public class QueryResult {

    private List<List<String>> rows;
    private List<StructField> columns;

    public QueryResult() {
        this(new LinkedList<>(), new LinkedList<>());
    }

    public QueryResult(List<List<String>> rows, List<StructField> columns) {
        this.rows = rows;
        this.columns = columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public List<StructField> getColumns() {
        return columns;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
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
