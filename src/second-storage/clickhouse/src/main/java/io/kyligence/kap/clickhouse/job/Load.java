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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.ddl.exp.ColumnWithType;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.datatype.DataType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

public interface Load {
    static String clickHouseType(DataType dt) {
        switch (dt.getName()) {
            case DataType.BOOLEAN:
                return "UInt8";
            case DataType.BYTE:
            case DataType.TINY_INT:
                return "Int8";
            case DataType.SHORT:
            case DataType.SMALL_INT:
                return "Int16";
            case DataType.INT:
            case DataType.INT4:
            case DataType.INTEGER:
                return "Int32";
            case DataType.LONG:
            case DataType.LONG8:
            case DataType.BIGINT:
                return "Int64";
            case DataType.FLOAT:
                return "Float32";
            case DataType.DOUBLE:
                return "Float64";
            case DataType.DECIMAL:
            case DataType.NUMERIC:
                return String.format(Locale.ROOT, "Decimal(%d,%d)", dt.getPrecision(), dt.getScale());
            case DataType.VARCHAR:
            case DataType.CHAR:
            case DataType.STRING:
                return "String";
            case DataType.DATE:
                return "Date";
            case DataType.TIMESTAMP:
            case DataType.DATETIME:
                return "DateTime";
            case DataType.TIME:
            case DataType.REAL:
            case DataType.ANY_STR:
            case DataType.BINARY:
            case DataType.ARRAY:
                throw new UnsupportedOperationException("will support");
            default:
        }
        throw new UnsupportedOperationException("");
    }

    static List<ColumnWithType> columns(LayoutEntity layout, String partitionCol, boolean addPrefix) {
        List<ColumnWithType> cols = new ArrayList<>();
        layout.getOrderedDimensions().forEach((k, v) ->
                cols.add(new ColumnWithType(addPrefix ? getPrefixColumn(String.valueOf(k)) : String.valueOf(k),
                        clickHouseType(v.getType()),
                        // partition column must not be null
                        v.getColumnDesc().isNullable() && !String.valueOf(k).equals(partitionCol), true))
        );
        return cols;
    }

    static String getPrefixColumn(String col) {
        return "c" + col;
    }

    void load(List<LoadInfo> loadInfoBatch) throws InterruptedException, ExecutionException, SQLException;


}
