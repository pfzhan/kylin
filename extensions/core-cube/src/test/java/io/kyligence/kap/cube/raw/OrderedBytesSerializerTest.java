/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.metadata.datatype.OrderedBytesStringSerializer;

public class OrderedBytesSerializerTest {

    BigDecimal largeBigDecimal = normalize(new BigDecimal(Double.MAX_VALUE).multiply(new BigDecimal(2)));
    BigDecimal normalBigDecimal = normalize(new BigDecimal("123123.231123"));
    BigDecimal smallBigDecimal = normalize(new BigDecimal(Double.MIN_VALUE).multiply(new BigDecimal(0.5)));

    RawTableCodeSystem codeSystem;
    ImmutableBitSet allCols;

    String[] inputs1 = new String[] { null, //
            "FFFF", //
            largeBigDecimal.toPlainString(), //
            normalBigDecimal.toPlainString(), //
            smallBigDecimal.toPlainString(), //
            "0.1324324", //
            "0.0001", //
            String.valueOf(Long.MAX_VALUE), //
            String.valueOf(Integer.MAX_VALUE), //
            String.valueOf(Short.MAX_VALUE), //
            String.valueOf(Byte.MAX_VALUE), //
            String.valueOf(true), //
            "1970-01-01", //
            "1970-01-01 00:00:00", //
            "1970-01-01 00:00:00", //
    };
    Object[] expectedOutputs1 = new Object[] { null, //
            "FFFF", //
            largeBigDecimal, //
            normalBigDecimal, //
            smallBigDecimal, //
            0.1324324d, //
            0.0001f, //
            Long.MAX_VALUE, //
            Integer.MAX_VALUE, //
            Short.MAX_VALUE, //
            Byte.MAX_VALUE, //
            (byte) 1, //
            0L, //
            0L, //
            0L, //
    };

    String[] inputs2 = new String[] { null, //
            "FFFF", //
            largeBigDecimal.negate().toPlainString(), //
            normalBigDecimal.negate().toPlainString(), //
            smallBigDecimal.negate().toPlainString(), //
            "0.1324324", //
            "0.0001", //
            String.valueOf(Long.MAX_VALUE), //
            String.valueOf(Integer.MAX_VALUE), //
            String.valueOf(Short.MAX_VALUE), //
            String.valueOf(Byte.MAX_VALUE), //
            String.valueOf(true), //
            "1970-01-01", //
            "1970-01-01 00:00:00", //
            "1970-01-01 00:00:00", //
    };
    Object[] expectedOutputs2 = new Object[] { null, //
            "FFFF", //
            largeBigDecimal.negate(), //
            normalBigDecimal.negate(), //
            smallBigDecimal.negate(), //
            0.1324324d, //
            0.0001f, //
            Long.MAX_VALUE, //
            Integer.MAX_VALUE, //
            Short.MAX_VALUE, //
            Byte.MAX_VALUE, //
            (byte) 1, //
            0L, //
            0L, //
            0L, //
    };

    TableDesc extTable = null;
    List<TblColRef> columns = null;

    @Before
    public void prepare() {
        extTable = TableDesc.mockup("ext");
        columns = Lists.newArrayList();
        columns.add(ColumnDesc.mockup(extTable, 0, "PK", "char").getRef());
        columns.add(ColumnDesc.mockup(extTable, 1, "A", "char").getRef());
        columns.add(ColumnDesc.mockup(extTable, 2, "B1", "decimal").getRef());
        columns.add(ColumnDesc.mockup(extTable, 3, "B2", "decimal").getRef());
        columns.add(ColumnDesc.mockup(extTable, 4, "B3", "decimal").getRef());
        columns.add(ColumnDesc.mockup(extTable, 5, "C", "double").getRef());
        columns.add(ColumnDesc.mockup(extTable, 6, "D", "float").getRef());
        columns.add(ColumnDesc.mockup(extTable, 7, "E", "bigint").getRef());
        columns.add(ColumnDesc.mockup(extTable, 8, "F", "integer").getRef());
        columns.add(ColumnDesc.mockup(extTable, 9, "G", "smallint").getRef());
        columns.add(ColumnDesc.mockup(extTable, 10, "H", "tinyint").getRef());
        columns.add(ColumnDesc.mockup(extTable, 11, "I", "boolean").getRef());
        columns.add(ColumnDesc.mockup(extTable, 12, "J", "date").getRef());
        columns.add(ColumnDesc.mockup(extTable, 13, "K", "datetime").getRef());
        columns.add(ColumnDesc.mockup(extTable, 14, "L", "timestamp").getRef());

        GTInfo.Builder builder = GTInfo.builder();
        builder.setTableName("RawTable ");
        builder.setCodeSystem(new RawTableCodeSystem(new DimensionEncoding[columns.size()]));
        List<DataType> types = Lists.newArrayList();
        for (TblColRef col : columns) {
            types.add(col.getType());
        }
        builder.setColumns(types.toArray(new DataType[0]));
        builder.setPrimaryKey(new ImmutableBitSet(0));
        GTInfo gtInfo = builder.build();
        codeSystem = (RawTableCodeSystem) gtInfo.getCodeSystem();

        allCols = new ImmutableBitSet(1, columns.size());
    }

    @Test
    public void wholeTest1() {

        BufferedRawColumnCodec columnCodec = new BufferedRawColumnCodec(codeSystem);

        Object[] inputObjects = RawValueIngester.buildObjectOf(inputs1, columnCodec, allCols);
        ByteBuffer byteBuffer = columnCodec.encode(inputObjects, allCols);
        byteBuffer.flip();
        Object[] outputObjects = new Object[columns.size()];
        columnCodec.decode(byteBuffer, outputObjects, allCols);

        assertArrayEquals(expectedOutputs1, outputObjects);
    }

    @Test
    public void wholeTest2() {

        BufferedRawColumnCodec columnCodec = new BufferedRawColumnCodec(codeSystem);

        Object[] inputObjects = RawValueIngester.buildObjectOf(inputs2, columnCodec, allCols);
        ByteBuffer byteBuffer = columnCodec.encode(inputObjects, allCols);
        byteBuffer.flip();
        Object[] outputObjects = new Object[columns.size()];
        columnCodec.decode(byteBuffer, outputObjects, allCols);

        assertArrayEquals(expectedOutputs2, outputObjects);
    }

    @Test
    public void testEncodeNull() {
        OrderedBytesStringSerializer s = new OrderedBytesStringSerializer(DataType.getType("string"));
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        s.serialize(null, buffer);
        buffer.flip();
        String deserialize = s.deserialize(buffer);
        assertEquals(null, deserialize);
    }

    private static BigDecimal normalize(BigDecimal val) {
        return null == val ? null : val.stripTrailingZeros().round(OrderedBytes.DEFAULT_MATH_CONTEXT);
    }

}
