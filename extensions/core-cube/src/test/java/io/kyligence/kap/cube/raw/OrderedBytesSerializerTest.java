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

package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.datatype.DataType;
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
        columns.add(TblColRef.mockup(extTable, 0, "PK", "char"));
        columns.add(TblColRef.mockup(extTable, 1, "A", "char"));
        columns.add(TblColRef.mockup(extTable, 2, "B1", "decimal"));
        columns.add(TblColRef.mockup(extTable, 3, "B2", "decimal"));
        columns.add(TblColRef.mockup(extTable, 4, "B3", "decimal"));
        columns.add(TblColRef.mockup(extTable, 5, "C", "double"));
        columns.add(TblColRef.mockup(extTable, 6, "D", "float"));
        columns.add(TblColRef.mockup(extTable, 7, "E", "bigint"));
        columns.add(TblColRef.mockup(extTable, 8, "F", "integer"));
        columns.add(TblColRef.mockup(extTable, 9, "G", "smallint"));
        columns.add(TblColRef.mockup(extTable, 10, "H", "tinyint"));
        columns.add(TblColRef.mockup(extTable, 11, "I", "boolean"));
        columns.add(TblColRef.mockup(extTable, 12, "J", "date"));
        columns.add(TblColRef.mockup(extTable, 13, "K", "datetime"));
        columns.add(TblColRef.mockup(extTable, 14, "L", "timestamp"));

        GTInfo.Builder builder = GTInfo.builder();
        builder.setTableName("RawTable ");

        List<Pair<String, Integer>> encodings = Lists.newArrayList();
        for (int i = 0; i < columns.size(); i++) {
            encodings.add(Pair.newPair(RawTableColumnDesc.RAWTABLE_ENCODING_ORDEREDBYTES, 1));
        }
        builder.setCodeSystem(new RawTableCodeSystem(encodings));
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
