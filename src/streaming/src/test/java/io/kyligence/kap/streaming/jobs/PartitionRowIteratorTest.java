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
package io.kyligence.kap.streaming.jobs;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.gson.JsonParser;

import io.kyligence.kap.streaming.PartitionRowIterator;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import scala.collection.AbstractIterator;

public class PartitionRowIteratorTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testNextEmpty() {
        val schema = new StructType().add("value", StringType);

        val partitionRowIter = new PartitionRowIterator(new AbstractIterator<Row>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Row next() {
                return RowFactory.create("");
            }
        }, schema);
        Assert.assertTrue(partitionRowIter.hasNext());
        val row = partitionRowIter.next();
        Assert.assertEquals(0, row.length());
    }

    @Test
    public void testNextParseException() {
        val schema = new StructType().add("value", IntegerType);

        val partitionRowIter = new PartitionRowIterator(new AbstractIterator<Row>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Row next() {
                return RowFactory.create("{\"value\":\"ab\"}");
            }
        }, schema);
        Assert.assertTrue(partitionRowIter.hasNext());
        val row = partitionRowIter.next();
        Assert.assertEquals(0, row.length());
    }

    @Test
    public void testConvertJson2Row() {
        val parser = new JsonParser();
        val schemas = Arrays.asList(ShortType, IntegerType, LongType, DoubleType, FloatType, BooleanType, TimestampType,
                DateType, DecimalType.apply(5, 2));
        schemas.stream().forEach(dataType -> {
            val schema = new StructType().add("value", dataType);
            val partitionRowIter = new PartitionRowIterator(null, schema);

            val row = partitionRowIter.convertJson2Row("{}", parser);
            Assert.assertNull(row.get(0));
            val row1 = partitionRowIter.convertJson2Row("{value:\"\"}", parser);
            Assert.assertNull(row1.get(0));
            val row2 = partitionRowIter.convertJson2Row("{value2:\"\"}", parser);
            Assert.assertNull(row2.get(0));
        });

    }
}
