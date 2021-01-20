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

package io.kyligence.kap.metadata.acl;

import static io.kyligence.kap.metadata.acl.ColumnToConds.concatConds;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.CLOSED;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.LEFT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.LIKE;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.OPEN;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.RIGHT_INCLUSIVE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.DateFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;

@RunWith(TimeZoneTestRunner.class)
public class ColumnToCondsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testTrimConds() {
        Assert.assertEquals("'a'", ColumnToConds.Cond.trim("a", "varchar(256)"));
        Assert.assertEquals("'a''b'", ColumnToConds.Cond.trim("a'b", "string"));
        Assert.assertEquals("DATE '2017-08-30'",
                ColumnToConds.Cond.trim(DateFormat.stringToMillis("2017-08-30 20:17:40") + "", "date"));
        Assert.assertEquals("TIME '20:17:40'",
                ColumnToConds.Cond.trim(DateFormat.stringToMillis("1979-01-01 20:17:40") + "", "time"));
        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'",
                ColumnToConds.Cond.trim(DateFormat.stringToMillis("2017-09-13 04:12:12") + "", "datetime"));

        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'",
                ColumnToConds.Cond.trim(DateFormat.stringToMillis("2017-09-13 04:12:12") + "", "timestamp"));
        Assert.assertEquals("7", ColumnToConds.Cond.trim("7", "int"));
    }

    @Test
    public void testConcatConds() {
        Map<String, List<ColumnToConds.Cond>> condsWithCol = new HashMap<>();
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "timestamp");
        columnWithType.put("COL3", "int");
        List<ColumnToConds.Cond> cond1 = Lists.newArrayList(
                new ColumnToConds.Cond("a", ColumnToConds.Cond.IntervalType.CLOSED),
                new ColumnToConds.Cond("b", ColumnToConds.Cond.IntervalType.CLOSED),
                new ColumnToConds.Cond("a'b", ColumnToConds.Cond.IntervalType.CLOSED));
        List<ColumnToConds.Cond> cond6 = Lists
                .newArrayList(new ColumnToConds.Cond(LEFT_INCLUSIVE, "2017-09-13 04:12:12", "2017-09-25 06:32:35")); //timestamp
        List<ColumnToConds.Cond> cond7 = Lists.newArrayList(new ColumnToConds.Cond(RIGHT_INCLUSIVE, "7", "100")); //normal type
        condsWithCol.put("COL1", cond1);
        condsWithCol.put("COL2", cond6);
        condsWithCol.put("COL3", cond7);
        ColumnToConds columnToConds = new ColumnToConds(condsWithCol);

        Map<String, List<ColumnToConds.Cond>> likeCondsWithCol = new HashMap<>();
        List<ColumnToConds.Cond> condLike = Lists.newArrayList(
                new ColumnToConds.Cond("like abc%", LIKE),
                new ColumnToConds.Cond("like cba%", LIKE));
        likeCondsWithCol.put("COL1", condLike);
        ColumnToConds columnToLikeConds = new ColumnToConds(likeCondsWithCol);

        Assert.assertEquals(
                "(((COL3>7 AND COL3<=100)) AND ((COL2>=TIMESTAMP '2017-09-13 04:12:12' AND "
                        + "COL2<TIMESTAMP '2017-09-25 06:32:35')) AND (COL1 in ('a','b','a''b') or "
                        + "COL1 like 'like abc%' or COL1 like 'like cba%'))",
                concatConds(columnToConds, columnToLikeConds, columnWithType));
    }

    @Test
    public void testRowACLToString() {
        ColumnToConds.Cond cond1 = new ColumnToConds.Cond(OPEN, null, "100");
        ColumnToConds.Cond cond2 = new ColumnToConds.Cond(RIGHT_INCLUSIVE, null, "100");
        ColumnToConds.Cond cond3 = new ColumnToConds.Cond(OPEN, "100", null);
        ColumnToConds.Cond cond4 = new ColumnToConds.Cond(LEFT_INCLUSIVE, "100", null);
        ColumnToConds.Cond cond5 = new ColumnToConds.Cond(OPEN, null, null);
        ColumnToConds.Cond cond6 = new ColumnToConds.Cond(CLOSED, null, null);
        Assert.assertEquals("(c1<100)", cond1.toString("c1", "int"));
        Assert.assertEquals("(c1<=100)", cond2.toString("c1", "int"));
        Assert.assertEquals("(c1>100)", cond3.toString("c1", "int"));
        Assert.assertEquals("(c1>=100)", cond4.toString("c1", "int"));
        Assert.assertEquals("(c1<>null)", cond5.toString("c1", "int"));
        Assert.assertEquals("(c1=null)", cond6.toString("c1", "int"));
        Assert.assertNotEquals(cond1, cond2);
        Assert.assertNotEquals(cond1, cond3);
        Assert.assertNotEquals(cond1, cond4);
        Assert.assertNotEquals(cond1, cond5);
        Assert.assertNotEquals(cond1, cond6);

        ColumnToConds.Cond copyCond1 = new ColumnToConds.Cond(OPEN, null, "100");
        Assert.assertEquals(copyCond1, cond1);
        Assert.assertEquals(copyCond1.hashCode(), cond1.hashCode());
    }

    @Test
    public void testGetColumnWithType() {
        Map<String, String> colType = ColumnToConds.getColumnWithType("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals("bigint", colType.get("ORDER_ID"));
    }
}