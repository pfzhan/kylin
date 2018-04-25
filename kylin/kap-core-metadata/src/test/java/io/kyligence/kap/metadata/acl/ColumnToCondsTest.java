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
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.OPEN;
import static io.kyligence.kap.metadata.acl.ColumnToConds.Cond.IntervalType.RIGHT_INCLUSIVE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;


public class ColumnToCondsTest {
    @Test
    public void testTrimConds() {
        Assert.assertEquals("'a'", ColumnToConds.Cond.trim("a", "varchar(256)"));
        Assert.assertEquals("'a''b'", ColumnToConds.Cond.trim("a'b", "string"));
        Assert.assertEquals("DATE '2017-08-30'", ColumnToConds.Cond.trim("1504051200000", "date"));
        Assert.assertEquals("TIME '20:17:40'", ColumnToConds.Cond.trim("73060000", "time"));
        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'", ColumnToConds.Cond.trim("1505275932000", "datetime"));
        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'", ColumnToConds.Cond.trim("1505275932000", "timestamp"));
        Assert.assertEquals("7", ColumnToConds.Cond.trim("7", "int"));
    }

    @Test
    public void testConcatConds() {
        Map<String, List<ColumnToConds.Cond>> condsWithCol = new HashMap<>();
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "timestamp");
        columnWithType.put("COL3", "int");
        List<ColumnToConds.Cond> cond1 = Lists.newArrayList(new ColumnToConds.Cond("a"), new ColumnToConds.Cond("b"), new ColumnToConds.Cond("a'b"));
        List<ColumnToConds.Cond> cond6 = Lists.newArrayList(new ColumnToConds.Cond(LEFT_INCLUSIVE, "1505275932000", "1506321155000")); //timestamp
        List<ColumnToConds.Cond> cond7 = Lists.newArrayList(new ColumnToConds.Cond(RIGHT_INCLUSIVE, "7", "100")); //normal type
        condsWithCol.put("COL1", cond1);
        condsWithCol.put("COL2", cond6);
        condsWithCol.put("COL3", cond7);
        ColumnToConds columnToConds = new ColumnToConds(condsWithCol);
        Assert.assertEquals(
                "((COL1='a') OR (COL1='b') OR (COL1='a''b')) AND (COL2>=TIMESTAMP '2017-09-13 04:12:12' AND COL2<TIMESTAMP '2017-09-25 06:32:35') AND (COL3>7 AND COL3<=100)",
                concatConds(columnToConds, columnWithType));
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
    }
}