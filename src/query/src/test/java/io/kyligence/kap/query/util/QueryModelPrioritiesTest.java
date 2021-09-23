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

package io.kyligence.kap.query.util;

import org.junit.Assert;
import org.junit.Test;

public class QueryModelPrioritiesTest {

    @Test
    public void testGetPriorities() {
        Assert.assertEquals("", getModelHints("select MODEL_PRIORITY(model1)"));
        Assert.assertEquals("", getModelHints("select MODEL_PRIORITY(model1)   */"));
        Assert.assertEquals("", getModelHints("select /*+ MODEL_PRIORITY()"));
        Assert.assertEquals("", getModelHints("select /*+ MODEL_PRIORITY111(model1)"));
        Assert.assertEquals("", getModelHints("select /* MODEL_PRIORITY(model1)"));
        Assert.assertEquals("MODEL1", getModelHints("select /*+ MODEL_PRIORITY(model1) */"));
        Assert.assertEquals("MODEL1,MODEL2", getModelHints("select /*+ MODEL_PRIORITY(model1, model2)   */"));
        Assert.assertEquals("MODEL1,MODEL2,MODEL3", getModelHints("select /*+ MODEL_PRIORITY(model1, model2,     model3)*/"));
        Assert.assertEquals("MODEL1", getModelHints("select   /*+   MODEL_PRIORITY(model1)  */ a from tbl"));
        Assert.assertEquals("MODEL1,MODEL2", getModelHints("select a from table inner join (select /*+ MODEL_PRIORITY(model1, model2) */b from table)"));
        Assert.assertEquals("MODEL3", getModelHints("select /*+MODEL_PRIORITY(model3)*/ LO_COMMITDATE,LO_CUSTKEY,count(LO_LINENUMBER) from  SSB.LINEORDER  where LO_COMMITDATE in(select /*+ MODEL_PRIORITY(model1)*/LO_COMMITDATE from SSB.LINEORDER) group by LO_CUSTKEY,LO_COMMITDATE"));
    }

    private String getModelHints(String sql) {
        return String.join(",", QueryModelPriorities.getModelPrioritiesFromComment(sql));
    }

    private void assertModelHints(String expected, String sql) {
        Assert.assertEquals(expected, getModelHints(sql));
    }

    @Test
    public void testCubePriorityWithComment() {
        //illegal CubePriority, cubepriority wont be recognized
        assertModelHints("", "-- cubepriority(aaa,bbb)\n"
                + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        //illegal CubePriority, no blank space between '('
        assertModelHints("", "-- CubePriority (aaa,bbb)\n"
                + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        // case insensitive
        assertModelHints("AAA,BBB", "-- CubePriority(aaa,bbb)\n"
                + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        //recog first matched
        String sql = "   --  CubePriority(kylin_1,kylin_2,kylin_1,kylin_3)   \n"
                + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n"
                + " -- CubePriority(kylin_4,kylin_5)   \n -- CubePriority(kylin_4,kylin_5)\n";
        assertModelHints("KYLIN_1,KYLIN_2,KYLIN_1,KYLIN_3", sql);
        // has both model_priority and CubePriority
        Assert.assertEquals("MODEL1", getModelHints("-- CubePriority(aaa,bbb) \nselect   /*+   MODEL_PRIORITY(model1)  */ a from tbl"));
    }

}
