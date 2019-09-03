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

package io.kyligence.kap.metadata.model.tool;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import io.kyligence.kap.metadata.model.ModelNonEquiCondMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class NonEquiJoinConditionComparatorTest {

    ModelNonEquiCondMock nonEquiMock = new ModelNonEquiCondMock();

    @After
    public void teardown() {
        nonEquiMock.clearTableRefCache();
    }

    @Test
    public void testSimpleEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1");
        Assert.assertEquals(cond1, cond1);
    }

    @Test
    public void testNestedCondEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.AND, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        Assert.assertEquals(cond1, cond1);
    }

    @Test
    public void testOrderingIrrelevantCondEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"),
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"));
        Assert.assertEquals(cond1, cond2);
    }

    @Test
    public void testComparingWithNull() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"));
        Assert.assertNotEquals(cond1, null);
    }

    @Test
    public void testNotEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.AND, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.OR, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnTable() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b1");
        NonEquiJoinCondition cond2 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "C.b1");
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnColumn() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b1");
        NonEquiJoinCondition cond2 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b2");
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnConstant() {
        NonEquiJoinCondition cond1 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.CHAR);
        NonEquiJoinCondition cond2 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT1", SqlTypeName.CHAR);
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnType() {
        NonEquiJoinCondition cond1 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.CHAR);
        NonEquiJoinCondition cond2 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.INTEGER);
        Assert.assertNotEquals(cond1, cond2);
    }

}
