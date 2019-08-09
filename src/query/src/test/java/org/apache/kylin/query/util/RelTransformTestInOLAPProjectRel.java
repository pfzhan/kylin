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

package org.apache.kylin.query.util;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RelTransformTestInOLAPProjectRel {

    private RexBuilder rexBuilder = null;
    private RexNode x, y, z;
    private RexNode literalOne, literalTwo, literalThree;
    private RelDataType boolRelDataType;
    private RelDataTypeFactory typeFactory;

    @Before
    public void setUp() {

        typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);
        boolRelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        x = new RexInputRef(0, typeFactory.createTypeWithNullability(boolRelDataType, true));
        y = new RexInputRef(1, typeFactory.createTypeWithNullability(boolRelDataType, true));
        z = new RexInputRef(2, typeFactory.createTypeWithNullability(boolRelDataType, true));
        literalOne = rexBuilder.makeLiteral("1");
        literalTwo = rexBuilder.makeLiteral("2");
        literalThree = rexBuilder.makeLiteral("3");
    }

    @After
    public void testDown() {
        typeFactory = null;
        rexBuilder = null;
        boolRelDataType = null;
        x = y = z = null;
    }

    private RexNode lessThan(RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, a0, a1);
    }

    private RexNode greaterThan(RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, a0, a1);
    }

    @Test
    public void testTransformRexNode() {
        RexNode node1 = greaterThan(x, literalOne);
        RexNode node2 = lessThan(y, literalTwo);
        RexNode node3 = greaterThan(z, literalThree);

        // test flatten and
        RexNode structuredAND = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.AND, node1, node2), node3);
        RexNode flattenAnd = RexUtil.flatten(rexBuilder, structuredAND);
        RexNode transformedAndRexNode = RexToTblColRefTranslator.createLeftCall(flattenAnd);
        Assert.assertEquals("AND(AND(>($0, '1'), <($1, '2')), >($2, '3'))", structuredAND.toString());
        Assert.assertEquals("AND(>($0, '1'), <($1, '2'), >($2, '3'))", flattenAnd.toString());
        Assert.assertEquals("AND(AND(>($0, '1'), <($1, '2')), >($2, '3'))", transformedAndRexNode.toString());

        // test flatten or
        RexNode structuredOR = rexBuilder.makeCall(SqlStdOperatorTable.OR,
                rexBuilder.makeCall(SqlStdOperatorTable.OR, node1, node2), node3);
        Assert.assertEquals("OR(OR(>($0, '1'), <($1, '2')), >($2, '3'))", structuredOR.toString());
        RexNode originOrRexNode = RexUtil.flatten(rexBuilder, structuredOR);
        RexNode transformedOrRexNode = RexToTblColRefTranslator.createLeftCall(originOrRexNode);
        Assert.assertEquals("OR(>($0, '1'), <($1, '2'), >($2, '3'))", originOrRexNode.toString());
        Assert.assertEquals("OR(OR(>($0, '1'), <($1, '2')), >($2, '3'))", transformedOrRexNode.toString());

        // test will not flatten case
        RexNode complex = rexBuilder.makeCall(SqlStdOperatorTable.OR,
                rexBuilder.makeCall(SqlStdOperatorTable.AND, node1, node2),
                rexBuilder.makeCall(SqlStdOperatorTable.AND, node2, node3));
        RexNode complexNotFlatten = RexUtil.flatten(rexBuilder, complex);
        RexNode transformedComplex = RexToTblColRefTranslator.createLeftCall(complexNotFlatten);
        String expected = "OR(AND(>($0, '1'), <($1, '2')), AND(<($1, '2'), >($2, '3')))";
        Assert.assertEquals(expected, complex.toString());
        Assert.assertEquals(expected, complexNotFlatten.toString());
        Assert.assertEquals(expected, transformedComplex.toString());
    }
}
