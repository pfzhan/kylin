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

package io.kyligence.kap.metadata.model;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinTableDescTest {

    private JoinTableDesc first;
    private JoinTableDesc second;

    @Before
    public void setUp() {
        first = new JoinTableDesc();
        JoinDesc join = new JoinDesc();
        join.setForeignKey(new String[] { "a.col" });
        join.setPrimaryKey(new String[] { "b.col" });
        join.setType("inner");
        join.setPrimaryTable("s.b");
        join.setForeignTable("s.a");
        first.setJoin(join);

        second = new JoinTableDesc();
        JoinDesc join2 = new JoinDesc();
        join2.setForeignKey(new String[] { "a.col" });
        join2.setPrimaryKey(new String[] { "b.col" });
        join2.setType("inner");
        join2.setPrimaryTable("s.b");
        join2.setForeignTable("s.a");
        second.setJoin(join2);
    }

    @Test
    public void basicTest() {

        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable("other");
        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertFalse(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        first.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
        Assert.assertFalse(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertTrue(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.FLATTEN);
        first.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
        Assert.assertTrue(first.isFlattenable());
        Assert.assertTrue(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());
    }

    @Test
    public void testHasDifferentAntiFlattenable() {

        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.FLATTEN);
        second.setFlattenable(null);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.FLATTEN);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(null);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable("other");
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable("other");
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));
    }
}
