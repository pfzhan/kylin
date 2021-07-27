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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class ComputedColumnDescTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void simpleParserCheckTestSuccess1() {
        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + b.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail1() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t recognize column \"C.Y\". Please use \"TABLE_ALIAS.COLUMN\" to reference a column.");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + c.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail2() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: SUM");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("sum(a.x) * 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail3() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MIN");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("min(a.x) + 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail4() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MAX");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("max(a.x + b.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail5() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(*)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail6() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail7() {
        //value window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: LEAD");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("lead(a.x,1) over (order by a.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail8() {
        //ranking window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: NTILE");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("ntile(5) over(order by a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail9() {
        //aggregate function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: AVG");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) ", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail10() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain keyword AS");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) as xxx", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail11() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(distinct a.x)", aliasSet);
    }

    @Test
    public void testUnwrap() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        var dataModelDesc = dataModelManager.getDataModelDesc("4a45dc4d-937e-43cc-8faa-34d59d4e11d3");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_NUM"))
                .findAny().get();

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // 1+2
        List<TblColRef> tblColRefList = ComputedColumnDesc.unwrap(dataModelDesc,
                computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(0, tblColRefList.size());


        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_LTAX"))
                .findAny().get();

        // LINEORDER`.`LO_TAX` +1
        tblColRefList = ComputedColumnDesc.unwrap(dataModelDesc,
                computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertEquals("LINEORDER.LO_TAX", tblColRefList.get(0).getIdentity());

        dataModelDesc = dataModelManager.getDataModelDesc("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");

        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_TOTAL_TAX"))
                .findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // LINEORDER.LO_QUANTITY*LINEORDER.LO_TAX
        tblColRefList = ComputedColumnDesc.unwrap(dataModelDesc,
                computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(2, tblColRefList.size());

        Assert.assertTrue(tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_QUANTITY")));
        Assert.assertTrue(tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_TAX")));

        computedColumn = dataModelDesc.getAllTableRefs()
                        .stream().filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER"))
                        .map(TableRef::getColumns)
                                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_EXTRACT"))
                        .findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // MINUTE(`LINEORDER`.`LO_ORDERDATE`)
        tblColRefList = ComputedColumnDesc.unwrap(dataModelDesc,
                computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERDATE")));

        computedColumn = dataModelDesc.getAllTableRefs()
                .stream().filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER"))
                .map(TableRef::getColumns)
                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_CAST_LO_ORDERKEY"))
                .findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // cast(`lineorder`.`lo_orderkey` as double)
        tblColRefList = ComputedColumnDesc.unwrap(dataModelDesc,
                computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERKEY")));

    }

}
