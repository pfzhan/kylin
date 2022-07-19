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
package io.kyligence.kap.newten;

import java.util.Collection;
import java.util.Set;

import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.var;

public class IndexDependencyParserTest extends NLocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata("src/test/resources/ut_meta/heterogeneous_segment_2");
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void unwrapComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        var dataModelDesc = dataModelManager.getDataModelDesc("4a45dc4d-937e-43cc-8faa-34d59d4e11d3");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_NUM")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // 1+2
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(0, tblColRefList.size());

        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_LTAX")).findAny().get();

        // LINEORDER`.`LO_TAX` +1
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> "LINEORDER.LO_TAX".equals(tblColRef.getIdentity())));

        dataModelDesc = dataModelManager.getDataModelDesc("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");

        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_TOTAL_TAX")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // LINEORDER.LO_QUANTITY*LINEORDER.LO_TAX
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(2, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_QUANTITY")));
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_TAX")));

        computedColumn = dataModelDesc.getAllTableRefs().stream()
                .filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER")).map(TableRef::getColumns)
                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_EXTRACT")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // MINUTE(`LINEORDER`.`LO_ORDERDATE`)
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERDATE")));

        computedColumn = dataModelDesc.getAllTableRefs().stream()
                .filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER")).map(TableRef::getColumns)
                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_CAST_LO_ORDERKEY")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // cast(`lineorder`.`lo_orderkey` as double)
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERKEY")));

    }

    @Test
    public void unwrapNestComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        var dataModelDesc = dataModelManager.getDataModelDesc("4802b471-fb69-4b08-a45e-ab3e314e2f6c");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_LTAX_NEST")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> "LINEORDER.LO_TAX".equals(tblColRef.getIdentity())));
    }

    @Test
    public void unwrapDateComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "heterogeneous_segment_2");
        var dataModelDesc = dataModelManager.getDataModelDesc("3f2860d5-0a4c-4f52-b27b-2627caafe769");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC2")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(tblColRefList.stream()
                .anyMatch(tblColRef -> tblColRef.getExpressionInSourceDB().equals("KYLIN_SALES.PART_DT")));
    }
}