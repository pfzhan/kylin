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

package io.kyligence.kap.rest.config.initialize;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.SmartMaster;
import lombok.val;

public class SourceUsageUpdateListenerMetaTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testRealTimeTableVerify() {

        val project = "ssb";
        // prepare an origin model
        AbstractContext smartContext = new SmartContext(getTestConfig(), project,
                new String[] { "SELECT DATES.D_DATE FROM SSB.P_LINEORDER AS LINEORDER \n"
                        + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                        + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                        + "where LO_ORDERDATE='2020-10-01 00:00:00'" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();

        val model = NDataModelManager.getInstance(getTestConfig(), project).listAllModels().get(0);
        val partColRef = new TblColRef(model.getRootFactTable(),
                model.getColRef(model.getColumnIdByColumnName("P_LINEORDER.LO_ORDERDATE")).getColumnDesc());
        partColRef.getTableRef().getTableDesc().setSourceType(ISourceAware.ID_STREAMING);
        model.setPartitionDesc(new PartitionDesc());
        model.getPartitionDesc().setPartitionDateColumnRef(partColRef);

        NDataModelManager.getInstance(getTestConfig(), project).copyForWrite(model);

        Boolean verifyResult = ReflectionTestUtils.invokeMethod(new SourceUsageUpdateListener(), "verifyProject",
                getTestConfig(), project, null, null);

        Assert.assertTrue(verifyResult);

    }
}
