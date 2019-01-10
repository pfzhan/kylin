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
package org.apache.kylin;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.gridtable.NCuboidToGridTableMapping;
import io.kyligence.kap.metadata.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.cube.gridtable.GridTableMapping;
import org.apache.kylin.cube.gridtable.GridTables;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;

public class GridTablesTest extends NLocalFileMetadataTestCase {

    public static final String DEFAULT_PROJECT = "default";

    private ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        buffer.clear();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    //fixme
    @Ignore("dict dir been deleted.")
    public void testGTInfo() {

        NDataflowManager mgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        NDataSegment dataSegment = df.getSegments(SegmentStatusEnum.READY).get(0);
        LayoutEntity cuboidLayout = df.getIndexPlan().getAllLayouts().get(0);
        GridTableMapping mapping = new NCuboidToGridTableMapping(cuboidLayout);
        GTInfo info = GridTables.newGTInfo(mapping, new NCubeDimEncMap(dataSegment));
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    private void compareTwoGTInfo(GTInfo info, GTInfo sInfo) {
        Assert.assertEquals(info.getTableName(), sInfo.getTableName());
        Assert.assertEquals(info.getPrimaryKey(), sInfo.getPrimaryKey());

        for (int i = 0; i < info.getColTypes().length; i++) {
            Assert.assertEquals(info.getCodeSystem().maxCodeLength(i), sInfo.getCodeSystem().maxCodeLength(i));
            Assert.assertTrue(info.getCodeSystem().maxCodeLength(i) > 0);
            Assert.assertEquals(info.colRef(i), sInfo.colRef(i));
        }
        Assert.assertArrayEquals(info.getColumnBlocks(), sInfo.getColumnBlocks());
        Assert.assertEquals(info.getRowBlockSize(), sInfo.getRowBlockSize());
        Assert.assertEquals(info.getRowBlockSize(), sInfo.getRowBlockSize());

    }
}