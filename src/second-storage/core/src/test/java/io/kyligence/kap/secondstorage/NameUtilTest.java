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
package io.kyligence.kap.secondstorage;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import io.kyligence.kap.metadata.cube.model.NDataflow;


@RunWith(PowerMockRunner.class)
public class NameUtilTest {
    private static final String PROJECT = "test_table_index";
    private static final long LAYOUT_ID = 2000000001;
    private final NDataflow dataflow = Mockito.mock(NDataflow.class);
    private final KylinConfigExt config = Mockito.mock(KylinConfigExt.class);

    @Test
    public void testNameUtilInUT() {
        final String uuid = RandomUtil.randomUUIDStr();
        Mockito.when(dataflow.getConfig()).thenReturn(config);
        Mockito.when(dataflow.getProject()).thenReturn(PROJECT);
        Mockito.when(dataflow.getUuid()).thenReturn(uuid);
        Mockito.when(config.isUTEnv()).thenReturn(true);
        final String tablePrefix = NameUtil.tablePrefix(uuid);

        Assert.assertEquals("UT_" + PROJECT, NameUtil.getDatabase(dataflow));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).startsWith(tablePrefix));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).endsWith(String.valueOf(LAYOUT_ID)));

        Assert.assertEquals(PROJECT, NameUtil.recoverProject(NameUtil.getDatabase(dataflow), config));
        Assert.assertEquals(Pair.newPair(uuid, LAYOUT_ID),
                NameUtil.recoverLayout(NameUtil.getTable(dataflow, LAYOUT_ID)));
    }

    @Test
    public void testNameUtil() {
        final String uuid = RandomUtil.randomUUIDStr();
        final String metaUrl = "ke_metadata";
        Mockito.when(dataflow.getConfig()).thenReturn(config);
        Mockito.when(dataflow.getProject()).thenReturn(PROJECT);
        Mockito.when(dataflow.getUuid()).thenReturn(uuid);
        Mockito.when(config.isUTEnv()).thenReturn(false);
        Mockito.when(config.getMetadataUrlPrefix()).thenReturn(metaUrl);

        final String tablePrefix = NameUtil.tablePrefix(uuid);

        Assert.assertEquals(metaUrl + "_" + PROJECT, NameUtil.getDatabase(dataflow));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).startsWith(tablePrefix));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).endsWith(String.valueOf(LAYOUT_ID)));

        Assert.assertEquals(PROJECT, NameUtil.recoverProject(NameUtil.getDatabase(dataflow), config));
        Assert.assertEquals(Pair.newPair(uuid, LAYOUT_ID),
                NameUtil.recoverLayout(NameUtil.getTable(dataflow, LAYOUT_ID)));
    }
}
