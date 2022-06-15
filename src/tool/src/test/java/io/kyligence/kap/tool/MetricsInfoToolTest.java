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

package io.kyligence.kap.tool;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.metrics.MetricsInfo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsInfoToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testMetricsInfoSuccess() throws Exception {
        MetricsInfoTool tool = new MetricsInfoTool();
        String date = DateFormat.formatToDateStr(System.currentTimeMillis(), DateFormat.COMPACT_DATE_PATTERN);
        tool.execute(new String[] { "-date", date });
        MetricsInfo info = tool.inPutMetricsInfo(date);
        Assert.assertNotNull(info);
        Assert.assertTrue(info.getProjectMetrics().size() > 0);
        Assert.assertNull(info.getProjectMetrics().get(0).getModelAddCount());

    }

    @Test
    public void testMetricsInfoFail() throws Exception {
        testMetricsInfoFail("20180101");
        testMetricsInfoFail("20210229");
        testMetricsInfoFail("20211301");
        String date = DateFormat.formatToDateStr(System.currentTimeMillis(), DateFormat.COMPACT_DATE_PATTERN);
        testMetricsInfoFail("0" + date);

    }

    private void testMetricsInfoFail(String date) {
        MetricsInfoTool tool = new MetricsInfoTool();
        boolean result = true;
        try {
            tool.execute(new String[] { "-date", date });
        } catch (Exception e) {
            result = false;
        }
        Assert.assertFalse(result);
    }

    private void prepare() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(config);
    }

}
