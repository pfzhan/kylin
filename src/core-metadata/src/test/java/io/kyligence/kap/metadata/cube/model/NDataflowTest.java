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

package io.kyligence.kap.metadata.cube.model;

import java.io.IOException;

import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.var;

public class NDataflowTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws IOException {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        NDataflow df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        IndexPlan cube = df.getIndexPlan();

        Assert.assertNotNull(df);
        Assert.assertNotNull(cube);
        Assert.assertSame(getTestConfig(), df.getConfig().base());
        Assert.assertEquals(getTestConfig(), df.getConfig());
        Assert.assertEquals(getTestConfig().hashCode(), df.getConfig().hashCode());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

        Segments<NDataSegment> segments = df.getSegments();
        Assert.assertEquals(1, segments.size());
        for (NDataSegment seg : segments) {
            Assert.assertNotNull(seg);
            Assert.assertNotNull(seg.getName());
        }
    }

    @Test
    public void getConfig() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val indexPlan = indexPlanManager.getIndexPlanByModelAlias("nmodel_basic");
        val indexPlanConfig = indexPlan.getConfig();
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        var config = df.getConfig();
        Assert.assertEquals(indexPlanConfig.base(), config.base());
        Assert.assertEquals(2, config.getExtendedOverrides().size());

        indexPlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.getOverrideProps().put("test", "test");
        });

        config = df.getConfig();
        Assert.assertEquals(indexPlanConfig.base(), config.base());
        Assert.assertEquals(3, config.getExtendedOverrides().size());
    }

    @Test
    public void testCollectPrecalculationResource() {
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), "cc_test");
        val df = dsMgr.getDataflowByModelAlias("test_model");
        val strings = df.collectPrecalculationResource();
        Assert.assertEquals(9, strings.size());

        Assert.assertTrue(strings.stream().anyMatch(path -> path.equals("/_global/project/cc_test.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/model_desc/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/index_plan/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/dataflow/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/dataflow_details/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/table/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/table_exd/")));
    }

    @Test
    public void testCollectPrecalculationResource_Streaming() {
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), "streaming_test");
        val df = dsMgr.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        val strings = df.collectPrecalculationResource();
        Assert.assertEquals(7, strings.size());

        Assert.assertTrue(strings.stream()
                .anyMatch(path -> path.equals("/streaming_test/dataflow/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith(
                "/streaming_test/dataflow_details/4965c827-fbb4-4ea1-a744-3f341a3b030d/3e560d22-b749-48c3-9f64-d4230207f120.json")));
        Assert.assertTrue(strings.stream().anyMatch(
                path -> path.startsWith("/streaming_test/index_plan/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/_global/project/streaming_test.json")));
        Assert.assertTrue(strings.stream().anyMatch(
                path -> path.startsWith("/streaming_test/model_desc/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(
                strings.stream().anyMatch(path -> path.startsWith("/streaming_test/table/DEFAULT.SSB_STREAMING.json")));
        Assert.assertTrue(
                strings.stream().anyMatch(path -> path.startsWith("/streaming_test/kafka/DEFAULT.SSB_STREAMING.json")));
    }
}
