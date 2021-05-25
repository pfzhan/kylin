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
package io.kyligence.kap.streaming.metadata;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class StreamingJobMetaTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateBuildJob() {
        val config = KylinConfig.getInstanceFromEnv();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, PROJECT);
        val model = dataModelManager.getDataModelDesc(MODEL_ID);
        val jobStatus = JobStatusEnum.NEW;
        val jobType = JobTypeEnum.STREAMING_BUILD;
        val jobMeta = StreamingJobMeta.create(model, jobStatus, JobTypeEnum.STREAMING_BUILD);
        Assert.assertNotNull(jobMeta);
        val params = jobMeta.getParams();
        assertJobMeta(model, jobMeta, jobStatus, jobType);
        assertCommonParams(params);
        Assert.assertEquals(StreamingConstants.STREAMING_DURATION_DEFAULT,
                params.get(StreamingConstants.STREAMING_DURATION));
        Assert.assertEquals(StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION_DEFAULT,
                params.get(StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION));
    }

    @Test
    public void testCreateMergeJob() {
        val config = KylinConfig.getInstanceFromEnv();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, PROJECT);
        val model = dataModelManager.getDataModelDesc(MODEL_ID);
        val jobStatus = JobStatusEnum.NEW;
        val jobType = JobTypeEnum.STREAMING_MERGE;
        val jobMeta = StreamingJobMeta.create(model, jobStatus, jobType);
        Assert.assertNotNull(jobMeta);
        val params = jobMeta.getParams();
        assertJobMeta(model, jobMeta, jobStatus, jobType);
        assertCommonParams(params);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT,
                params.get(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE));
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT,
                params.get(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD));
    }

    private void assertJobMeta(NDataModel dataModel, StreamingJobMeta jobMeta, JobStatusEnum status,
            JobTypeEnum jobType) {
        Assert.assertEquals(status, jobMeta.getCurrentStatus());
        Assert.assertEquals(dataModel.getUuid(), jobMeta.getModelId());
        Assert.assertEquals(dataModel.getAlias(), jobMeta.getModelName());
        Assert.assertEquals(dataModel.getRootFactTableName(), jobMeta.getFactTableName());
        Assert.assertEquals(dataModel.getRootFactTable().getTableDesc().getKafkaConfig().getSubscribe(),
                jobMeta.getTopicName());
        Assert.assertEquals(dataModel.getOwner(), jobMeta.getOwner());
        Assert.assertEquals(StreamingUtils.getJobId(dataModel.getUuid(), jobType.name()), jobMeta.getUuid());

    }

    private void assertCommonParams(Map<String, String> params) {
        Assert.assertTrue(!params.isEmpty());
        Assert.assertEquals(StreamingConstants.SPARK_MASTER_DEFAULT, params.get(StreamingConstants.SPARK_MASTER));
        Assert.assertEquals(StreamingConstants.SPARK_DRIVER_MEM_DEFAULT,
                params.get(StreamingConstants.SPARK_DRIVER_MEM));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_INSTANCES));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_CORES));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_MEM));
        Assert.assertEquals(StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT,
                params.get(StreamingConstants.SPARK_SHUFFLE_PARTITIONS));
        Assert.assertEquals("false", params.get(StreamingConstants.STREAMING_RETRY_ENABLE));
    }
}
