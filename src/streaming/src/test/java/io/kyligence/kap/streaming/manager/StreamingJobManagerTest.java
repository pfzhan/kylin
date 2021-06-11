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
package io.kyligence.kap.streaming.manager;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import lombok.val;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class StreamingJobManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private StreamingJobManager mgr;
    private static String PROJECT = "streaming_test";
    private String modelTest = "model_test";
    private String ownerTest = "owner_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStreamingJobByUuid() {
        val emptyId = "";
        Assert.assertNull(mgr.getStreamingJobByUuid(emptyId));

        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        Assert.assertNotNull(streamingJobMeta);
    }

    @Test
    public void testCreateStreamingJobForBuild() {
        val uuid = UUID.randomUUID().toString();
        val model = mockModel(uuid);
        mgr.createStreamingJob(model, JobTypeEnum.STREAMING_BUILD);
        val meta = mgr.getStreamingJobByUuid(uuid + "_build");
        Assert.assertNotNull(meta);
        Assert.assertEquals(uuid + "_build", meta.getUuid());
        Assert.assertEquals(ownerTest, meta.getOwner());
        Assert.assertEquals(uuid, meta.getModelId());
        Assert.assertEquals(modelTest, meta.getModelName());
        Assert.assertEquals(PROJECT, meta.getProject());
    }

    @Test
    public void testCreateStreamingJobForMerge() {
        val uuid = UUID.randomUUID().toString();
        val model = mockModel(uuid);
        mgr.createStreamingJob(model, JobTypeEnum.STREAMING_MERGE);
        val meta = mgr.getStreamingJobByUuid(uuid + "_merge");
        Assert.assertNotNull(meta);
        Assert.assertEquals(uuid + "_merge", meta.getUuid());
        Assert.assertEquals(ownerTest, meta.getOwner());
        Assert.assertEquals(uuid, meta.getModelId());
        Assert.assertEquals(modelTest, meta.getModelName());
        Assert.assertEquals(PROJECT, meta.getProject());
    }

    @Test
    public void testCreateStreamingJob() {
        val uuid = UUID.randomUUID().toString();
        val model = mockModel(uuid);
        mgr.createStreamingJob(model);
        val buildMeta = mgr.getStreamingJobByUuid(uuid + "_build");
        Assert.assertNotNull(buildMeta);
        val mergeMeta = mgr.getStreamingJobByUuid(uuid + "_merge");
        Assert.assertNotNull(mergeMeta);
    }

    @Test
    public void testCopy() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        val copyMeta = mgr.copy(streamingJobMeta);
        Assert.assertNotNull(copyMeta);
        Assert.assertEquals(streamingJobMeta, copyMeta);
        Assert.assertNotEquals(System.identityHashCode(streamingJobMeta), System.identityHashCode(copyMeta));
    }

    @Test
    public void testUpdateStreamingJob() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val result = mgr.updateStreamingJob(id, copyForWrite -> {
            copyForWrite.setProcessId("9999");
            copyForWrite.setNodeInfo("localhost:7070");
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        Assert.assertNotNull(result);
        val meta = mgr.getStreamingJobByUuid(id);
        Assert.assertEquals("9999", meta.getProcessId());
        Assert.assertEquals("localhost:7070", meta.getNodeInfo());
        Assert.assertEquals(JobStatusEnum.ERROR, meta.getCurrentStatus());
    }

    @Test
    public void testUpdateStreamingJobOfErrorId() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b7_build";
        val result = mgr.updateStreamingJob(id, copyForWrite -> {
            copyForWrite.setProcessId("9999");
            copyForWrite.setNodeInfo("localhost:7070");
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        Assert.assertNull(result);
        val meta = mgr.getStreamingJobByUuid(id);
        Assert.assertNull(meta);
    }

    @Test
    public void testDeleteStreamingJob() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        mgr.deleteStreamingJob(id);
        val meta = mgr.getStreamingJobByUuid(id);
        Assert.assertNull(meta);
    }

    @Test
    public void testListAllStreamingJobMeta() {
        val lists = mgr.listAllStreamingJobMeta();
        Assert.assertEquals(8, lists.size());
    }

    private NDataModel mockModel(String uuid) {
        NDataModel model = new NDataModel();
        model.setUuid(uuid);
        model.setProject(PROJECT);
        model.setAlias(modelTest);
        model.setOwner(ownerTest);
        val tableName = "DEFAULT.SSB_TOPIC";
        model.setRootFactTableName(tableName);
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName("test_measure");
        measure.setFunction(FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
                Lists.newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        model.setAllMeasures(Lists.newArrayList(measure));

        val kafkaConf = new KafkaConfig();
        kafkaConf.setProject(PROJECT);
        kafkaConf.setDatabase("DEFAULT");
        kafkaConf.setName("SSB_TOPIC");
        kafkaConf.setKafkaBootstrapServers("10.1.2.210:9094");
        kafkaConf.setStartingOffsets("earliest");

        final TableDesc tableDesc = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName);
        model.setRootFactTableRef(new TableRef(model, model.getAlias(), tableDesc, true));
        return model;
    }
}
