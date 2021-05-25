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
package io.kyligence.kap.rest.service;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class StreamingTableServiceTest extends CSVSourceTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @InjectMocks
    private StreamingTableService streamingTableService = Mockito.spy(new StreamingTableService());

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private static String PROJECT = "streaming_test";

    @Before
    public void setup() {
        super.setup();
        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(streamingTableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(streamingTableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(streamingTableService, "userGroupService", userGroupService);

        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableService, "userGroupService", userGroupService);

        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject(PROJECT);
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }

        EventBusFactory.getInstance().register(modelBrokenListener, false);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testInnerReloadTable() {
        val database = "SSB";

        val config = getTestConfig();
        try {
            val tableDescList = tableService.getTableDesc(PROJECT, true, "P_LINEORDER_STR", database, true);
            Assert.assertEquals(1, tableDescList.size());
            val tableDesc = tableDescList.get(0);
            val tableExtDesc = tableService.getOrCreateTableExt(PROJECT, tableDesc);
            val list = streamingTableService.innerReloadTable(PROJECT, tableDesc, tableExtDesc);
            Assert.assertEquals(0, list.size());
        } catch (Exception e) {
            Assert.fail();
        }

    }

    @Test
    public void testReloadTable() {
        val database = "DEFAULT";

        val config = getTestConfig();
        try {
            val tableDescList = tableService.getTableDesc(PROJECT, true, "", database, true);
            Assert.assertEquals(1, tableDescList.size());
            val tableDesc = tableDescList.get(0);
            val tableExtDesc = tableService.getOrCreateTableExt(PROJECT, tableDesc);
            streamingTableService.reloadTable(PROJECT, tableDesc, tableExtDesc);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateKafkaConfig() {
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("DEFAULT");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingTableService.createKafkaConfig(PROJECT, kafkaConfig);

        val kafkaConf = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.TPCH_TOPIC");
        Assert.assertEquals("DEFAULT", kafkaConf.getDatabase());
        Assert.assertEquals("TPCH_TOPIC", kafkaConf.getName());
        Assert.assertEquals("10.1.2.210:9092", kafkaConf.getKafkaBootstrapServers());
        Assert.assertEquals("tpch_topic", kafkaConf.getSubscribe());
        Assert.assertEquals("latest", kafkaConf.getStartingOffsets());
    }

    @Test
    public void testUpdateKafkaConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val kafkaConfig = KafkaConfigManager.getInstance(kylinConfig, PROJECT).getKafkaConfig("DEFAULT.SSB_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9093");
        streamingTableService.updateKafkaConfig(PROJECT, kafkaConfig);
        val kafkaConf = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.SSB_TOPIC");
        Assert.assertEquals("10.1.2.210:9093", kafkaConf.getKafkaBootstrapServers());
    }

}
