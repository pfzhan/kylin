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

import java.util.Arrays;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
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
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.rest.request.StreamingRequest;
import lombok.val;

public class StreamingTableServiceTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

//    @Mock
//    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @InjectMocks
    private StreamingTableService streamingTableService = Mockito.spy(new StreamingTableService());

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);


    private static String PROJECT = "streaming_test";

    @Before
    public void setup() {
        ExecutableUtils.initJobFactory();
        createTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");

        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(streamingTableService, "aclEvaluate", aclEvaluate);
        //ReflectionTestUtils.setField(streamingTableService, "aclTCRService", aclTCRService);
        //ReflectionTestUtils.setField(streamingTableService, "userGroupService", userGroupService);
        //ReflectionTestUtils.setField(streamingTableService,"tableSupporters", Arrays.asList(tableService));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        //ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
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
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testInnerReloadTable() {
        val database = "SSB";

        val config = getTestConfig();
        try {
            val tableDescList = tableService.getTableDesc(PROJECT, true, "P_LINEORDER_STR", database, false);
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
            Assert.assertEquals(2, tableDescList.size());
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

    @Test
    public void testDecimalConvertToDouble() {
        StreamingRequest streamingRequest = new StreamingRequest();
        TableDesc tableDesc = new TableDesc();
        tableDesc.setColumns(new ColumnDesc[] { new ColumnDesc("1", "name1", "DECIMAL", "", "", "", ""),
                new ColumnDesc("2", "name2", "double", "", "", "", ""),
                new ColumnDesc("3", "name3", "int", "", "", "", "") });
        streamingRequest.setTableDesc(tableDesc);

        streamingTableService.decimalConvertToDouble(PROJECT, streamingRequest);

        Assert.assertEquals(2L, Arrays.stream(streamingRequest.getTableDesc().getColumns())
                .filter(column -> StringUtils.equalsIgnoreCase(column.getDatatype(), DataType.DOUBLE)).count());
    }

    @Test
    public void testCheckColumnsNotMatch() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setColumns(new ColumnDesc[] { new ColumnDesc("1", "name1", "DECIMAL", "", "", "", ""),
                new ColumnDesc("2", "name2", "double", "", "", "", ""),
                new ColumnDesc("3", "name3", "int", "", "", "", "") });
        streamingRequest.setTableDesc(tableDesc);
        val kafkaConfig = new KafkaConfig();
        val batchTableName = "SSB.P_LINEORDER";
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setBatchTable(batchTableName);
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT,
                MsgPicker.getMsg().getBATCH_STREAM_TABLE_NOT_MATCH(), batchTableName));
        streamingTableService.checkColumns(streamingRequest);
    }

    /**
     * fusion model check
     */
    @Test
    public void testCheckColumnsNoTimestampPartition() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc streamingTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getTableDesc("SSB.LINEORDER");
        streamingRequest.setTableDesc(streamingTableDesc);

        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setBatchTable("SSB.LINEORDER");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getTIMESTAMP_COLUMN_NOT_EXIST());
        streamingTableService.checkColumns(streamingRequest);
    }

    /**
     * streaming model check
     */
    @Test
    public void testCheckColumnsNoTimestampPartition1() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc streamingTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getTableDesc("SSB.LINEORDER");
        streamingRequest.setTableDesc(streamingTableDesc);

        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getTIMESTAMP_COLUMN_NOT_EXIST());
        streamingTableService.checkColumns(streamingRequest);
    }
}
