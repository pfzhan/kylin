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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.ReflectionUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecondStorageNodeHelper.class, ExecutableManager.class})
public class SecondStorageUtilTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private Manager<TableFlow> tableFlowManager = Mockito.mock(Manager.class);
    private ExecutableManager executableManager = Mockito.mock(ExecutableManager.class);

    @Before
    public void setUp() throws Exception {
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    private void prepareManger() {
        PowerMockito.stub(PowerMockito.method(ExecutableManager.class, "getInstance", KylinConfig.class, String.class)).toReturn(executableManager);
    }

    private TableFlow prepareTableFlow() throws NoSuchFieldException {
        TableFlow tableFlow = new TableFlow();
        TableData tableData = new TableData();
        tableData.addPartition(TablePartition.builder().setNodeFileMap(Collections.emptyMap())
                .setSizeInNode(Collections.emptyMap())
                .setShardNodes(Collections.emptyList())
                .setSegmentId("test")
                .build());
        Field tableDataField = tableFlow.getClass().getDeclaredField("tableDataList");
        ReflectionUtils.makeAccessible(tableDataField);
        List<TableData> tableDataList = (List<TableData>) ReflectionUtils.getField(tableDataField, tableFlow);
        tableDataList.add(tableData);
        return tableFlow;
    }

    private TableFlow prepareTableFlow2Partition() throws NoSuchFieldException {
        TableFlow tableFlow = new TableFlow();
        TableData tableData = new TableData();
        tableData.addPartition(TablePartition.builder().setNodeFileMap(Collections.emptyMap())
                .setSizeInNode(Collections.emptyMap())
                .setShardNodes(ImmutableList.of("node01", "node02"))
                .setSegmentId("test1")
                .build());
        tableData.addPartition(TablePartition.builder().setNodeFileMap(Collections.emptyMap())
                .setSizeInNode(Collections.emptyMap())
                .setShardNodes(ImmutableList.of("node01", "node02"))
                .setSegmentId("test2")
                .build());
        Field tableDataField = tableFlow.getClass().getDeclaredField("tableDataList");
        ReflectionUtils.makeAccessible(tableDataField);
        List<TableData> tableDataList = (List<TableData>) ReflectionUtils.getField(tableDataField, tableFlow);
        tableDataList.add(tableData);

        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        cluster.setCluster(clusterNodes);
        clusterNodes.put("pair1",
                ImmutableList.of(new Node().setName("node01").setIp("127.0.0.1").setPort(9000),
                new Node().setName("node02").setIp("127.0.0.1").setPort(9000)));
        SecondStorageNodeHelper.initFromCluster(cluster, null, null);
        return tableFlow;
    }

    @Test
    public void testConvertNodesToPairs() throws NoSuchFieldException {
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        clusterNodes.put("pair1",
                ImmutableList.of(new Node().setName("node01").setIp("127.0.0.1").setPort(9000),
                        new Node().setName("node02").setIp("127.0.0.1").setPort(9000)));
        clusterNodes.put("pair2",
                ImmutableList.of(new Node().setName("node03").setIp("127.0.0.1").setPort(9000),
                        new Node().setName("node04").setIp("127.0.0.1").setPort(9000)));
        cluster.setCluster(clusterNodes);
        SecondStorageNodeHelper.initFromCluster(cluster, null, null);
        List<String> nodes1 = Arrays.asList("node01", "node02");
        Map<String, List<SecondStorageNode>> pairs1 = SecondStorageUtil.convertNodesToPairs(nodes1);
        Assert.assertEquals(1, pairs1.size());
        Assert.assertEquals(2, pairs1.get("pair1").size());

        List<String> nodes2 = Arrays.asList("node01", "node03");
        Map<String, List<SecondStorageNode>> pairs2 = SecondStorageUtil.convertNodesToPairs(nodes2);
        Assert.assertEquals(2, pairs2.size());
        Assert.assertEquals(1, pairs2.get("pair1").size());
    }

    @Test
    public void transformNode() {
        Node node = new Node().setIp("127.0.0.1")
                .setName("test")
                .setPort(3000);
        PowerMockito.stub(PowerMockito.method(SecondStorageNodeHelper.class, "getNode", String.class)).toReturn(node);
        SecondStorageNode secondStorageNode = SecondStorageUtil.transformNode("test");
        Assert.assertEquals("127.0.0.1", secondStorageNode.getIp());
        Assert.assertEquals("test", secondStorageNode.getName());
        Assert.assertEquals(3000, secondStorageNode.getPort());
    }

    @Test
    public void isTableFlowEmpty() throws Exception {
        Assert.assertTrue(SecondStorageUtil.isTableFlowEmpty(new TableFlow()));
        Assert.assertFalse(SecondStorageUtil.isTableFlowEmpty(prepareTableFlow()));
    }

    @Test
    public void findSecondStorageJobByProject() {
        prepareManger();
        List<String> jobs = Arrays.asList("job1", "job2");
        AbstractExecutable job1 = Mockito.mock(AbstractExecutable.class);
        AbstractExecutable job2 = Mockito.mock(AbstractExecutable.class);
        Mockito.when(job1.getJobType()).thenReturn(JobTypeEnum.INDEX_BUILD);
        Mockito.when(job2.getJobType()).thenReturn(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        Mockito.when(executableManager.getJobs()).thenReturn(jobs);
        Mockito.when(executableManager.getJob("job1")).thenReturn(job1);
        Mockito.when(executableManager.getJob("job2")).thenReturn(job2);
        Assert.assertEquals(2, SecondStorageUtil.findSecondStorageRelatedJobByProject("test").size());
    }

    @Test
    public void checkJobRestartWhenNotEnable() {
        SecondStorageUtil.checkJobRestart("default", RandomUtil.randomUUIDStr());
    }

    @Test
    public void checkJobResumeAndRemoveWhenNotEnable() {
        SecondStorageUtil.checkJobResume("default", RandomUtil.randomUUIDStr());
        SecondStorageUtil.checkSegmentRemove("default", RandomUtil.randomUUIDStr(), new String[]{RandomUtil.randomUUIDStr()});
    }
}