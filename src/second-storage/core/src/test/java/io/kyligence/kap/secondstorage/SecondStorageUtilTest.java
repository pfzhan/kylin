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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.NManager;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecondStorageNodeHelper.class, NExecutableManager.class})
public class SecondStorageUtilTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private NManager<TableFlow> tableFlowManager = Mockito.mock(NManager.class);
    private NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);

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
        PowerMockito.stub(PowerMockito.method(NExecutableManager.class, "getInstance", KylinConfig.class, String.class)).toReturn(executableManager);
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

    @Test
    public void setSecondStorageSizeInfo() throws Exception {
        prepareManger();
        Mockito.when(tableFlowManager.get(Mockito.anyString())).thenReturn(Optional.empty());
        NDataModel model = new NDataModel();
        List<NDataModel> models = new ArrayList<>();
        models.add(model);
        List<SecondStorageInfo> newModels = SecondStorageUtil.setSecondStorageSizeInfo(models, tableFlowManager);
        Assert.assertEquals(0, (newModels.get(0)).getSecondStorageSize());
        Assert.assertEquals(0, (newModels.get(0)).getSecondStorageNodes().size());

        Mockito.when(tableFlowManager.get(Mockito.anyString())).thenReturn(Optional.of(prepareTableFlow()));
        List<SecondStorageInfo> secondStorageInfos = SecondStorageUtil.setSecondStorageSizeInfo(models, tableFlowManager);
        Assert.assertEquals(0, (newModels.get(0)).getSecondStorageSize());
        Assert.assertEquals(0, (newModels.get(0)).getSecondStorageNodes().size());
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
}