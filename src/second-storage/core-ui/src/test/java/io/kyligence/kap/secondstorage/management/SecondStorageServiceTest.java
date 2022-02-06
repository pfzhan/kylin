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

package io.kyligence.kap.secondstorage.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ NProjectManager.class, SecondStorageUtil.class, NExecutableManager.class, UserGroupInformation.class,
        AbstractExecutable.class })
public class SecondStorageServiceTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
    private SecondStorageService secondStorageService = new SecondStorageService();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        Unsafe.setProperty("kylin.external-storage.cluster.config", "src/test/resources/test.yaml");
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        Unsafe.clearProperty("kylin.external-storage.cluster.config");
        cleanupTestMetadata();
    }

    @Test
    public void listAvailableNodes() {
        PowerMockito.mockStatic(NProjectManager.class);
        val projectManager = Mockito.mock(NProjectManager.class);
        PowerMockito.when(NProjectManager.getInstance(Mockito.any(KylinConfig.class))).thenReturn(projectManager);
        Mockito.when(projectManager.listAllProjects()).thenReturn(Collections.emptyList());
        Cluster cluster = new Cluster();
        List<Node> nodes = new ArrayList<>();
        cluster.setNodes(nodes);
        nodes.add(new Node().setName("node01").setIp("127.0.0.1").setPort(9000));
        nodes.add(new Node().setName("node02").setIp("127.0.0.2").setPort(9000));
        nodes.add(new Node().setName("node03").setIp("127.0.0.3").setPort(9000));
        SecondStorageNodeHelper.initFromCluster(cluster, null);
        val result = secondStorageService.listAvailableNodes();
        Assert.assertEquals(3, result.size());
    }

    private void prepareManger() {
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "getProjectLocks", String.class))
                .toReturn(new ArrayList<>());
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isGlobalEnable")).toReturn(true);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable", String.class)).toReturn(true);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable", String.class, String.class))
                .toReturn(true);
        PowerMockito.stub(PowerMockito.method(NExecutableManager.class, "getInstance", KylinConfig.class, String.class))
                .toReturn(executableManager);
    }

    @Test
    public void validateProjectDisable() {
        ProjectEnableRequest projectEnableRequest = new ProjectEnableRequest();
        projectEnableRequest.setProject("project");
        projectEnableRequest.setEnabled(false);
        prepareManger();
        List<String> jobs = Arrays.asList("job1", "job2");
        AbstractExecutable job1 = PowerMockito.mock(AbstractExecutable.class);
        AbstractExecutable job2 = PowerMockito.mock(AbstractExecutable.class);
        PowerMockito.when(job1.getStatus()).thenReturn(ExecutableState.RUNNING);
        PowerMockito.when(job2.getStatus()).thenReturn(ExecutableState.SUCCEED);
        PowerMockito.when(job1.getJobType()).thenReturn(JobTypeEnum.INDEX_BUILD);
        PowerMockito.when(job2.getJobType()).thenReturn(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);

        Mockito.when(job1.getProject()).thenReturn("project");
        Mockito.when(job2.getProject()).thenReturn("project");

        Mockito.when(job1.getTargetSubject()).thenReturn("model1");
        Mockito.when(job2.getTargetSubject()).thenReturn("model2");

        Mockito.when(executableManager.getJobs()).thenReturn(jobs);
        Mockito.when(executableManager.getJob("job1")).thenReturn(job1);
        Mockito.when(executableManager.getJob("job2")).thenReturn(job2);
        Assert.assertEquals(1, secondStorageService.validateProjectDisable(projectEnableRequest.getProject()).size());
    }

    @Test
    public void projecLoad() {
        PowerMockito.mockStatic(NProjectManager.class);
        val projectManager = Mockito.mock(NProjectManager.class);
        PowerMockito.when(NProjectManager.getInstance(Mockito.any(KylinConfig.class))).thenReturn(projectManager);
        Mockito.when(projectManager.listAllProjects()).thenReturn(Collections.emptyList());

        Cluster cluster = new Cluster();
        List<Node> nodes = new ArrayList<>();
        List<String> nodeNames = new ArrayList<>();
        cluster.setNodes(nodes);
        nodes.add(new Node().setName("node01").setIp("127.0.0.1").setPort(9000));
        SecondStorageNodeHelper.initFromCluster(cluster, null);
        nodes.forEach(node -> nodeNames.add(node.getName()));

        prepareManger();
        Assert.assertEquals(1, secondStorageService.projectLoadData(Lists.newArrayList("project")).getLoads().size());

    }
}