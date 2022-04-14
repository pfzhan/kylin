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

package io.kyligence.kap.metadata.project;

import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.MaintainModelType;
import lombok.val;

public class NProjectManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectsFromResource() throws Exception {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        KylinConfig config = getTestConfig();
        KapConfig kapConf = KapConfig.wrap(config);

        String path = kapConf.getReadHdfsWorkingDirectory() + "dict-store/test";
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path metadataPath = new Path(path);
        if (!fs.exists(metadataPath)) {
            fs.mkdirs(metadataPath);
        }

        val projects = projectManager.listAllProjects();
        Assert.assertEquals(27, projects.size());
        Assert.assertTrue(projects.stream().noneMatch(p -> p.getName().equals("test")));
    }

    @Test
    public void testCreateProjectWithBreaker() {

        NProjectManager manager = Mockito.spy(NProjectManager.getInstance(getTestConfig()));
        val projects = Arrays.asList("test_ck__1", "test_ck_2", "test_ck_3");
        Mockito.doReturn(projects).when(manager).listAllProjects();

        getTestConfig().setProperty("kylin.circuit-breaker.threshold.project", "1");
        NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
        try {
            thrown.expect(KylinException.class);
            manager.createProject("test_ck_project", "admin", "", null, MaintainModelType.MANUAL_MAINTAIN);
        } finally {
            NCircuitBreaker.stop();
        }
    }
}
