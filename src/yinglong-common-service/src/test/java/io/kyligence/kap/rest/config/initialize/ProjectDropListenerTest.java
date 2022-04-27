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
package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.SpringContext;
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
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SpringContext.class, MetricsGroup.class, UserGroupInformation.class, MeterRegistry.class })
public class ProjectDropListenerTest extends NLocalFileMetadataTestCase {

    private MeterRegistry meterRegistry;

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        meterRegistry = new SimpleMeterRegistry();
        PowerMockito.mockStatic(MetricsGroup.class);
        PowerMockito.mockStatic(SpringContext.class);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testDeleteProjectStorage() throws IOException {
        val project = "drop_project";
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        String strPath = kylinConfig.getHdfsWorkingDirectory(project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path path = new Path(strPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        Path file = new Path(strPath + project + "_empty");
        fs.createNewFile(file);
        Assert.assertTrue(fs.exists(file));

        ProjectDropListener projectDropListener = new ProjectDropListener();
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        projectDropListener.onDelete(project);

        Assert.assertFalse(fs.exists(path));
    }
}
