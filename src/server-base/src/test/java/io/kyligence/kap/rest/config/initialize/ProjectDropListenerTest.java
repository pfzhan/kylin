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

import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.staticCleanupTestMetadata;
import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.staticCreateTestMetadata;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;

public class ProjectDropListenerTest {

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testDeleteProjectStorage() throws IOException {
        val project = "drop_project";
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        String strPath = kylinConfig.getHdfsWorkingDirectory(project);
        FileSystem fs = HadoopUtil.getFileSystem(strPath);
        Path path = new Path(strPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        Path file = new Path(strPath + project + "_empty");
        fs.createNewFile(file);
        Assert.assertTrue(fs.exists(file));

        ProjectDropListener projectDropListener = new ProjectDropListener();

        projectDropListener.onDelete(project);

        Assert.assertFalse(fs.exists(path));
    }
}
