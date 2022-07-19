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

package io.kyligence.kap.tool.upgrade;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.tool.util.JobMetadataWriter;
import lombok.val;

public class MigrateJobToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
        JobMetadataWriter.writeJobMetaData(getTestConfig());
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Test
    public void test() {
        // execute before
        ExecutableManager executableManager = ExecutableManager.getInstance(getTestConfig(), "version40");

        List<AbstractExecutable> executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        Assert.assertEquals(1, executeJobs.size());

        val tool = new MigrateJobTool();

        tool.execute(new String[] { "-dir", getTestConfig().getMetadataUrl().toString() });

        getTestConfig().clearManagersByProject("version40");
        getTestConfig().clearManagers();
        ResourceStore.clearCache();

        JobMetadataWriter.writeJobMetaData(getTestConfig());
        executableManager = ExecutableManager.getInstance(getTestConfig(), "version40");

        executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.SUB_PARTITION_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        Assert.assertEquals(1, executeJobs.size());

        for (AbstractExecutable executeJob : executeJobs) {
            DefaultChainedExecutableOnModel job = (DefaultChainedExecutableOnModel) executeJob;
            Assert.assertEquals(3, job.getTasks().size());

            switch (job.getJobType()) {
            case INDEX_BUILD:
                Assert.assertEquals("io.kyligence.kap.job.execution.handler.ExecutableAddCuboidHandler",
                        job.getHandler().getClass().getName());
                break;
            case INC_BUILD:
                Assert.assertEquals("io.kyligence.kap.job.execution.handler.ExecutableAddSegmentHandler",
                        job.getHandler().getClass().getName());
                break;
            case INDEX_REFRESH:
            case SUB_PARTITION_REFRESH:
            case INDEX_MERGE:
                Assert.assertEquals("io.kyligence.kap.job.execution.handler.ExecutableMergeOrRefreshHandler",
                        job.getHandler().getClass().getName());
                break;
            default:
                break;
            }
        }

    }
}