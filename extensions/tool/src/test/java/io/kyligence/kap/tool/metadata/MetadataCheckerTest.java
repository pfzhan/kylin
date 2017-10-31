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

package io.kyligence.kap.tool.metadata;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobManager;

public class MetadataCheckerTest extends LocalFileMetadataTestCase {

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        MetadataChecker checker = new MetadataChecker();
        checker.doCheck("check");
        List<String> checkRet1 = (List<String>) checker.getCheckResult().get(MetadataChecker.TABLEINDEX_CUBE_RULE);
        List<String> checkRet2 = (List<String>) checker.getCheckResult().get(MetadataChecker.SCHEDULERJOB_CUBE_RULE);
        List<String> checkRet3 = (List<String>) checker.getCheckResult().get(MetadataChecker.EXECUTABLE_OUT_RULE);
        List<String> checkRet4 = (List<String>) checker.getCheckResult().get(MetadataChecker.CUBE_MODEL_RULE);

        Assert.assertEquals(4, checker.getCheckResult().size());
        Assert.assertEquals(0, checkRet1.size());
        Assert.assertEquals(0, checkRet2.size());
        Assert.assertEquals(0, checkRet3.size());
        Assert.assertEquals(0, checkRet4.size());
    }

    @Test
    public void testMetadataCheck() throws IOException {
        ResourceStore store = getStore();
        SchedulerJobInstance job = new SchedulerJobInstance("ssb_cube1", "default", "Cube", "ssb_cube1", false, 0, 0, 0,
                0, 0, 0);
        String path = SchedulerJobInstance.concatResourcePath(job.getName());
        store.putResource(path, job, SchedulerJobManager.SCHEDULER_JOB_INSTANCE_SERIALIZER);
        path = CubeInstance.concatResourcePath("ssb_cube1");
        store.deleteResource(path);
        path = CubeInstance.concatResourcePath("ci_left_join_cube");
        store.deleteResource(path);
        path = DataModelDesc.concatResourcePath("ci_inner_join_model");
        store.deleteResource(path);
        store.deleteResource(ExecutableDao.pathOfJobOutput("f8edd777-8756-40d5-be19-3159120e4f7b"));

        MetadataChecker checker = new MetadataChecker();
        checker.doCheck("check");

        List<String> checkRet1 = (List<String>) checker.getCheckResult().get(MetadataChecker.TABLEINDEX_CUBE_RULE);
        List<String> checkRet2 = (List<String>) checker.getCheckResult().get(MetadataChecker.SCHEDULERJOB_CUBE_RULE);
        List<String> checkRet3 = (List<String>) checker.getCheckResult().get(MetadataChecker.EXECUTABLE_OUT_RULE);
        List<String> checkRet4 = (List<String>) checker.getCheckResult().get(MetadataChecker.CUBE_MODEL_RULE);

        Assert.assertEquals(4, checker.getCheckResult().size());
        Assert.assertEquals(2, checkRet1.size());
        Assert.assertEquals(1, checkRet2.size());
        Assert.assertEquals(15, checkRet3.size());
        Assert.assertEquals(5, checkRet4.size());

        checker.doOpts("recovery");

        checker.doCheck("check");
        checkRet1 = (List<String>) checker.getCheckResult().get(MetadataChecker.TABLEINDEX_CUBE_RULE);
        checkRet2 = (List<String>) checker.getCheckResult().get(MetadataChecker.SCHEDULERJOB_CUBE_RULE);
        checkRet3 = (List<String>) checker.getCheckResult().get(MetadataChecker.EXECUTABLE_OUT_RULE);
        checkRet4 = (List<String>) checker.getCheckResult().get(MetadataChecker.CUBE_MODEL_RULE);

        Assert.assertEquals(0, checkRet1.size());
        Assert.assertEquals(0, checkRet2.size());
        Assert.assertEquals(0, checkRet3.size());
        Assert.assertEquals(0, checkRet4.size());
    }

}
