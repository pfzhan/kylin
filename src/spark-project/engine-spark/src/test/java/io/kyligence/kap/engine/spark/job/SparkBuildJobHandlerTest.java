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

package io.kyligence.kap.engine.spark.job;

import static io.kyligence.kap.engine.spark.job.NSparkExecutable.SPARK_MASTER;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;

public class SparkBuildJobHandlerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testKillOrphanApplicationIfExists() {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        Map<String, String> sparkConf = Maps.newHashMap();
        String jobStepId = "testId";
        handler.killOrphanApplicationIfExists(jobStepId, config, sparkConf);
        sparkConf.put(SPARK_MASTER, "mock");
        config.setProperty("kylin.engine.cluster-manager-timeout-threshold", "3s");
        handler.killOrphanApplicationIfExists(jobStepId, config, sparkConf);
    }

    @Test
    public void testCheckApplicationJar() {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        try {
            handler.checkApplicationJar(config);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof IllegalStateException);
        }
        String key = "kylin.engine.spark.job-jar";
        config.setProperty(key, "hdfs://127.0.0.1:0/mock");
        try {
            handler.checkApplicationJar(config);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof ExecuteException);
        }
    }

    @Test
    public void testExecuteCmd() throws ExecuteException {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        String cmd = "";
        Map<String, String> updateInfo = handler.runSparkSubmit(cmd, "");
        Assert.assertEquals(cmd, updateInfo.get("output"));
        Assert.assertNotNull(updateInfo.get("process_id"));

    }
}
