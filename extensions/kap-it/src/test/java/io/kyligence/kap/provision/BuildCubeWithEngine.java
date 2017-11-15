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

package io.kyligence.kap.provision;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.KAPDeployUtil;
import io.kyligence.kap.source.hive.modelstats.CollectModelStatsJob;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

public class BuildCubeWithEngine extends org.apache.kylin.provision.BuildCubeWithEngine {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithEngine.class);

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();
            File spark_home = new File(System.getenv("SPARK_HOME") + "/jars");

            ClassUtil.addClasspath(spark_home.getAbsolutePath());
            File[] files = spark_home.listFiles();
            for (File file : files) {
                URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                Class<URLClassLoader> urlClass = URLClassLoader.class;
                Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
                method.setAccessible(true);
                method.invoke(urlClassLoader, new Object[] { file.toURI().toURL() });
            }
            BuildCubeWithEngine buildCubeWithEngine = new BuildCubeWithEngine();
            buildCubeWithEngine.before();
            buildCubeWithEngine.build();
            buildCubeWithEngine.after();
            logger.info("Build is done");
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Exception e) {
            logger.error("error", e);
            System.exit(1);
        }
    }

    @Override
    protected boolean testTableExt() throws Exception {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        logger.info("Start testing tablestats: {}", tableName);
        DefaultChainedExecutable job = new HiveTableExtSampleJob("default", tableName, 1).build();
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    @Override
    protected boolean testModel() throws Exception {
        String modelName = "ci_inner_join_model";
        logger.info("Start testing model stats: {}", modelName);
        DefaultChainedExecutable job = new CollectModelStatsJob("default", modelName, "TEST",
                new TSRange(0L, Long.MAX_VALUE), 1, 7, false).build();
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    @Override
    protected void deployEnv() throws IOException {
        KAPDeployUtil.initCliWorkDir();
        KAPDeployUtil.deployMetadata();
        KAPDeployUtil.overrideJobJarLocations();
    }
}