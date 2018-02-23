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

package io.kyligence.kap.newten;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.query.routing.Candidate;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.smart.NSmartMaster;

@Ignore("Still on progress")
public class NITQueryTest extends NLocalSparkWithCSVDataTest {
    private KylinConfig kylinConfig;
    private static final String project = "default";
    private static final String itSqlBaseDir = "../../kylin/kylin-it/src/test/resources/query";

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        super.setUp();
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        kylinConfig = getTestConfig();
    }

    @After
    public void after() throws Exception {
        Candidate.restorePriorities();

        DefaultScheduler.destroyInstance();
        super.tearDown();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void runITQueries() throws IOException {
        String[] sqls = retrieveITSqlFiles(itSqlBaseDir);
        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqls);
        master.runAll();
    }

    private String[] retrieveITSqlFiles(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.startsWith("sql_")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }
        List<String> allSqls = new ArrayList<>();
        for (File file : sqlFiles) {
            allSqls.addAll(Arrays.asList(retrieveITSqls(file)));
        }
        return allSqls.toArray(new String[0]);
    }

    private String[] retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null) {
            if (file.exists() && file.listFiles() != null) {
                sqlFiles = file.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".sql")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }
        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            //sqls[i] = KylinTestBase.getTextFromFile(sqlFiles[i]);
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }
        return sqls;
    }
}
