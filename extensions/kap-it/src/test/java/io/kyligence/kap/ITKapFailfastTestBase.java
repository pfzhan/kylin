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
package io.kyligence.kap;

import java.io.File;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.raw.RawTableInstance;

@Ignore("Contained by ITKapFailfastTest")
public class ITKapFailfastTestBase extends KapTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ITKapFailfastTestBase.class);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected static void configure(boolean testRaw) {
        if (testRaw) {
            logger.info("configure in ITKapFailfastTestBase");

            Map<String, Integer> priorities = Maps.newHashMap();
            priorities.put(HybridInstance.REALIZATION_TYPE, 1);
            priorities.put(CubeInstance.REALIZATION_TYPE, 1);
            priorities.put(RawTableInstance.REALIZATION_TYPE, 0);
            Candidate.setPriorities(priorities);

        } else {
            logger.info("setUp in ITKapKylinQueryTest");

            Map<String, Integer> priorities = Maps.newHashMap();
            priorities.put(HybridInstance.REALIZATION_TYPE, 0);
            priorities.put(CubeInstance.REALIZATION_TYPE, 0);
            priorities.put(RawTableInstance.REALIZATION_TYPE, 0);
            Candidate.setPriorities(priorities);
        }

        joinType = "left";

        logger.info("Into combination testRaw=" + testRaw);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITKapKylinQueryTest");

        Map<String, Integer> priorities = Maps.newHashMap();
        priorities.put(HybridInstance.REALIZATION_TYPE, 0);
        priorities.put(CubeInstance.REALIZATION_TYPE, 0);
        priorities.put(RawTableInstance.REALIZATION_TYPE, 0);
        Candidate.setPriorities(priorities);

        joinType = "left";

        setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in ITKapKylinQueryTest");
        Candidate.restorePriorities();
        clean();
    }

    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    @After
    public void cleanUp() {
        QueryContext.reset();
    }

    @Test
    public void testPartitionExceedMaxScanBytes() throws Exception {
        String key = "kylin.storage.partition.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getPartitionMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "1000");//very low threshold 

        boolean meetExpectedException = false;
        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            try {
                runSQL(sqlFile, false, false);
            } catch (Exception e) {

                String x = Throwables.getStackTraceAsString(e);
                if (x.contains("ResourceLimitExceededException")) {
                    //expected
                    meetExpectedException = true;
                } else {
                    throw new RuntimeException(e);
                }
            }

            if (!meetExpectedException) {
                throw new RuntimeException("Did not meet expected exception");
            }
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testPartitionNotExceedMaxScanBytes() throws Exception {
        String key = "kylin.storage.partition.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getPartitionMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "1000000");//enough threshold

        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            runSQL(sqlFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testQueryExceedMaxScanBytes() throws Exception {
        String key = "kylin.query.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getQueryMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "1000");//very low threshold 

        boolean meetExpectedException = false;
        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            try {
                runSQL(sqlFile, false, false);
            } catch (Exception e) {
                if (findRoot(e) instanceof ResourceLimitExceededException) {
                    //expected
                    meetExpectedException = true;
                } else {
                    throw new RuntimeException(e);
                }
            }

            if (!meetExpectedException) {
                throw new RuntimeException("Did not meet expected exception");
            }
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testQueryNotExceedMaxScanBytes() throws Exception {
        String key = "kylin.query.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getQueryMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "5000000");//enough threshold

        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            runSQL(sqlFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

}
