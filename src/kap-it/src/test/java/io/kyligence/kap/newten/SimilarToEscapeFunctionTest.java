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

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.util.ExecAndComp;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SimilarToEscapeFunctionTest extends NLocalWithSparkSessionTest {
    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/file_pruning");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "file_pruning";
    }

    @Test
    public void testSimilarToEscapeFunction() throws Exception {
        fullBuild("9cde9d25-9334-4b92-b229-a00f49453757");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        List<String> result;

        String query1 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch.na'";
        result = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("3", result.get(0));

        String query2 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@_na' ESCAPE '@'";
        result = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        String query3 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch%%na' ESCAPE '%'";
        result = ExecAndComp.queryModel(getProject(), query3).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        result.remove(0);

        try {
            String query4 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@.na' ESCAPE '@@'";
            result = ExecAndComp.queryModel(getProject(), query4).collectAsList().stream()
                    .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        } catch (Exception e) {
            Assert.assertEquals("0", String.valueOf(result.size()));
        }

        String query5 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@%na' ESCAPE '@'";
        result = ExecAndComp.queryModel(getProject(), query5).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        String query6 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch%na' ESCAPE '~'";
        result = ExecAndComp.queryModel(getProject(), query6).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));
    }
}
