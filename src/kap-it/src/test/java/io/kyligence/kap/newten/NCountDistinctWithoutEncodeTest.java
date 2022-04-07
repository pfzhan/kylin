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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.kyligence.kap.util.ExecAndComp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;

public class NCountDistinctWithoutEncodeTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/count_distinct_no_encode");
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
        return "count_distinct_no_encode";
    }

    @Test
    public void testWithoutEncode() throws Exception {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        indexPlanManager.updateIndexPlan("b06eee9f-3e6d-41de-ac96-89dbf170b99b",
                copyForWrite -> copyForWrite.getOverrideProps().put("kylin.query.skip-encode-integer-enabled", "true"));
        fullBuild("b06eee9f-3e6d-41de-ac96-89dbf170b99b");
        List<String> results1 = ExecAndComp
                .queryModel(getProject(),
                        "select city, " + "count(distinct string_id), " + "count(distinct tinyint_id), "
                                + "count(distinct smallint_id), " + "count(distinct int_id), "
                                + "count(distinct bigint_id) from test_count_distinct group by city order by city")
                .collectAsList().stream().map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals(3, results1.size());
        Assert.assertEquals("上海,4,4,4,4,4", results1.get(0));
        Assert.assertEquals("北京,3,3,3,3,3", results1.get(1));
        Assert.assertEquals("广州,5,5,5,5,5", results1.get(2));

        List<String> results2 = ExecAndComp
                .queryModel(getProject(),
                        "select " + "count(distinct string_id), " + "count(distinct tinyint_id), "
                                + "count(distinct smallint_id), " + "count(distinct int_id), "
                                + "count(distinct bigint_id) from test_count_distinct")
                .collectAsList().stream().map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals(1, results2.size());
        Assert.assertEquals("5,5,5,5,5", results2.get(0));

        String dictPath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/" + getProject()
                + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/DEFAULT.TEST_COUNT_DISTINCT";
        FileStatus[] fileStatuses = new Path(dictPath).getFileSystem(new Configuration())
                .listStatus(new Path(dictPath));
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertEquals("STRING_ID", fileStatuses[0].getPath().getName());
    }

    @Test
    public void testWithEncode() throws Exception {
        fullBuild("b06eee9f-3e6d-41de-ac96-89dbf170b99b");
        String dictPath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/" + getProject()
                + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/DEFAULT.TEST_COUNT_DISTINCT";
        FileStatus[] fileStatuses = new Path(dictPath).getFileSystem(new Configuration())
                .listStatus(new Path(dictPath));
        Assert.assertEquals(5, fileStatuses.length);

        String[] expected = { "BIGINT_ID", "INT_ID", "SMALLINT_ID", "STRING_ID", "TINYINT_ID" };
        Assert.assertArrayEquals(expected,
                Arrays.stream(fileStatuses).map(fileStatus -> fileStatus.getPath().getName()).sorted().toArray());
    }

}
