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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;

public class NBitmapContainsFunctionTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "table_index";
    }

    @Test
    public void testBitmapContains() throws Exception {
        fullBuildCube("acfde546-2cc9-4eec-bc92-e3bd46d4e2ee", getProject());

        List<String> result;

        // hit table index
        String query1 = "select ORDER_ID from test_kylin_fact "
                + "where BITMAP_CONTAINS(ORDER_ID, 'AAAAAAEAAAAAOjAAAAEAAAAAAAMAEAAAAAEAAwAEAAUA')";
        result = NExecAndComp.queryCube(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));
        Assert.assertEquals("3", result.get(1));
        Assert.assertEquals("3", result.get(2));
        Assert.assertEquals("4", result.get(3));
        Assert.assertEquals("4", result.get(4));
        Assert.assertEquals("5", result.get(5));

        // query pushdown
        String query2 = "select TRANS_ID from test_kylin_fact "
                + "where BITMAP_CONTAINS(TRANS_ID, 'AAAAAAEAAAAAOjAAAAEAAAAAAAMAEAAAAAEAAwAEAAUA')";
        result = NExecAndComp.querySparkSql(query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));
        Assert.assertEquals("3", result.get(1));
        Assert.assertEquals("4", result.get(2));
        Assert.assertEquals("5", result.get(3));
    }
}
