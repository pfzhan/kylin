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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class NPartitionColumnTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/partition_col");
        //TODO need to be rewritten
        //        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        //        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        //        if (!scheduler.hasStarted()) {
        //            throw new RuntimeException("scheduler has not been started");
        //        }
    }

    @After
    public void after() throws Exception {
        //TODO need to be rewritten
        // NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testVariousPartitionCol() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        List<String> dfs = new ArrayList<>();
        dfs.add("INT_PAR_COL");
        dfs.add("LONG_PAR_COL");
        for (int i = 1; i < 7; i++) {
            dfs.add("STR_PAR_COL" + i);
        }

        for (String df : dfs) {
            buildMultiSegs(df);
        }

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val base = "select count(*) from TEST_PAR_COL ";

        val sql = new ArrayList<String>();
        sql.add(base + "where INT_PAR_COL >= 20090101 and INT_PAR_COL < 20110101");
        sql.add(base + "where INT_PAR_COL >= 20110101 and INT_PAR_COL < 20130101");
        sql.add(base + "where INT_PAR_COL >= 20130101 and INT_PAR_COL < 20150101");

        sql.add(base + "where LONG_PAR_COL >= 20090101 and LONG_PAR_COL < 20110101");
        sql.add(base + "where LONG_PAR_COL >= 20110101 and LONG_PAR_COL < 20130101");
        sql.add(base + "where LONG_PAR_COL >= 20130101 and LONG_PAR_COL < 20150101");

        sql.add(base + "where STR_PAR_COL1 >= '20090101' and STR_PAR_COL1 < '20110101'");
        sql.add(base + "where STR_PAR_COL1 >= '20110101' and STR_PAR_COL1 < '20130101'");
        sql.add(base + "where STR_PAR_COL1 >= '20130101' and STR_PAR_COL1 < '20150101'");

        sql.add(base + "where STR_PAR_COL2 >= '2009-01-01' and STR_PAR_COL2 < '2011-01-01'");
        sql.add(base + "where STR_PAR_COL2 >= '2011-01-01' and STR_PAR_COL2 < '2013-01-01'");
        sql.add(base + "where STR_PAR_COL2 >= '2013-01-01' and STR_PAR_COL2 < '2015-01-01'");

        sql.add(base + "where STR_PAR_COL3 >= '2009/01/01' and STR_PAR_COL3 < '2011/01/01'");
        sql.add(base + "where STR_PAR_COL3 >= '2011/01/01' and STR_PAR_COL3 < '2013/01/01'");
        sql.add(base + "where STR_PAR_COL3 >= '2013/01/01' and STR_PAR_COL3 < '2015/01/01'");

        sql.add(base + "where STR_PAR_COL4 >= '2009.01.01' and STR_PAR_COL4 < '2011.01.01'");
        sql.add(base + "where STR_PAR_COL4 >= '2011.01.01' and STR_PAR_COL4 < '2013.01.01'");
        sql.add(base + "where STR_PAR_COL4 >= '2013.01.01' and STR_PAR_COL4 < '2015.01.01'");

        sql.add(base + "where STR_PAR_COL5 >= '2009-01-01 00:00:00' and STR_PAR_COL5 < '2011-01-01 00:00:00'");
        sql.add(base + "where STR_PAR_COL5 >= '2011-01-01 00:00:00' and STR_PAR_COL5 < '2013-01-01 00:00:00'");
        sql.add(base + "where STR_PAR_COL5 >= '2013-01-01 00:00:00' and STR_PAR_COL5 < '2015-01-01 00:00:00'");

        sql.add(base + "where STR_PAR_COL6 >= '2009-01-01 00:00:00.000' and STR_PAR_COL6 < '2011-01-01 00:00:00.000'");
        sql.add(base + "where STR_PAR_COL6 >= '2011-01-01 00:00:00.000' and STR_PAR_COL6 < '2013-01-01 00:00:00.000'");
        sql.add(base + "where STR_PAR_COL6 >= '2013-01-01 00:00:00.000' and STR_PAR_COL6 < '2015-01-01 00:00:00.000'");

        ExecAndComp.execAndCompareQueryList(sql, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Override
    public String getProject() {
        return "partition_col";
    }
}
