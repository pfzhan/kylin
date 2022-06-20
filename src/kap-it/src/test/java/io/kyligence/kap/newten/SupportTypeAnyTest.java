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

import java.sql.SQLException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.util.ExecAndComp;

public class SupportTypeAnyTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        //TODO need to be rewritten
        //        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        //        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        //        if (!scheduler.hasStarted()) {
        //            throw new RuntimeException("scheduler has not been started");
        //        }
    }

    @After
    public void after() {
        //TODO need to be rewritten
        // NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void test() throws SQLException {
        String sql = "select replace(TEST_COUNT_DISTINCT_BITMAP, 'TEST', '') as HEADER from TEST_KYLIN_FACT where 1 = 0";
        Dataset<Row> dataset = ExecAndComp.queryModel(getProject(), sql);
        Assert.assertEquals("HEADER", dataset.schema().apply(0).name());
        Assert.assertEquals(0, dataset.collectAsList().size());
    }
}
