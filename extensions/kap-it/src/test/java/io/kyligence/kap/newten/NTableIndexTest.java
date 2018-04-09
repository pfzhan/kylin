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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.spark.KapSparkSession;

public class NTableIndexTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    @Ignore("keep for debug.")
    public void testQuery() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        ss.close();
        KapSparkSession ksc = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        ksc.use("default");
        populateSSWithCSVData(config, DEFAULT_PROJECT, ksc);

        List<Pair<String, String>> queries = new ArrayList<>();
        String[] joinTypes = new String[] { "left", "inner" };

        for (String joinType : joinTypes) {
            //ITKapKylinQueryTest.testCommonQuery
            queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_raw");
            NExecAndComp.execAndCompare(queries, ksc, NExecAndComp.CompareLevel.SAME, joinType);

            //ITKapKylinQueryTest.testCommonQuery
            queries = NExecAndComp.fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_rawtable");
            NExecAndComp.execAndCompare(queries, ksc, NExecAndComp.CompareLevel.SAME, joinType);
        }
    }
}
