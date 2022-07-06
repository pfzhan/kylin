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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.util.ExecAndComp;

public class CharNColumnTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        this.createTestMetadata("src/test/resources/ut_meta/test_char_n_column");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "char_n_column";
    }

    @Test
    public void testCharNColumn() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String query1 = "select AGE, CITY, " + "intersect_count(USER_ID, TAG, array['rich','tall','handsome']) "
                + "from TEST_CHAR_N where city=\'Beijing  \' group by AGE, CITY ";
        List<String> r1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("19,Beijing   ,0", r1.get(0));

    }
}
