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

import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.util.ExecAndComp;
import scala.collection.JavaConversions;

public class CalciteDynamicTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        super.init();
    }

    @After
    public void after() {
        //TODO need to be rewritten
        // NDefaultScheduler.destroyInstance();
    }

    @Test
    public void testCalciteGroupByDynamicParam() throws Exception {
        fullBuild("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), getProject(), SparderEnv.getSparkSession());
        String sqlOrigin = "SELECT (case when 1=1 then SELLER_ID else TRANS_ID end) as id,  SUM(price) as PRICE\n"
                + "FROM TEST_KYLIN_FACT\n" + "GROUP BY (case when 1=1 then SELLER_ID else TRANS_ID end) limit 5";
        String parameter = "1";
        // benchmark
        List<List<String>> benchmark = ExecAndComp.queryCubeWithJDBC(getProject(), sqlOrigin);
        // setTimestamp
        String sqlWithPlaceholder = sqlOrigin.replace("case when 1=1", "case when ?=1");
        List<Row> rows = ExecAndComp
                .queryModel(getProject(), sqlWithPlaceholder, Arrays.asList(new String[] { parameter, parameter }))
                .collectAsList();
        List<List<String>> results = transformToString(rows);
        for (int i = 0; i < benchmark.size(); i++) {
            if (!ListUtils.isEqualList(benchmark.get(i), results.get(i))) {
                String expected = Strings.join(benchmark.get(i), ",");
                String actual1 = Strings.join(results.get(i), ",");
                fail("expected: " + expected + ", results: " + actual1);
            }
        }
    }

    private List<List<String>> transformToString(List<Row> rows) {
        return rows.stream().map(row -> JavaConversions.seqAsJavaList(row.toSeq()).stream().map(r -> {
            if (r == null) {
                return null;
            } else {
                String s = r.toString();
                if (r instanceof Timestamp) {
                    return s.substring(0, s.length() - 2);
                } else {
                    return s;
                }
            }
        }).collect(Collectors.toList())).collect(Collectors.toList());
    }
}
