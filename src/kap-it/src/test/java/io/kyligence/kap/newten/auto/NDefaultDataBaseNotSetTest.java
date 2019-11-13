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
package io.kyligence.kap.newten.auto;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;

public class NDefaultDataBaseNotSetTest extends NAutoTestBase {

    @Before
    public void setupDefaultDatabase() {
        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        ProjectInstance projectInstance = npr.getProject("ssb");
        projectInstance.setDefaultDatabase("DEFAULT");
        npr.updateProject(projectInstance);
    }

    @After
    public void recoverDefaultDatabase() {
        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        ProjectInstance projectInstance = npr.getProject("ssb");
        projectInstance.setDefaultDatabase("SSB");
        npr.updateProject(projectInstance);
    }

    @Test
    public void testUdafAndUdf() {
        String[] sqls = new String[] {
                //test Udaf
                "SELECT LO_SUPPKEY, percentile_approx(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER GROUP BY LO_SUPPKEY",
                "SELECT LO_SUPPKEY, percentile(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER GROUP BY LO_SUPPKEY",
                "SELECT LO_SUPPKEY, percentile_approx(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER "
                        + "GROUP BY LO_SUPPKEY,LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_ORDERDATE,LO_ORDERPRIOTITY,LO_SHIPPRIOTITY,"
                        + "LO_QUANTITY,LO_EXTENDEDPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE,V_REVENUE",
                "SELECT LO_SUPPKEY, percentile(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER "
                        + "GROUP BY LO_SUPPKEY,LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_ORDERDATE,LO_ORDERPRIOTITY,LO_SHIPPRIOTITY,"
                        + "LO_QUANTITY,LO_EXTENDEDPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE,V_REVENUE",
                "SELECT\n" + "  \"LO_LINENUMBER\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101]) AS \"FIRST_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960102]) AS \"SECOND_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960103]) AS \"THIRD_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101, 19960102]) AS \"RETENTION_ONEDAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101, 19960102, 19960103]) AS \"RETENTION_TWODAY\"\n"
                        + "FROM ssb.\"P_LINEORDER\"\n" + "GROUP BY\n" + "  \"LO_LINENUMBER\"",
                //test Udf
                "select initcapb(LO_ORDERPRIOTITY),substr(LO_ORDERPRIOTITY,2),instr(LO_ORDERPRIOTITY,'A'),"
                        + "ifnull(LO_LINENUMBER,0),rlike(LO_ORDERPRIOTITY,'*') from SSB.P_LINEORDER" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, "ssb", sqls);
        smartMaster.runAll();

        val modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals("COUNT",
                modelContext.getTargetModel().getAllMeasures().get(0).getFunction().getExpression());
        Assert.assertEquals("PERCENTILE_APPROX",
                modelContext.getTargetModel().getAllMeasures().get(1).getFunction().getExpression());
    }
}
