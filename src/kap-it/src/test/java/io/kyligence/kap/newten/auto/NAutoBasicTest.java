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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.cube.model.IndexPlan;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkContext;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.spark.KapSparkSession;

public class NAutoBasicTest extends NAutoTestBase {

    @Test
    public void testAutoSingleModel() throws Exception {

        // 1. Create simple model with one fact table
        String targetModelId;
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 0, 1);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            targetModelId = dataModel.getUuid();
            Assert.assertEquals(1, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 2. Feed query with left join using same fact table, should update same model
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 1, 2);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(targetModelId, dataModel.getUuid());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        //FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));

        // 3. Auto suggested model is able to serve related query
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 0, 3);
            kapSparkSession.use(getProject());
            populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, "default");
        }

        // 4. Feed bad queries
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql_bad", 0, 0);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(0, modelContexts.size());
        }

        // 5. Feed query with inner join using same fact table, should create another model
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 3, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertNotEquals(targetModelId, dataModel.getUuid());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 6. Finally, run all queries
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 0, 4);
            kapSparkSession.use(getProject());
            populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, "default");
        }

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testAutoMultipleModel() throws Exception {

        Map<String, IndexPlan> indexPlanOfParts = new HashMap<>();
        Map<String, IndexPlan> indexPlanOfAll = new HashMap<>();

        // 1. Feed queries part1
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 0, 2);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 2. Feed queries part2
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 2, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 3. Retry all queries
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            List<Pair<String, String>> queries = fetchQueries("auto/sql", 0, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfAll.put(indexPlan.getId(), indexPlan);
            }
        }

        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
        {
            Assert.assertEquals(indexPlanOfParts.size(), indexPlanOfAll.size());
            for (IndexPlan actual : indexPlanOfAll.values()) {
                IndexPlan expected = indexPlanOfParts.get(actual.getId());
                Assert.assertNotNull(expected);
                // compare cuboids
                Assert.assertEquals(expected.getAllIndexes().size(), actual.getAllIndexes().size());
                Assert.assertEquals(expected.getAllLayouts().size(), actual.getAllLayouts().size());
                for (IndexEntity actualCuboid : actual.getAllIndexes()) {
                    IndexEntity expectedCuboid = expected.getIndexEntity(actualCuboid.getId());
                    Assert.assertThat(expectedCuboid.getDimensions(), CoreMatchers.is(actualCuboid.getDimensions()));
                    Assert.assertThat(expectedCuboid.getMeasures(), CoreMatchers.is(actualCuboid.getMeasures()));
                }
            }
        }

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    private NSmartMaster proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        NSmartMaster master = new NSmartMaster(kylinConfig, getProject(), sqls);
        master.runAll();
        return master;
    }

}
