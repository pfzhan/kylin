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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT_DISTINCT;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MAX;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MIN;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_PERCENTILE;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_SUM;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_TOP_N;

import java.util.List;
import java.util.stream.Collectors;

import io.kyligence.kap.cube.model.IndexPlan;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.cube.model.NIndexPlanManager;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.cube.model.IndexEntity;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.util.CubeUtils;
import io.kyligence.kap.spark.KapSparkSession;
import lombok.val;

public class NMeasuresTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    // Need to fill in support for top-n and percentile #8848
    public void testMeasures() throws Exception {

        //validate Cube Data by decode
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        buildCuboid();
        //build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        // Validate results between sparksql and cube
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use("default");
        populateSSWithCSVData(config, "default", kapSparkSession);
        SchemaProcessor.checkSchema(kapSparkSession, "cb596712-3a09-46f8-aea1-988b43fe9b6c", getProject());
        String querySql = fetchQuerySql();
        Assert.assertEquals(
                "select ID1,COUNT(*),SUM(1),SUM(1.0),SUM(1.0),SUM(ID1),SUM(ID2),SUM(ID3),SUM(ID4),SUM(PRICE1),SUM(PRICE2),"
                        + "SUM(PRICE3),SUM(PRICE5),SUM(PRICE6),SUM(PRICE7),SUM(NAME4),MAX(ID1),MAX(ID2),MAX(ID3),"
                        + "MAX(ID4),MAX(PRICE1),MAX(PRICE2),MAX(PRICE3),MAX(PRICE5),MAX(PRICE6),MAX(PRICE7),MAX(NAME1),"
                        + "MAX(NAME2),MAX(NAME3),MAX(NAME4),MAX(TIME1),MAX(TIME2),MAX(FLAG),MIN(ID1),MIN(ID2),MIN(ID3),"
                        + "MIN(ID4),MIN(PRICE1),MIN(PRICE2),MIN(PRICE3),MIN(PRICE5),MIN(PRICE6),MIN(PRICE7),MIN(NAME1),"
                        + "MIN(NAME2),MIN(NAME3),MIN(NAME4),MIN(TIME1),MIN(TIME2),MIN(FLAG),COUNT(ID1),COUNT(ID2),"
                        + "COUNT(ID3),COUNT(ID4),COUNT(PRICE1),COUNT(PRICE2),COUNT(PRICE3),COUNT(PRICE5),COUNT(PRICE6),"
                        + "COUNT(PRICE7),COUNT(NAME1),COUNT(NAME2),COUNT(NAME3),COUNT(NAME4),COUNT(TIME1),COUNT(TIME2),"
                        + "COUNT(FLAG),count(distinct ID1),count(distinct ID2),count(distinct ID3),count(distinct ID4),"
                        + "count(distinct PRICE1),count(distinct PRICE2),count(distinct PRICE3),count(distinct PRICE5),"
                        + "count(distinct PRICE6),count(distinct PRICE7),count(distinct NAME1),count(distinct NAME2),"
                        + "count(distinct NAME3),count(distinct NAME4),count(distinct TIME1),count(distinct TIME2),"
                        + "count(distinct FLAG),count(distinct ID1,ID2,ID3,ID4,PRICE1,PRICE2,PRICE3,PRICE5,PRICE6,"
                        + "PRICE7,NAME1,NAME2,NAME3,NAME4,TIME1,TIME2,FLAG), 1 from TEST_MEASURE  group by ID1",
                querySql);
        Pair<String, String> pair = new Pair<>("sql", querySql);

        NExecAndComp.execAndCompare(newArrayList(pair), kapSparkSession, NExecAndComp.CompareLevel.SAME, "left");
    }

    private String fetchQuerySql() {
        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        IndexPlan indexPlan = indePlanManager.getIndexPlan("cb596712-3a09-46f8-aea1-988b43fe9b6c");
        StringBuilder sqlBuilder;
        sqlBuilder = new StringBuilder("select ");
        for (TblColRef col : indexPlan.getEffectiveDimCols().values())
            sqlBuilder.append(col.getColumnDesc().getName()).append(",");
        for (NDataModel.Measure mea : indexPlan.getEffectiveMeasures().values()) {
            if (mea.getFunction().getParameter().isColumnType()) {
                if (mea.getFunction().getExpression().equalsIgnoreCase(FUNC_COUNT_DISTINCT)) {
                    String countDistinctString = "distinct";
                    for (TblColRef col : mea.getFunction().getParameter().getColRefs()) {
                        countDistinctString = countDistinctString + "," + col.getName();
                    }
                    countDistinctString = countDistinctString.replaceFirst(",", " ");
                    sqlBuilder.append(String.format("%s(%s)", "count", countDistinctString)).append(",");
                } else {
                    sqlBuilder.append(String.format("%s(%s)", mea.getFunction().getExpression(),
                            mea.getFunction().getParameter().getColRef().getName())).append(",");
                }
            } else {
                sqlBuilder.append(String.format("%s(%s)", mea.getFunction().getExpression(),
                        mea.getFunction().getParameter().getValue())).append(",");
            }

        }
        sqlBuilder.append(" 1 from TEST_MEASURE ");

        sqlBuilder.append(" group by ");
        for (TblColRef col : indexPlan.getEffectiveDimCols().values()) {
            sqlBuilder.append(col.getColumnDesc().getName()).append(",");
        }
        return StringUtils.removeEnd(sqlBuilder.toString(), ",");
    }

    private List<NDataModel.Measure> generateMeasList(NDataModel model) {
        List<String> funList = newArrayList(FUNC_SUM, FUNC_MAX, FUNC_MIN, FUNC_COUNT, FUNC_COUNT_DISTINCT,
                FUNC_PERCENTILE, FUNC_TOP_N);
        funList.remove(FUNC_PERCENTILE); //TODO #8848
        funList.remove(FUNC_TOP_N); //TODO #8848
        List<String> cdReturnTypeList = newArrayList("hllc(10)", "bitmap");
        List<TblColRef> columnList = model.getEffectiveColsMap().values().asList();

        int meaStart = 110000;
        List<NDataModel.Measure> measureList = model.getAllMeasures();
        for (String fun : funList) {
            for (TblColRef col : columnList) {
                // cannot support sum(date) sum(string) sum(boolean)
                if (fun.equalsIgnoreCase(FUNC_SUM) && (col.getType().isDateTimeFamily()
                        || col.getType().isStringFamily() || col.getType().isBoolean())) {
                    continue;
                }

                String returnType = col.getDatatype();
                if (fun.equalsIgnoreCase(FUNC_COUNT_DISTINCT)) {
                    returnType = cdReturnTypeList.get(RandomUtils.nextInt(2));
                }
                NDataModel.Measure measure = CubeUtils.newMeasure(
                        FunctionDesc.newInstance(fun, ParameterDesc.newInstance(col), returnType), meaStart + "_" + fun,
                        meaStart++);
                measureList.add(measure);
            }
        }

        NDataModel.Measure measure = CubeUtils.newMeasure(FunctionDesc.newInstance(FUNC_COUNT_DISTINCT,
                ParameterDesc.newInstance(columnList.toArray()), "hllc(10)"), meaStart + "_" + FUNC_COUNT_DISTINCT,
                meaStart++);
        measureList.add(measure);

        return measureList;
    }

    private void buildCuboid() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        NDataModelManager modelMgr = NDataModelManager.getInstance(config, getProject());
        NDataModel model = modelMgr.getDataModelDesc("cb596712-3a09-46f8-aea1-988b43fe9b6c");
        model.setAllMeasures(generateMeasList(model));

        NDataModel modelUpdate = modelMgr.copyForWrite(model);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelMgr.updateDataModelDesc(modelUpdate);
        modelMgr = NDataModelManager.getInstance(config, getProject());
        model = modelMgr.getDataModelDesc("cb596712-3a09-46f8-aea1-988b43fe9b6c");

        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(config, getProject());
        NDataModel finalModel = model;
        indePlanManager.updateIndexPlan("cb596712-3a09-46f8-aea1-988b43fe9b6c", copyForWrite -> {
            IndexEntity indexEntity = copyForWrite.getAllIndexes().get(0);
            indexEntity.setMeasures(finalModel.getEffectiveMeasureMap().inverse().values().asList());
            LayoutEntity layout = indexEntity.getLayouts().get(0);
            List<Integer> colList = newArrayList(indexEntity.getDimensions());
            colList.addAll(indexEntity.getMeasures());
            layout.setColOrder(colList);
            copyForWrite.setIndexes(newArrayList(indexEntity));
        });

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("cb596712-3a09-46f8-aea1-988b43fe9b6c");

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> toBuildLayouts = newArrayList(df.getIndexPlan().getAllLayouts().get(0));

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(toBuildLayouts),
                "ADMIN");
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        Assert.assertEquals(ExecutableState.SUCCEED, wait(job));

        val analysisStore = ExecutableUtils.getRemoteStore(config, job.getSparkAnalysisStep());
        val buildStore = ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep());
        AfterBuildResourceMerger merger = new AfterBuildResourceMerger(config, getProject());
        val layoutIds = toBuildLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);
        merger.mergeAnalysis(df.getUuid(), analysisStore);
    }
}
