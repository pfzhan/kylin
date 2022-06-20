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

package io.kyligence.kap.engine.spark.job;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import scala.Option;
import scala.runtime.AbstractFunction1;

@SuppressWarnings("serial")
public class InferiorBuildJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Override
    public String getProject() {
        return "test_inferior_job";
    }

    @Before
    public void setup() {
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        //TODO need to be rewritten
        //        NDefaultScheduler.destroyInstance();
        //        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        //        scheduler.init(new JobEngineConfig(getTestConfig()));
        //        if (!scheduler.hasStarted()) {
        //            throw new RuntimeException("scheduler has not been started");
        //        }

        config = getTestConfig();
    }

    @After
    public void after() {
        //TODO need to be rewritten
        //        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testBuildFromInferiorTable() throws Exception {

        final String conventionId = "bb4e7e15-06f5-519d-c36f-1af5d05f7b60";

        // prepare segment
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, getProject());
        cleanupSegments(dfMgr, conventionId);

        NDataflow df = dfMgr.getDataflow(conventionId);
        List<LayoutEntity> layoutList = df.getIndexPlan().getAllLayouts();

        indexDataConstructor.buildIndex(conventionId, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layoutList), true);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, getProject());
        NDataflow dataflow = dataflowManager.getDataflow(conventionId);
        Assert.assertEquals(64, dataflow.getFirstSegment().getSegDetails().getLayouts().size());
    }

    private void cleanupSegments(NDataflowManager dsMgr, String dfName) {
        NDataflow df = dsMgr.getDataflow(dfName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    public static class MockParquetStorage extends ParquetStorage {

        @Override
        public Dataset<Row> getFrom(String path, SparkSession ss) {
            return super.getFrom(path, ss);
        }

        @Override
        public void saveTo(String path, Dataset<Row> data, SparkSession ss) {
            Option<LogicalPlan> option = data.queryExecution().optimizedPlan()
                    .find(new AbstractFunction1<LogicalPlan, Object>() {
                        @Override
                        public Object apply(LogicalPlan v1) {
                            return v1 instanceof Join;
                        }
                    });
            Assert.assertFalse(option.isDefined());
            super.saveTo(path, data, ss);
        }
    }

    public static class MockupStorageEngine implements IStorage {

        @Override
        public IStorageQuery createQuery(IRealization realization) {
            return null;
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            Class clz;
            try {
                clz = Class.forName("io.kyligence.kap.engine.spark.NSparkCubingEngine$NSparkCubingStorage");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (engineInterface == clz) {
                return (I) ClassUtil
                        .newInstance("io.kyligence.kap.engine.spark.job.NSparkCubingJobTest$MockParquetStorage");
            } else {
                throw new RuntimeException("Cannot adapt to " + engineInterface);
            }
        }
    }

}
