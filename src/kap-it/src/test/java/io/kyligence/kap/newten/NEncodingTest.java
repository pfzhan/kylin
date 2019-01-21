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

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.spark.KapSparkSession;

@Ignore("new build and query needn't decode")
public class NEncodingTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        init();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testQueryWithEncoding() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("test_encoding");
        NDataSegment toBeBuild = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(layouts.get(0));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(toBeBuild,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertEquals(null, layout);
        }

        // Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(toBeBuild), Sets.newLinkedHashSet(round1),
                "ADMIN");
        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        NDataflow ndf = dsMgr.getDataflow("test_encoding");
        NDataSegment seg = ndf.getLastSegment();
        List<Object[]> resultFromLayout = getCuboidDataAfterDecoding(seg, 1);
        List<String[]> dataAfterDecode = Lists.newArrayList();
        for (Object[] data : resultFromLayout) {
            String[] colData = new String[data.length];
            for (int i = 0; i < data.length; i++) {
                colData[i] = data[i] == null ? null : data[i].toString();
            }
            dataAfterDecode.add(colData);
        }

        String[] e1 = new String[] { "1", "1.1", "string_dict", "-62135769600000", "0", "T", "true", "1", "fix_length",
                "FF00FF", "-32767", "-32767", "00010101", "-62135769600000", "-62135769600000", "-62135769600000", "0",
                "1" };
        String[] e2 = new String[] { "2", "2.2", "string_dict", "253402214400000", "2147483646000", "F", "false", "0",
                "fix_length", "1A2BFF", "32767", "32767", "99991231", "253402214400000", "253402214400000",
                "253402214400000", "2147483647000", "1" };
        String[] e3 = new String[18];
        e3[17] = "1";
        Assert.assertArrayEquals(e1, dataAfterDecode.get(0));
        Assert.assertArrayEquals(e2, dataAfterDecode.get(1));
        Assert.assertArrayEquals(e3, dataAfterDecode.get(2));

        ss.close();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(getProject());

        populateSSWithCSVData(config, getProject(), kapSparkSession);

        List<Pair<String, String>> queries = new ArrayList<>();
        queries.add(Pair.newPair("int_dict", "select int_dict from test_encoding group by int_dict"));
        queries.add(Pair.newPair("double_dict", "select double_dict from test_encoding group by double_dict"));
        queries.add(Pair.newPair("str_dict", "select str_dict from test_encoding group by str_dict"));
        queries.add(Pair.newPair("date_dict", "select date_dict from test_encoding group by date_dict"));
        queries.add(Pair.newPair("time_dict", "select time_dict from test_encoding group by time_dict"));
        queries.add(Pair.newPair("str_bool", "select str_bool from test_encoding group by str_bool"));
        queries.add(Pair.newPair("standard_bool", "select standard_bool from test_encoding group by standard_bool"));
        queries.add(Pair.newPair("int_bool", "select int_bool from test_encoding group by int_bool"));
        queries.add(Pair.newPair("fix_len", "select substring(fix_len,1,10) from test_encoding group by fix_len"));
        queries.add(Pair.newPair("fix_len_hex", "select fix_len_hex from test_encoding group by fix_len_hex"));
        queries.add(Pair.newPair("standard_int", "select standard_int from test_encoding group by standard_int"));
        queries.add(Pair.newPair("str_int", "select str_int from test_encoding group by str_int"));
        queries.add(Pair.newPair("date_in_int", "select date_in_int from test_encoding group by date_in_int"));
        queries.add(Pair.newPair("standard_date", "select standard_date from test_encoding group by standard_date"));
        queries.add(Pair.newPair("date_in_time",
                "select cast(date_in_time as date) from test_encoding group by date_in_time"));
        queries.add(Pair.newPair("date_in_time_ms",
                "select cast(date_in_time_ms as date) from test_encoding group by date_in_time_ms"));
        queries.add(Pair.newPair("standard_time", "select standard_time from test_encoding group by standard_time"));
        NExecAndComp.execAndCompareOld(queries, kapSparkSession, CompareLevel.SAME, "left");
    }
}
