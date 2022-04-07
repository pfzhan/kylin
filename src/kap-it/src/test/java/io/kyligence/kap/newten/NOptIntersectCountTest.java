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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.kyligence.kap.util.ExecAndComp;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;

public class NOptIntersectCountTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        this.createTestMetadata("src/test/resources/ut_meta/opt_intersect_count");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "opt_intersect_count";
    }

    @Test
    public void testOptIntersectCountBuild() throws Exception {
        String dfName = "c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e";
        fullBuild(dfName);
        NDataSegment seg = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(dfName)
                .getLatestReadySegment();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), 100001);
        ParquetStorage storage = new ParquetStorage();
        List<Row> rows = storage.getFrom(NSparkCubingUtil.getStoragePath(seg, dataCuboid.getLayoutId()), ss)
                .collectAsList();
        Assert.assertEquals(9, rows.size());

        String ret = rows.stream().map(row -> {
            List<String> list = new ArrayList<>();

            for (int i = 0; i < 4; i++) {
                list.add(row.get(i).toString());
            }
            list.add(String.valueOf(getCountDistinctValue((byte[]) (row.get(4)))));
            return list;
        }).collect(Collectors.toList()).toString();

        Assert.assertEquals("[[18, Shenzhen, male, handsome, 1]," //
                + " [18, Shenzhen, male, rich, 1]," //
                + " [18, Shenzhen, male, tall, 1]," //
                + " [19, Beijing, female, handsome, 1]," //
                + " [19, Beijing, female, rich, 1]," //
                + " [19, Beijing, female, tall, 2]," //
                + " [20, Shanghai, male, handsome, 2]," //
                + " [20, Shanghai, male, rich, 2]," //
                + " [20, Shanghai, male, tall, 1]]", ret);
    }

    private int getCountDistinctValue(byte[] bytes) {
        Roaring64NavigableMap bitMap = new Roaring64NavigableMap();
        try {
            bitMap.deserialize(new KryoDataInput(new Input((bytes))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bitMap.getIntCardinality();
    }

    @Test
    public void testIntersectCountQuery() throws Exception {
        /*
        Source table: USER_ID, AGE, CITY, TAG
        TAG value split by "|"

        group by key: 20,Shanghai

        rich, 2
        tall, 1
        handsome, 2
        ====================================

        group by key: 19,Beijing

        rich, 1
        tall, 2
        handsome, 1
        ====================================

        group by key: 18,Shenzhen

        rich, 1
        tall, 1
        handsome, 1
        * */
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String query1 = "select AGE, CITY, " + "intersect_count(USER_ID, TAG, array['rich','tall','handsome']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,0", r1.get(0));
        Assert.assertEquals("19,Beijing,0", r1.get(1));
        Assert.assertEquals("20,Shanghai,1", r1.get(2));

        String query2 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['rich']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r2 = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r2.get(0));
        Assert.assertEquals("19,Beijing,1", r2.get(1));
        Assert.assertEquals("20,Shanghai,2", r2.get(2));

        String query3 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['tall']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r3 = ExecAndComp.queryModel(getProject(), query3).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r3.get(0));
        Assert.assertEquals("19,Beijing,2", r3.get(1));
        Assert.assertEquals("20,Shanghai,1", r3.get(2));

        String query4 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['handsome']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r4 = ExecAndComp.queryModel(getProject(), query4).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r4.get(0));
        Assert.assertEquals("19,Beijing,1", r4.get(1));
        Assert.assertEquals("20,Shanghai,2", r4.get(2));

        String query5 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['rich', 'tall']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r5 = ExecAndComp.queryModel(getProject(), query5).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,0", r5.get(0));
        Assert.assertEquals("19,Beijing,1", r5.get(1));
        Assert.assertEquals("20,Shanghai,1", r5.get(2));

        String query6 = "select CITY, intersect_count(USER_ID, TAG, array['rich', 'tall']) "
                + "from TEST_INTERSECT_COUNT group by CITY";
        List<String> r6 = ExecAndComp.queryModel(getProject(), query6).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("Beijing,1", r6.get(0));
        Assert.assertEquals("Shanghai,1", r6.get(1));
        Assert.assertEquals("Shenzhen,0", r6.get(2));
    }
}
