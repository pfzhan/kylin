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
package io.kyligence.kap.engine.spark;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.spark_project.guava.collect.Sets;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

@SuppressWarnings("serial")
public class NLocalWithSparkSessionTest extends NLocalFileMetadataTestCase implements Serializable {

    private static final String CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv";

    protected static final String KAP_SQL_BASE_DIR = "../kap-it/src/test/resources/query";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    @AfterClass
    public static void afterClass() {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        ss.close();
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    public String getProject() {
        return "default";
    }

    protected void init() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    protected static void populateSSWithCSVData(KylinConfig kylinConfig, String project, SparkSession sparkSession) {

        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        Preconditions.checkArgument(projectInstance != null);
        for (String table : projectInstance.getTables()) {

            if ("DEFAULT.STREAMING_TABLE".equals(table)) {
                continue;
            }

            TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(table);
            ColumnDesc[] columns = tableDesc.getColumns();
            StructType schema = new StructType();
            for (ColumnDesc column : columns) {
                schema = schema.add(column.getName(), convertType(column.getType()), false);
            }
            Dataset<Row> ret = sparkSession.read().schema(schema).csv(String.format(CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }

    }

    private static DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isTimeFamily())
            return DataTypes.TimestampType;

        if (type.isDateTimeFamily())
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            return DataTypes.LongType;

        if (type.isNumberFamily())
            return DataTypes.createDecimalType(19, 4);

        if (type.isStringFamily())
            return DataTypes.StringType;

        if (type.isBoolean())
            return DataTypes.BooleanType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    protected List<Object[]> getCuboidDataAfterDecoding(NDataSegment seg, long cuboidLayoutId) {
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, cuboidLayoutId);
        List<Object[]> resultFromLayout = new ArrayList<>();
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getCuboidData(dataCuboid, ss);
        IDimensionEncodingMap dimEncoding = new NCubeDimEncMap(seg);

        for (TblColRef colRef : seg.getCubePlan().getEffectiveDimCols().values()) {
            dimEncoding.get(colRef);
        }

        NCuboidLayout layout = dataCuboid.getCuboidLayout();
        RowKeyColumnIO colIO = new RowKeyColumnIO(dimEncoding);
        MeasureCodec measureCodec = new MeasureCodec(layout.getOrderedMeasures().values().toArray(new MeasureDesc[0]));
        int ms = layout.getOrderedMeasures().size();

        for (Row row : ret.collectAsList()) {
            List<Object> l = new ArrayList<>();
            int i = 0;
            for (TblColRef rowkey : layout.getOrderedDimensions().values()) {
                byte[] bytes = (byte[]) row.get(i);
                String value = colIO.readColumnString(rowkey, bytes, 0, bytes.length);
                l.add(value);
                i++;
            }
            for (int j = 0; j < ms; j++, i++) {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) row.get(i));
                l.add(measureCodec.decode(j, buffer));
            }
            resultFromLayout.add(l.toArray());
        }
        return resultFromLayout;

    }

    protected ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            ExecutableState status = job.getStatus();
            if (!status.isReadyOrRunning()) {
                return status;
            }
        }
    }

    protected void fullBuildCube(String dfName, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = Lists.newArrayList(layouts);
        buildCuboid(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1),
                prj, true);
    }

    protected void buildCuboid(String cubeName, SegmentRange segmentRange, Set<NCuboidLayout> toBuildLayouts, boolean isAppend)
            throws Exception {
        buildCuboid(cubeName, segmentRange, toBuildLayouts, getProject(), isAppend);
    }

    protected void buildCuboid(String cubeName, SegmentRange segmentRange, Set<NCuboidLayout> toBuildLayouts,
            String prj, boolean isAppend) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, prj);
        NDataflow df = dsMgr.getDataflow(cubeName);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, segmentRange);
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN");
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        if (!Objects.equals(wait(job), ExecutableState.SUCCEED)) {
            throw new IllegalStateException();
        }

        val merger = new AfterBuildResourceMerger(config, getProject());
        if (isAppend) {
            merger.mergeAfterIncrement(df.getName(), oneSeg.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                    ExecutableUtils.getRemoteStore(config, sparkStep));
        } else {
            merger.mergeAfterCatchup(df.getName(), Sets.newHashSet(oneSeg.getId()),
                    ExecutableUtils.getLayoutIds(sparkStep), ExecutableUtils.getRemoteStore(config, sparkStep));
        }
        merger.mergeAnalysis(df.getName(), ExecutableUtils.getRemoteStore(config, job.getSparkAnalysisStep()));

        //Assert.assertEquals(ExecutableState.SUCCEED, wait(job));
    }
}
