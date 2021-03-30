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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
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
import org.sparkproject.guava.collect.Sets;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.job.JobBucket;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NLocalWithSparkSessionTest extends NLocalFileMetadataTestCase implements Serializable {

    private static final String CSV_TABLE_DIR = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv";

    protected static final String KAP_SQL_BASE_DIR = "../kap-it/src/test/resources/query";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;
    private TestingServer zkTestServer;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        sparkConf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        sparkConf.set("spark.sql.parquet.mergeSchema", "true");
        sparkConf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

    }

    @AfterClass
    public static void afterClass() {
        ss.close();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("calcite.keep-in-clause", "true");
        this.createTestMetadata();
        ExecutableUtils.initJobFactory();
        Random r = new Random(10000);
        zkTestServer = new TestingServer(r.nextInt(), true);
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    public String getProject() {
        return "default";
    }

    protected void init() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("calcite.keep-in-clause", "true");
        this.createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    public static void populateSSWithCSVData(KylinConfig kylinConfig, String project, SparkSession sparkSession) {

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
            Dataset<Row> ret = sparkSession.read().schema(schema).csv(String.format(Locale.ROOT, CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }

    }

    private static DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isTimeFamily())
            return DataTypes.TimestampType;

        if (type.isDateTimeFamily())
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            switch (type.getName()) {
            case "tinyint":
                return DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "integer":
            case "int4":
                return DataTypes.IntegerType;
            default:
                return DataTypes.LongType;
            }

        if (type.isNumberFamily())
            switch (type.getName()) {
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            default:
                if (type.getPrecision() == -1 || type.getScale() == -1) {
                    return DataTypes.createDecimalType(19, 4);
                } else {
                    return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
                }
            }

        if (type.isStringFamily())
            return DataTypes.StringType;

        if (type.isBoolean())
            return DataTypes.BooleanType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    protected static ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }

    protected void fullBuildCube(String dfName, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        buildCuboid(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1),
                prj, true);
    }

    protected void buildCuboid(String cubeName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts,
            boolean isAppend) throws Exception {
        buildCuboid(cubeName, segmentRange, toBuildLayouts, getProject(), isAppend);
    }

    public static void buildCuboid(String cubeName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts, String prj,
            boolean isAppend) throws Exception {
        buildCuboid(cubeName, segmentRange, toBuildLayouts, prj, isAppend, null);
    }

    protected static void buildCuboid(String cubeName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts, String prj,
            boolean isAppend, List<String[]> partitionValues) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        NDataflow df = dsMgr.getDataflow(cubeName);
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, segmentRange, SegmentStatusEnum.NEW, partitionValues);
        buildSegment(cubeName, oneSeg, toBuildLayouts, prj, isAppend, partitionValues);
    }

    protected static void buildSegment(String cubeName, NDataSegment segment, Set<LayoutEntity> toBuildLayouts, String prj,
            boolean isAppend, List<String[]> partitionValues) throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, prj);
        NDataflow df = dsMgr.getDataflow(cubeName);
        Set<JobBucket> buckets = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(partitionValues)) {
            NDataModelManager modelManager = NDataModelManager.getInstance(config, prj);
            Set<Long> targetPartitions = modelManager.getDataModelDesc(cubeName).getMultiPartitionDesc()
                    .getPartitionIdsByValues(partitionValues);
            val bucketStart = new AtomicLong(segment.getMaxBucketId());
            toBuildLayouts.forEach(layout -> {
                targetPartitions.forEach(partition -> {
                    buckets.add(
                            new JobBucket(segment.getId(), layout.getId(), bucketStart.incrementAndGet(), partition));
                });
            });
            dsMgr.updateDataflow(df.getId(),
                    copyForWrite -> copyForWrite.getSegment(segment.getId()).setMaxBucketId(bucketStart.get()));
        }
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), toBuildLayouts, "ADMIN", buckets);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        if (!Objects.equals(wait(job), ExecutableState.SUCCEED)) {
            throw new IllegalStateException();
        }

        val merger = new AfterBuildResourceMerger(config, prj);
        if (isAppend) {
            merger.mergeAfterIncrement(df.getUuid(), segment.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                    ExecutableUtils.getRemoteStore(config, sparkStep));
        } else {
            merger.mergeAfterCatchup(df.getUuid(), Sets.newHashSet(segment.getId()),
                    ExecutableUtils.getLayoutIds(sparkStep), ExecutableUtils.getRemoteStore(config, sparkStep), null);
        }
    }

    public void buildMultiSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2011-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);

        start = SegmentRange.dateToLong("2011-01-01 00:00:00");
        end = SegmentRange.dateToLong("2013-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);

        start = SegmentRange.dateToLong("2013-01-01 00:00:00");
        end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
    }

    public void buildMultiSegmentPartitions(String dfName, String segStart, String segEnd, List<Long> layouts,
            List<Long> partitionIds) throws Exception {
        val config = getTestConfig();
        val project = getProject();
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfName);
        val partitionValues = df.getModel().getMultiPartitionDesc().getPartitionValuesById(partitionIds);

        // append segment
        long start = SegmentRange.dateToLong(segStart);
        long end = SegmentRange.dateToLong(segEnd);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment = dfManager.appendSegment(df, segmentRange, SegmentStatusEnum.NEW, partitionValues);

        // build segment with partitions
        val jobParam = new JobParam(Sets.newHashSet(dataSegment.getId()), Sets.newHashSet(layouts), dfName, "ADMIN",
                Sets.newHashSet(partitionIds), null);
        jobParam.setJobTypeEnum(JobTypeEnum.INC_BUILD);
        val jobManager = JobManager.getInstance(config, project);

        val jobId = jobManager.addJob(jobParam);

        val job = NExecutableManager.getInstance(config, project).getJob(jobId);
        if (!Objects.equals(wait(job), ExecutableState.SUCCEED)) {
            throw new IllegalStateException();
        }

        Assert.assertTrue(job instanceof NSparkCubingJob);
        val sparkJob = (NSparkCubingJob) job;
        NSparkCubingStep sparkStep = sparkJob.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(getTestConfig().getHdfsWorkingDirectory()));

        val merger = new AfterBuildResourceMerger(config, getProject());
        merger.mergeAfterIncrement(df.getUuid(), dataSegment.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                ExecutableUtils.getRemoteStore(config, sparkStep));
    }
}
