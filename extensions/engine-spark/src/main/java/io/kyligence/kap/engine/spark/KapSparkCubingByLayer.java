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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.steps.ReducerNumSizing;
import org.apache.kylin.engine.spark.SparkCubingByLayer;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.steps.PartitionPreparer;

public class KapSparkCubingByLayer extends SparkCubingByLayer {
    protected static final Logger logger = LoggerFactory.getLogger(KapSparkCubingByLayer.class);

    public KapSparkCubingByLayer() {
        super();
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        Class[] kryoClassArray = new Class[] { org.apache.hadoop.io.Text.class,
                Class.forName("scala.reflect.ClassTag$$anon$1"), java.lang.Class.class };

        SparkConf conf = new SparkConf().setAppName("Cubing for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        JavaSparkContext sc = new JavaSparkContext(conf);
        HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

        KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        Configuration confOverwrite = new Configuration(sc.hadoopConfiguration());
        confOverwrite.set("dfs.replication", "2"); // cuboid intermediate files, replication=2
        final Job job = Job.getInstance(confOverwrite);

        logger.info("RDD Output path: {}", outputPath);
        setHadoopConf(job, cubeSegment, metaUrl);

        int countMeasureIndex = 0;
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (measureDesc.getFunction().isCount() == true) {
                break;
            } else {
                countMeasureIndex++;
            }
        }
        final CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, envConfig);
        boolean[] needAggr = new boolean[cubeDesc.getMeasures().size()];
        boolean allNormalMeasure = true;
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            needAggr[i] = !cubeDesc.getMeasures().get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid();
            allNormalMeasure = allNormalMeasure && needAggr[i];
        }
        logger.info("All measure are normal (agg on all cuboids) ? : " + allNormalMeasure);
        StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK_SER();

        HiveContext sqlContext = new HiveContext(sc.sc());
        final Dataset<Row> intermediateTable = sqlContext.table(hiveTable);

        // encode with dimension encoding, transform to <ByteArray, Object[]> RDD
        final JavaPairRDD<ByteArray, Object[]> encodedBaseRDD = intermediateTable.javaRDD()
                .mapToPair(new SparkCubingByLayer.EncodeBaseCuboid(cubeName, segmentId, metaUrl));

        Long totalCount = 0L;
        if (envConfig.isSparkSanityCheckEnabled()) {
            totalCount = encodedBaseRDD.count();
        }

        final BaseCuboidReducerFunction2 baseCuboidReducerFunction = new BaseCuboidReducerFunction2(cubeName, metaUrl);
        BaseCuboidReducerFunction2 reducerFunction2 = baseCuboidReducerFunction;
        if (allNormalMeasure == false) {
            reducerFunction2 = new CuboidReducerFunction2(cubeName, metaUrl, needAggr);
        }

        final int totalLevels = cubeDesc.getBuildLevel();
        JavaPairRDD<ByteArray, Object[]>[] allRDDs = new JavaPairRDD[totalLevels + 1];
        int level = 0;
        long baseRDDSize = SizeEstimator.estimate(encodedBaseRDD) / (1024 * 1024);
        int repartition = repartitionForOutput(cubeSegment, baseRDDSize, level);

        Map<Pair<Long, Short>, Integer> partitionMap = PartitionPreparer.preparePartitionMapping(envConfig, cubeSegment,
                repartition);
        // aggregate to calculate base cuboid
        allRDDs[0] = encodedBaseRDD.reduceByKey(baseCuboidReducerFunction)
                .repartitionAndSortWithinPartitions(new SparkCubingPartitioner(repartition, partitionMap))
                .persist(storageLevel);

        saveToHDFS(allRDDs[0], metaUrl, cubeName, cubeSegment, outputPath, 0, job);

        // aggregate to ND cuboids
        for (level = 1; level <= totalLevels; level++) {
            long levelRddSize = SizeEstimator.estimate(allRDDs[level - 1]) / (1024 * 1024);
            repartition = repartitionForOutput(cubeSegment, levelRddSize, level);
            partitionMap = PartitionPreparer.preparePartitionMapping(envConfig, cubeSegment, repartition);
            allRDDs[level] = allRDDs[level - 1].flatMapToPair(new CuboidFlatMap(cubeName, segmentId, metaUrl))
                    .reduceByKey(reducerFunction2)
                    .repartitionAndSortWithinPartitions(new SparkCubingPartitioner(repartition, partitionMap))
                    .persist(storageLevel);
            if (envConfig.isSparkSanityCheckEnabled() == true) {
                sanityCheck(allRDDs[level], totalCount, level, cubeStatsReader, countMeasureIndex);
            }
            saveToHDFS(allRDDs[level], metaUrl, cubeName, cubeSegment, outputPath, level, job);
            allRDDs[level - 1].unpersist();
        }
        allRDDs[totalLevels].unpersist();
        logger.info("Finished on calculating all level cuboids.");
        deleteHDFSMeta(metaUrl);
    }

    protected int repartitionForOutput(CubeSegment segment, long rddSize, int level)
            throws ClassNotFoundException, JobException, InterruptedException, IOException {
        int partition = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment, (double) rddSize, level);
        List<List<Long>> layeredCuboids = segment.getCubeDesc().getCuboidScheduler().getCuboidsByLayer();
        for (Long cuboidId : layeredCuboids.get(level)) {
            partition = Math.max(partition, segment.getCuboidShardNum(cuboidId));
        }
        logger.info("Repartition for output, level: {}, rdd size: {}, partition: {}", level, rddSize, partition);
        return partition;
    }

    protected void setHadoopConf(Job job, CubeSegment segment, String metaUrl) throws Exception {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, segment.getCubeInstance().getName());
        job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segment.getUuid());
        job.getConfiguration().set(BatchConstants.CFG_MR_SPARK_JOB, "spark");
        job.getConfiguration().set(BatchConstants.CFG_SPARK_META_URL, metaUrl);
    }

    static class SparkCubingPartitioner extends Partitioner {
        private Map<Pair<Long, Short>, Integer> partitionMap;
        private int partitions;

        public SparkCubingPartitioner(int partitions, Map<Pair<Long, Short>, Integer> partitionMap) {
            this.partitions = partitions;
            this.partitionMap = partitionMap;
        }

        public int getByteArrayPartition(byte[] key) {
            short shardId = (short) BytesUtil.readShort(key, 0, RowConstants.ROWKEY_SHARDID_LEN);
            long cuboidId = BytesUtil.readLong(key, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
            if (partitionMap == null) {
                return mod(key, 0, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, partitions);
            }
            int partition = partitionMap.get(new Pair<>(cuboidId, shardId));
            return partition;
        }

        protected int mod(byte[] src, int start, int end, int total) {
            int sum = Bytes.hashBytes(src, start, end - start);
            int mod = sum % total;
            if (mod < 0)
                mod += total;

            return mod;
        }

        @Override
        public int numPartitions() {
            return this.partitions;
        }

        @Override
        public int getPartition(Object o) {
            return getByteArrayPartition(((ByteArray) o).array());
        }
    }
}
