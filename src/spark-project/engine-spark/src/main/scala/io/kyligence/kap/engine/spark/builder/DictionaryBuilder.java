/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.engine.spark.builder;

import static io.kyligence.kap.engine.spark.builder.NGlobalDictionaryBuilderAssist.resize;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import scala.Tuple2;

public class DictionaryBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(DictionaryBuilder.class);
    private SparkSession ss;
    private NDataSegment seg;
    private Set<TblColRef> colRefSet;
    private DistributedLock lock;

    public DictionaryBuilder(NDataSegment seg, SparkSession ss, Set<TblColRef> colRefSet) {
        this.seg = seg;
        this.ss = ss;
        this.colRefSet = colRefSet;
        lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
    }

    public void buildDictionary() throws IOException {

        logger.info("Building global dictionaries V2 for seg {}", seg);

        final long start = System.currentTimeMillis();

        for (TblColRef col : colRefSet) {
            safeBuild(col);
        }

        final long end = System.currentTimeMillis();

        logger.info("Building global dictionaries V2 for seg {} , cost {} ms", seg, end - start);
    }

    void safeBuild(TblColRef col) throws IOException {
        String sourceColumn = col.getTable() + "." + col.getName();
        lock.lock(getLockPath(sourceColumn), Long.MAX_VALUE);
        try {
            if (lock.lock(getLockPath(sourceColumn))) {
                TableDesc tableDesc = col.getTableRef().getTableDesc();
                Dataset<Row> df = SourceFactory
                        .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                        .getSourceData(tableDesc, ss, new HashMap<>());
                Dataset<Row> dictColDistinct = df.select(col.getName()).distinct();
                int bucketPartitionSize = calculateBucketSize(col, dictColDistinct);
                build(col, bucketPartitionSize, dictColDistinct);
            }
        } finally {
            lock.unlock(getLockPath(sourceColumn));
        }
    }

    /**
     * Dictionary resize in three cases
     *  #1 The number of dictionaries currently needed to be built is greater than the number of
     *  buckets multiplied by the threshold
     *  #2 After the last build, the total number of existing dictionaries is greater than the total
     *  number of buckets multiplied by the threshold
     *  #3 After the last build, the number of individual buckets in the existing dictionary is greater
     *  than the threshold multiplied by KylinConfigBase.getGlobalDictV2BucketOverheadFactor
     */
    int calculateBucketSize(TblColRef col, Dataset<Row> afterDistinct) throws IOException {
        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(seg.getProject(), col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        int bucketPartitionSize = globalDict.getBucketSizeOrDefault(seg.getConfig().getGlobalDictV2MinHashPartitions());
        int bucketThreshold = seg.getConfig().getGlobalDictV2ThresholdBucketSize();
        int resizeBucketSize = bucketPartitionSize;

        if (globalDict.isFirst()) {
            long afterDisCount = afterDistinct.count();
            double loadFactor = seg.getConfig().getGlobalDictV2InitLoadFactor();
            resizeBucketSize = Math.max(Math.toIntExact(afterDisCount / (int) (bucketThreshold * loadFactor)),
                    bucketPartitionSize);
            logger.info("Building a global dictionary column first for  {} , the size of the bucket is set to {}",
                    col.getName(), bucketPartitionSize);
        } else {
            long afterDisCount = afterDistinct.count();
            NGlobalDictMetaInfo metaInfo = globalDict.getMetaInfo();
            long[] bucketCntArray = metaInfo.getBucketCount();

            double loadFactor = seg.getConfig().getGlobalDictV2InitLoadFactor();
            double bucketOverheadFactor = seg.getConfig().getGlobalDictV2BucketOverheadFactor();

            int averageBucketSize = 0;

            // rule #1
            int newDataBucketSize = Math.toIntExact(afterDisCount / bucketThreshold);
            if (newDataBucketSize > metaInfo.getBucketSize()) {
                newDataBucketSize = Math.toIntExact(afterDisCount / (int) (bucketThreshold * loadFactor));
            }

            // rule #2
            if (metaInfo.getDictCount() >= bucketThreshold * metaInfo.getBucketSize()) {
                averageBucketSize = Math.toIntExact(metaInfo.getDictCount() / (int) (bucketThreshold * loadFactor));
            }

            int peakBucketSize = 0;
            //rule #3
            for (long bucketCnt : bucketCntArray) {
                if (bucketCnt > bucketThreshold * bucketOverheadFactor) {
                    peakBucketSize = bucketPartitionSize * 2;
                    break;
                }
            }

            resizeBucketSize = Math.max(Math.max(newDataBucketSize, averageBucketSize),
                    Math.max(peakBucketSize, bucketPartitionSize));
        }

        if (resizeBucketSize != bucketPartitionSize) {
            logger.info("Start building a global dictionary column for {}, need resize from {} to {} ", col.getName(),
                    bucketPartitionSize, resizeBucketSize);
            resize(col, seg, resizeBucketSize, afterDistinct.sparkSession().sparkContext());
            logger.info("End building a global dictionary column for {}, need resize from {} to {} ", col.getName(),
                    bucketPartitionSize, resizeBucketSize);
        }

        return resizeBucketSize;
    }

    void build(TblColRef col, int bucketPartitionSize, Dataset<Row> afterDistinct) throws IOException {
        logger.info("Start building global dict V2 for column {}.", col.getTable() + "." + col.getName());

        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(seg.getProject(), col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        globalDict.prepareWrite();
        Broadcast<NGlobalDictionaryV2> broadcastDict = JavaSparkContext
                .fromSparkContext(afterDistinct.sparkSession().sparkContext()).broadcast(globalDict);
        afterDistinct.javaRDD().mapToPair((PairFunction<Row, String, String>) row -> {
            if (row.get(0) == null) {
                return new Tuple2<>(null, null);
            }
            return new Tuple2<>(row.get(0).toString(), null);
        }).partitionBy(new NHashPartitioner(bucketPartitionSize)).mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Object>>) (bucketId, tuple2Iterator) -> {
                    NGlobalDictionaryV2 gDict = broadcastDict.getValue();
                    NBucketDictionary bucketDict = gDict.loadBucketDictionary(bucketId);

                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, String> tuple2 = tuple2Iterator.next();
                        bucketDict.addRelativeValue(tuple2._1);
                    }

                    bucketDict.saveBucketDict(bucketId);

                    return Lists.newArrayList().iterator();
                }, true).count();

        globalDict.writeMetaDict(bucketPartitionSize, seg.getConfig().getGlobalDictV2MaxVersions(),
                seg.getConfig().getGlobalDictV2VersionTTL());

        logger.info("Build global dict V2 for column {} success.", col.getName());
    }

    private static Set<TblColRef> extractGlobalColumns(NDataSegment seg, NSpanningTree toBuildTree, Boolean isBuild) {

        Collection<IndexEntity> toBuildIndexEntities = toBuildTree.getAllIndexEntities();
        List<LayoutEntity> toBuildCuboids = Lists.newArrayList();
        for (IndexEntity desc : toBuildIndexEntities) {
            if (isBuild) {
                LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(seg, desc);
                if (layout == null) {
                    toBuildCuboids.addAll(desc.getLayouts());
                }
            } else {
                toBuildCuboids.addAll(desc.getLayouts());
            }
        }

        List<LayoutEntity> buildedLayouts = Lists.newArrayList();
        if (seg.getSegDetails() != null && isBuild) {
            for (NDataLayout cuboid : seg.getSegDetails().getLayouts()) {
                buildedLayouts.add(cuboid.getLayout());
            }
        }
        Set<TblColRef> buildedColRefSet = findNeedDictCols(buildedLayouts);
        Set<TblColRef> toBuildColRefSet = findNeedDictCols(toBuildCuboids);
        toBuildColRefSet.removeIf(col -> buildedColRefSet.contains(col));
        return toBuildColRefSet;
    }

    private static Set<TblColRef> findNeedDictCols(List<LayoutEntity> layouts) {
        Set<TblColRef> dictColSet = Sets.newHashSet();
        for (LayoutEntity layout : layouts) {
            for (MeasureDesc measureDesc : layout.getIndex().getEffectiveMeasures().values()) {
                if (needGlobalDictionary(measureDesc) == null)
                    continue;
                TblColRef col = measureDesc.getFunction().getParameters().get(0).getColRef();
                dictColSet.add(col);
            }
        }
        return dictColSet;
    }

    public static Set<TblColRef> extractGlobalDictColumns(NDataSegment seg, NSpanningTree toBuildTree) {
        return extractGlobalColumns(seg, toBuildTree, true);
    }

    public static Set<TblColRef> extractGlobalEncodeColumns(NDataSegment seg, NSpanningTree toBuildTree) {
        return extractGlobalColumns(seg, toBuildTree, false);
    }

    public static TblColRef needGlobalDictionary(MeasureDesc measure) {
        String returnDataTypeName = measure.getFunction().getReturnDataType().getName();
        if (returnDataTypeName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
            List<TblColRef> cols = measure.getFunction().getColRefs();
            Preconditions.checkArgument(cols.size() == 1);
            return cols.get(0);
        }
        return null;
    }

    private String getLockPath(String pathName) {
        return "/" + seg.getProject() + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/" + pathName + "/lock";
    }

}
