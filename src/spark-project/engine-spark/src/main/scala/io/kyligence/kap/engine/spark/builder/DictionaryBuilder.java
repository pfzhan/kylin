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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class DictionaryBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(DictionaryBuilder.class);
    private Dataset<Row> dataSet;
    private NDataSegment seg;
    private Set<TblColRef> colRefSet;
    private DistributedLock lock;

    public DictionaryBuilder(NDataSegment seg, Dataset<Row> dataSet, Set<TblColRef> colRefSet) {
        this.seg = seg;
        this.dataSet = dataSet;
        this.colRefSet = colRefSet;
        lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
    }

    public NDataSegment buildDictionary() throws Exception {

        logger.info("building global dictionaries V2 for seg {}", seg);

        final long start = System.currentTimeMillis();

        for (TblColRef col : colRefSet) {
            safeBuild(col);
        }

        final long end = System.currentTimeMillis();

        logger.info("building global dictionaries V2 for seg {} , cost {} ms", seg, end - start);

        return seg;
    }

    private void safeBuild(TblColRef col) throws IOException {
        String sourceColumn = col.getTable() + "_" + col.getName();
        lock.lock(getLockPath(sourceColumn), Long.MAX_VALUE);
        try {
            if (lock.lock(getLockPath(sourceColumn))) {
                int id = seg.getDataflow().getCubePlan().getModel().getColumnIdByColumnName(col.getIdentity());
                Dataset<Row> afterDistinct = dataSet.select(String.valueOf(id)).distinct();
                int bucketPartitionSize = seg.getConfig().getGlobalDictV2HashPartitions();
                if (needResize(col)) {
                    NGlobalDictionaryBuilderAssist.resize(col, seg, bucketPartitionSize,
                            afterDistinct.sparkSession().sparkContext());
                }
                build(col, bucketPartitionSize, afterDistinct);
            }
        } finally {
            lock.unlock(getLockPath(sourceColumn));
        }
    }

    private boolean needResize(TblColRef col) throws IOException {
        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        int bucketPartitionSize = globalDict.getBucketSizeOrDefault(seg.getConfig().getGlobalDictV2HashPartitions());
        int globalDictPartitions = seg.getConfig().getGlobalDictV2HashPartitions();
        return bucketPartitionSize != globalDictPartitions;
    }

    private void build(TblColRef col, int bucketPartitionSize, Dataset<Row> afterDistinct) throws IOException {
        logger.info("building global dict V2 for column {}", col.getTable() + "_" + col.getName());

        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        globalDict.prepareWrite();
        Broadcast<NGlobalDictionaryV2> broadcastDict = afterDistinct.sparkSession().sparkContext().broadcast(globalDict,
                ClassTag$.MODULE$.apply(NGlobalDictionaryV2.class));

        afterDistinct.toJavaRDD().mapToPair((PairFunction<Row, String, String>) row -> {
            if (row.get(0) == null)
                return new Tuple2<>(null, null);
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
    }

    private static Set<TblColRef> extractGlobalColumns(NDataSegment seg, NSpanningTree toBuildTree, Boolean isBuild)
            throws IOException {

        Collection<NCuboidDesc> toBuildCuboidDescs = toBuildTree.getAllCuboidDescs();
        List<NCuboidLayout> toBuildCuboids = Lists.newArrayList();
        for (NCuboidDesc desc : toBuildCuboidDescs) {
            if (isBuild) {
                NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(seg,
                        desc.getEffectiveDimCols().keySet(), toBuildTree.retrieveAllMeasures(desc));
                if (layout == null) {
                    toBuildCuboids.addAll(desc.getLayouts());
                }
            } else {
                toBuildCuboids.addAll(desc.getLayouts());
            }
        }

        Set<TblColRef> toBuildColRefSet = Sets.newHashSet();
        Set<TblColRef> buildedColRefSet = Sets.newHashSet();
        if (seg.getSegDetails() != null && isBuild) {
            for (NDataCuboid cuboid : seg.getSegDetails().getCuboids()) {
                NCuboidLayout layout = cuboid.getCuboidLayout();
                for (MeasureDesc measureDesc : layout.getCuboidDesc().getEffectiveMeasures().values()) {
                    if (NDictionaryBuilder.needGlobalDictionary(measureDesc) == null)
                        continue;
                    TblColRef col = measureDesc.getFunction().getParameter().getColRef();
                    buildedColRefSet.add(col);
                }
            }
        }

        for (NCuboidLayout layout : toBuildCuboids) {
            for (MeasureDesc measureDesc : layout.getCuboidDesc().getEffectiveMeasures().values()) {
                if (NDictionaryBuilder.needGlobalDictionary(measureDesc) == null)
                    continue;
                TblColRef col = measureDesc.getFunction().getParameter().getColRef();
                if (!buildedColRefSet.contains(col)) {
                    toBuildColRefSet.add(col);
                }
            }
        }
        return toBuildColRefSet;
    }

    public static Set<TblColRef> extractGlobalDictColumns(NDataSegment seg, NSpanningTree toBuildTree)
            throws IOException {
        return extractGlobalColumns(seg, toBuildTree, true);
    }

    public static Set<TblColRef> extractGlobalEncodeColumns(NDataSegment seg, NSpanningTree toBuildTree)
            throws IOException {
        return extractGlobalColumns(seg, toBuildTree, false);
    }

    private String getLockPath(String pathName) {
        return ResourceStore.GLOBAL_DICT_RESOURCE_ROOT + "/" + pathName + "/lock";
    }

}
