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
import java.util.Iterator;
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

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class DictionaryBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(DictionaryBuilder.class);
    private Dataset<Row> dataSet;
    private NDataSegment seg;
    private DistributedLock lock;

    public DictionaryBuilder(NDataSegment seg, Dataset<Row> dataSet) {
        this.seg = seg;
        this.dataSet = dataSet;
        lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
    }

    public NDataSegment buildDictionary() throws Exception {

        logger.info("building global dictionaries V2 for seg {}", seg);

        final long start = System.currentTimeMillis();

        Set<TblColRef> colRefSet = extractGlobalDictColumns(seg);

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
                build(col);
            }
        } finally {
            lock.unlock(getLockPath(sourceColumn));
        }
    }

    private void build(TblColRef col) throws IOException {
        logger.info("building global dict V2 for column {}", col.getTable() + "_" + col.getName());

        int id = seg.getDataflow().getCubePlan().getModel().getColumnIdByColumnName(col.getIdentity());
        final Dataset<Row> afterDistinct = dataSet.select(String.valueOf(id)).distinct();

        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        globalDict.prepareWrite();

        Broadcast<NGlobalDictionaryV2> broadcastDict = afterDistinct.sparkSession().sparkContext().broadcast(globalDict,
                ClassTag$.MODULE$.apply(NGlobalDictionaryV2.class));

        int bucketPartitionSize = globalDict.getBucketSize(seg.getConfig().getGlobalDictV2HashPartitions());
        afterDistinct.toJavaRDD().mapToPair((PairFunction<Row, String, String>) row -> {
            if (row.get(0) == null)
                return new Tuple2<>(null, null);
            return new Tuple2<>(row.get(0).toString(), null);
        }).partitionBy(new NHashPartitioner(bucketPartitionSize)).mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Object>>) (bucketId, tuple2Iterator) -> {
                    NGlobalDictionaryV2 gDict = broadcastDict.getValue();
                    NBucketDictionary bucketDict = gDict.createBucketDictionary(bucketId);

                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, String> tuple2 = tuple2Iterator.next();
                        bucketDict.addValue(tuple2._1);
                    }

                    bucketDict.saveBucketDict(bucketId);

                    return Lists.newArrayList().iterator();
                }, true).count();

        globalDict.writeMetaDict(seg.getConfig().getGlobalDictV2MaxVersions(),
                seg.getConfig().getGlobalDictV2VersionTTL());
    }

    protected static Set<TblColRef> extractGlobalDictColumns(NDataSegment seg) {
        Set<TblColRef> colRefSet = Sets.newHashSet();
        NCubePlan cubePlan = seg.getDataflow().getCubePlan();
        for (NCuboidLayout layout : cubePlan.getAllCuboidLayouts()) {
            for (MeasureDesc measureDesc : layout.getCuboidDesc().getEffectiveMeasures().values()) {
                if (NDictionaryBuilder.needGlobalDictionary(measureDesc) == null)
                    continue;
                TblColRef col = measureDesc.getFunction().getParameter().getColRef();
                colRefSet.add(col);
            }
        }

        return colRefSet;
    }

    private String getLockPath(String pathName) {
        return ResourceStore.GLOBAL_DICT_RESOURCE_ROOT + "/" + pathName + "/lock";
    }

}
