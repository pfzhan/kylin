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

package io.kyligence.kap.engine.spark.builder;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;

public class NDatasetChooser {
    protected static final Logger logger = LoggerFactory.getLogger(NDatasetChooser.class);

    private NSpanningTree toBuildTree;
    private NDataSegment seg;
    private KylinConfig config;
    private SparkSession ss;
    private Map<NCuboidDesc, DataSource> sources = new HashMap<>();

    public NDatasetChooser(NSpanningTree toBuildTree, NDataSegment seg, SparkSession ss, KylinConfig config) {
        this.toBuildTree = toBuildTree;
        this.ss = ss;
        this.seg = seg;
        this.config = config;
    }

    public Map<NCuboidDesc, DataSource> decideSources() throws Exception {
        Dataset<Row> flatTable = null;
        for (NCuboidDesc rootCuboid : toBuildTree.getRootCuboidDescs()) {
            DataSource dataSource = new DataSource();
            NCuboidLayout layout = NCuboidLayoutChooser.selectCuboidLayout(seg,
                    rootCuboid.getEffectiveDimCols().keySet(), toBuildTree.retrieveAllMeasures(rootCuboid));
            NDataSegDetails segCuboids = seg.getSegDetails();

            if (layout != null) {
                NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, layout.getId());
                Dataset<Row> source = StorageFactory
                        .createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                        .getCuboidData(dataCuboid, ss);
                dataSource.ds = source;
                dataSource.sizeKB = dataCuboid.getSizeKB();
                dataSource.count = dataCuboid.getRows();
                sources.put(rootCuboid, dataSource);
                logger.info("Reuse a suitable layout: {} for building cuboid: {}", layout.getId(), rootCuboid.getId());
                break;
            } else {
                if (flatTable == null) {
                    flatTable = getDatasetFromFlatTable();
                    //TODO: should use better method to detect the modifications.
                    long count = flatTable.count();
                    if (0 == count) {
                        throw new RuntimeException(
                                "There are no available records in the flat table, the relevant model: "
                                        + seg.getModel().getName()
                                        + ", please make sure there are available records in the \n"
                                        + "source tables, and made the correct join on the model.");
                    }

                    if (-1 == seg.getSourceCount()) {
                        // first build of this segment, fill row count
                        NDataSegment segCopy = seg.getDataflow().copy().getSegment(seg.getId());
                        segCopy.setSourceCount(count);
                        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
                        update.setToUpdateSegs(segCopy);
                        NDataflow updated = NDataflowManager.getInstance(config).updateDataflow(update);
                        seg = updated.getSegment(seg.getId());

                    } else if (seg.getSourceCount() != count) {
                        throw new RuntimeException(
                                "Error: Current flat table's records are inconsistent with before, \n"
                                        + "please check if there are any modifications on the source tables, \n"
                                        + "the relevant model: " + seg.getModel().getName()
                                        + ", if the data in the source table has been changed \n"
                                        + "in purpose, KAP would update all the impacted cuboids.");
                        //TODO: Update all ready cuboids by using last data.
                    }
                }

                dataSource.ds = flatTable;
                dataSource.sizeKB = NSizeEstimator.estimate(flatTable,
                        KapConfig.wrap(config).getSampleDatasetSizeRatio()) / 1024;
                sources.put(rootCuboid, dataSource);
                logger.info("No suitable ready layouts could be reused, generate dataset from flat table.");
            }
        }
        return sources;
    }

    private Dataset<Row> getDatasetFromFlatTable() throws Exception {
        NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(seg.getCubePlan(), seg.getSegRange());
        Dataset<Row> afterJoin = NJoinedFlatTable.generateDataset(flatTable, ss);
        NDictionaryBuilder dictionaryBuilder = new NDictionaryBuilder(seg, afterJoin);
        seg = dictionaryBuilder.buildDictionary(); // note the segment instance is updated
        Dataset<Row> afterEncode = new NFlatTableEncoder(afterJoin, seg, config).encode();
        return afterEncode;
    }

    public static class DataSource {
        public Dataset<Row> ds;
        public long sizeKB;
        public long count;
    }
}
