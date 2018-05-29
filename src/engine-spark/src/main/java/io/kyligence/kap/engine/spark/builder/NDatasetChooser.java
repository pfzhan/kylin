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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NDatasetChooser {
    protected static final Logger logger = LoggerFactory.getLogger(NDatasetChooser.class);

    private NSpanningTree toBuildTree;
    private NDataSegment seg;
    private KylinConfig config;
    private SparkSession ss;
    private List<NBuildSourceInfo> reuseSources = new ArrayList<>();
    private NBuildSourceInfo flatTableSource = null;

    public NDatasetChooser(NSpanningTree toBuildTree, NDataSegment seg, SparkSession ss, KylinConfig config) {
        this.toBuildTree = toBuildTree;
        this.ss = ss;
        this.seg = seg;
        this.config = config;
    }

    public List<NBuildSourceInfo> getReuseSources() {
        return reuseSources;
    }

    public NBuildSourceInfo getFlatTableSource() {
        return flatTableSource;
    }

    public void decideSources() throws Exception {
        for (NCuboidDesc rootCuboid : toBuildTree.getRootCuboidDescs()) {
            final NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(seg,
                    rootCuboid.getEffectiveDimCols().keySet(), toBuildTree.retrieveAllMeasures(rootCuboid));

            if (layout != null) {
                Collection<NBuildSourceInfo> layoutSources = Collections2.filter(reuseSources,
                        new Predicate<NBuildSourceInfo>() {
                            @Override
                            public boolean apply(@Nullable NBuildSourceInfo input) {
                                return (input.getLayoutId() == layout.getId());
                            }
                        });
                if (layoutSources.size() == 0) {
                    NBuildSourceInfo sourceInfo = getSourceFromLayout(layout, rootCuboid);
                    reuseSources.add(sourceInfo);
                } else {
                    Preconditions.checkState(layoutSources.size() == 1);
                    List<NBuildSourceInfo> foundSource = new ArrayList<>();
                    foundSource.addAll(layoutSources);
                    foundSource.get(0).addCuboid(rootCuboid);
                }
            } else {
                if (flatTableSource == null) {
                    NSnapshotBuilder snapshotBuilder = new NSnapshotBuilder(seg, ss);
                    seg = snapshotBuilder.buildSnapshot();
                    flatTableSource = getFlatTableAfterEncode();
                }
                flatTableSource.getToBuildCuboids().add(rootCuboid);
            }
        }
    }

    private NBuildSourceInfo getSourceFromLayout(NCuboidLayout layout, NCuboidDesc cuboidDesc) {
        NBuildSourceInfo buildSource = new NBuildSourceInfo();
        NDataSegDetails segDetails = seg.getSegDetails();
        NDataCuboid dataCuboid = segDetails.getCuboidById(layout.getId());
        Preconditions.checkState(dataCuboid != null);
        Dataset<Row> layoutDs = StorageFactory.createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                .getCuboidData(dataCuboid, ss);
        layoutDs.persist();
        buildSource.setDataset(layoutDs);
        buildSource.setCount(dataCuboid.getRows());
        buildSource.setLayoutId(layout.getId());
        buildSource.setSizeKB(dataCuboid.getSizeKB());
        buildSource.getToBuildCuboids().add(cuboidDesc);
        logger.info("Reuse a suitable layout: {} for building cuboid: {}", layout.getId(), cuboidDesc.getId());
        return buildSource;
    }

    private NBuildSourceInfo getFlatTableAfterEncode() throws Exception {

        NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(seg.getCubePlan(), seg.getSegRange());
        Dataset<Row> afterJoin = NJoinedFlatTable.generateDataset(flatTable, ss).persist();
        long sourceSize = NSizeEstimator.estimate(afterJoin, KapConfig.wrap(config).getSampleDatasetSizeRatio());
        NDictionaryBuilder dictionaryBuilder = new NDictionaryBuilder(seg, afterJoin);
        seg = dictionaryBuilder.buildDictionary(); // note the segment instance is updated
        afterJoin.unpersist();
        Dataset<Row> afterEncode = new NFlatTableEncoder(afterJoin, seg, config, ss).encode().persist();
        long count = afterEncode.count();
        //TODO: should use better method to detect the modifications.
        if (0 == count) {
            throw new RuntimeException("There are no available records in the flat table, the relevant model: "
                    + seg.getModel().getName() + ", please make sure there are available records in the \n"
                    + "source tables, and made the correct join on the model.");
        }

        if (-1 == seg.getSourceCount()) {
            // first build of this segment, fill row count
            NDataSegment segCopy = seg.getDataflow().copy().getSegment(seg.getId());
            segCopy.setSourceCount(count);
            NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
            update.setToUpdateSegs(segCopy);
            NDataflow updated = NDataflowManager.getInstance(config, seg.getDataflow().getProject()).updateDataflow(update);
            seg = updated.getSegment(seg.getId());

        } else if (seg.getSourceCount() != count) {
            throw new RuntimeException("Error: Current flat table's records are inconsistent with before, \n"
                    + "please check if there are any modifications on the source tables, \n" + "the relevant model: "
                    + seg.getModel().getName() + ", if the data in the source table has been changed \n"
                    + "in purpose, KAP would update all the impacted cuboids.");
            //TODO: Update all ready cuboids by using last data.
        }

        NBuildSourceInfo sourceInfo = new NBuildSourceInfo();
        sourceInfo.setSizeKB(sourceSize / 1024);
        sourceInfo.setCount(count);
        sourceInfo.setDataset(afterEncode);

        logger.info("No suitable ready layouts could be reused, generate dataset from flat table.");

        return sourceInfo;
    }

    static NBuildSourceInfo getDataSourceByCuboid(List<NBuildSourceInfo> sources, final NCuboidDesc cuboid) {

        List<NBuildSourceInfo> filterSources = new ArrayList<>();
        filterSources.addAll(Collections2.filter(sources, new Predicate<NBuildSourceInfo>() {
            @Override
            public boolean apply(@Nullable NBuildSourceInfo input) {
                for (NCuboidDesc ncd : input.getToBuildCuboids()) {
                    if (ncd == cuboid)
                        return true;
                }
                return false;
            }
        }));
        Preconditions.checkState(filterSources.size() == 1);
        return filterSources.get(0);
    }
}
