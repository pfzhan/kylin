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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.NDictionaryManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;

public class NDataflowMergeJob extends NDataflowJob {
    protected static final Logger logger = LoggerFactory.getLogger(NDataflowMergeJob.class);

    @Override
    protected Options getOptions() {
        return super.getOptions();
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        super.execute(optionsHelper);
        String dfName = optionsHelper.getOptionValue(OPTION_DATAFLOW_NAME);
        int newSegmentId = Integer.parseInt(optionsHelper.getOptionValue(OPTION_SEGMENT_IDS));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(optionsHelper.getOptionValue(OPTION_LAYOUT_IDS));

        // Step1: merge dictionary
        mergeDictionary(dfName, newSegmentId);

        // Step2: merge and save segments
        mergeSegments(dfName, newSegmentId, layoutIds);
    }

    private void mergeDictionary(String dataflowName, int segmentId) {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowName);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Collections.sort(mergingSegments);

        try {
            NDataflow flowCopy = dataflow.copy();
            NDataSegment segCopy = flowCopy.getSegment(segmentId);

            makeDictForNewSegment(flowCopy, segCopy, mergingSegments);
            makeSnapshotForNewSegment(segCopy, mergingSegments);

            NDataflowUpdate update = new NDataflowUpdate(dataflowName);
            update.setToUpdateSegs(segCopy);
            mgr.updateDataflow(update);
        } catch (IOException e) {
            logger.error("fail to merge dictionary or lookup snapshots", e);
        }

    }

    private void makeDictForNewSegment(NDataflow dataflow, NDataSegment newSeg, List<NDataSegment> mergingSegments)
            throws IOException {
        NDictionaryManager dictMgr = NDictionaryManager.getInstance(config, project);
        NCubePlan cubePlan = dataflow.getCubePlan();

        for (TblColRef col : cubePlan.getAllColumnsNeedDictionaryBuilt()) {
            List<DictionaryInfo> dictInfos = new ArrayList<DictionaryInfo>();
            for (NDataSegment segment : mergingSegments) {
                logger.info("Including fact table dictionary of segment : " + segment);
                if (segment.getDictResPath(col) != null) {
                    DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                    if (dictInfo != null && !dictInfos.contains(dictInfo)) {
                        dictInfos.add(dictInfo);
                    } else {
                        logger.warn("Failed to load DictionaryInfo from " + segment.getDictResPath(col));
                    }
                }
            }
            mergeDictionaries(dictMgr, newSeg, dictInfos, col);
        }
    }

    private DictionaryInfo mergeDictionaries(NDictionaryManager dictMgr, NDataSegment seg, List<DictionaryInfo> dicts,
            TblColRef col) throws IOException {
        DictionaryInfo dictInfo = dictMgr.mergeDictionary(dicts);
        if (dictInfo != null) {
            seg.putDictResPath(col, dictInfo.getResourcePath());
        }

        return dictInfo;
    }

    private void makeSnapshotForNewSegment(NDataSegment newSeg, List<NDataSegment> mergingSegments) {
        NDataSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    private void mergeSegments(String dataflowName, int segmentId, Set<Long> specifiedCuboids) throws IOException {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowName);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        // collect layouts need to merge
        Map<Long, NLayoutMergeAssist> mergeCuboidsAsssit = Maps.newConcurrentMap();
        for (NDataSegment seg : mergingSegments) {
            for (NDataCuboid cuboid : seg.getSegDetails().getCuboidByStatus(SegmentStatusEnum.READY)) {
                long layoutId = cuboid.getCuboidLayoutId();

                NLayoutMergeAssist assist = mergeCuboidsAsssit.get(layoutId);
                if (assist == null) {
                    assist = new NLayoutMergeAssist();
                    assist.addCuboid(cuboid);
                    assist.setSs(ss);
                    assist.setNewSegment(mergedSeg);
                    assist.setLayout(cuboid.getCuboidLayout());
                    assist.setToMergeSegments(mergingSegments);
                    mergeCuboidsAsssit.put(layoutId, assist);
                } else
                    assist.addCuboid(cuboid);
            }
        }

        for (NLayoutMergeAssist assist : mergeCuboidsAsssit.values()) {
            Dataset<Row> afterMerge = assist.merge();
            NCuboidLayout layout = assist.getLayout();
            Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
            Dataset<Row> afterAgg = new NCuboidAggregator(ss, afterMerge, layout.getOrderedDimensions().keySet(),
                    layout.getOrderedMeasures()).aggregate();
            long count = afterAgg.count();
            int partition = NDataflowBuildJob.estimatePartitions(afterAgg, config);
            Dataset<Row> afterSort = afterAgg.repartition(partition).sortWithinPartitions(dimsCols);
            saveAndUpdateCuboid(afterSort, count, mergedSeg, layout, assist);
        }
    }

    private void saveAndUpdateCuboid(Dataset<Row> dataset, long cuboidRowCnt, NDataSegment seg, NCuboidLayout layout,
            NLayoutMergeAssist assist) throws IOException {
        long layoutId = layout.getId();
        long sourceSizeKB = 0L;
        long sourceCount = 0L;

        for (NDataCuboid cuboid : assist.getCuboids()) {
            sourceSizeKB += cuboid.getSourceKB();
            sourceCount += cuboid.getSourceRows();
        }

        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(seg.getDataflow(), seg.getId(), layoutId);
        dataCuboid.setRows(cuboidRowCnt);
        dataCuboid.setSourceKB(sourceSizeKB);
        dataCuboid.setSourceRows(sourceCount);
        dataCuboid.setBuildJobId(jobId);
        dataCuboid.setStatus(SegmentStatusEnum.READY);

        StorageFactory.createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                .saveCuboidData(dataCuboid, dataset, ss);
        NDataflowBuildJob.fillCuboid(dataCuboid);

        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
        update.setToAddOrUpdateCuboids(dataCuboid);
        NDataflowManager.getInstance(config, project).updateDataflow(update);
    }

    public static void main(String[] args) {
        NDataflowMergeJob mergeJob = new NDataflowMergeJob();
        mergeJob.execute(args);
    }
}
