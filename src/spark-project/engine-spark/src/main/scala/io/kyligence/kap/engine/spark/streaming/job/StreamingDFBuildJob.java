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
package io.kyligence.kap.engine.spark.streaming.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.engine.spark.job.BuildJobInfos;
import io.kyligence.kap.engine.spark.job.BuildLayoutWithUpdate;
import io.kyligence.kap.engine.spark.job.DFBuildJob;
import io.kyligence.kap.engine.spark.streaming.StreamingCommitter;
import io.kyligence.kap.engine.spark.streaming.common.BuildJobEntry;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StreamingDFBuildJob extends DFBuildJob {

  private HashMap<Long, Dataset<Row>> cuboidDatasetMap;

  public void streamBuild(BuildJobEntry buildJobEntry) throws IOException {
    val config = KylinConfig.getInstanceFromEnv();
    dfMgr = NDataflowManager.getInstance(config, buildJobEntry.project());
    this.ss = buildJobEntry.spark();
    ss.sparkContext().setLocalProperty("spark.sql.execution.id", null);
    buildLayoutWithUpdate = new BuildLayoutWithUpdate();
    this.jobId = UUID.randomUUID().toString();
    this.project = buildJobEntry.project();
    this.infos = new BuildJobInfos();
    cuboidDatasetMap = Maps.newHashMap();

    setParam(NBatchConstants.P_DATAFLOW_ID, buildJobEntry.dataflowId());

    Preconditions.checkState(buildJobEntry.toBuildTree().getRootIndexEntities().size() != 0,
        "streaming mast have one root index");

    val theRootLevelBuildInfos = new NBuildSourceInfo();
    theRootLevelBuildInfos.setFlattableDS(buildJobEntry.streamingFlatDS());
    theRootLevelBuildInfos.setSparkSession(ss);
    theRootLevelBuildInfos.setToBuildCuboids(buildJobEntry.toBuildTree().getRootIndexEntities());

    build(Sets.newHashSet(theRootLevelBuildInfos), buildJobEntry.batchSegment().getId(),
        buildJobEntry.toBuildTree());

    StreamingCommitter.commit(ss, cuboidDatasetMap, buildJobEntry.batchSegment(), project);

    logger.info("start update segment");
    UnitOfWork.doInTransactionWithRetry(() -> {
      NDataflowManager dfMgr = NDataflowManager
          .getInstance(KylinConfig.getInstanceFromEnv(), project);
      NDataflow newDF = dfMgr.getDataflow(buildJobEntry.dataflowId()).copy();
      NDataSegment segUpdate = newDF.getSegment(buildJobEntry.batchSegment().getId());
      segUpdate.setStatus(SegmentStatusEnum.READY);
      val dfUpdate = new NDataflowUpdate(buildJobEntry.dataflowId());
      dfUpdate.setToUpdateSegs(segUpdate);
      dfMgr.updateDataflow(dfUpdate);
      return 0;
    }, project);
  }


  @Override
  protected List<NBuildSourceInfo> constructTheNextLayerBuildInfos( //
      NSpanningTree st, //
      NDataSegment seg, //
      Collection<IndexEntity> allIndexesInCurrentLayer) { //
    val childrenBuildSourceInfos = new ArrayList<NBuildSourceInfo>();
    for (IndexEntity index : allIndexesInCurrentLayer) {
      val children = st.getChildrenByIndexPlan(index);
      if (!children.isEmpty()) {
        val theRootLevelBuildInfos = new NBuildSourceInfo();
        theRootLevelBuildInfos.setSparkSession(ss);
        LayoutEntity layout = new ArrayList<>(st.getLayouts(index)).get(0);
        val parentDataset = cuboidDatasetMap.get(layout.getId());
        theRootLevelBuildInfos.setLayoutId(layout.getId());
        theRootLevelBuildInfos.setToBuildCuboids(children);
        theRootLevelBuildInfos.setFlattableDS(parentDataset);
        childrenBuildSourceInfos.add(theRootLevelBuildInfos);
      }
    }
    // return the next to be built layer.
    return childrenBuildSourceInfos;
  }

  @Override protected NDataLayout saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout)
      throws IOException {
    // just count and cache
    ss.sparkContext().setLocalProperty("spark.scheduler.pool", "build");
    long layoutId = layout.getId();
    NDataLayout dataLayout = getDataLayout(seg, layoutId);
    StreamingCommitter.saveAndCachedataset(dataset, ss, layout);
    cuboidDatasetMap.put(layout.getId(), dataset);
    return dataLayout;
  }

}
