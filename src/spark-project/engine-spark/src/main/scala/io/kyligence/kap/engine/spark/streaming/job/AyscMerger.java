package io.kyligence.kap.engine.spark.streaming.job;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.streaming.common.MergeJobEntry;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import java.util.List;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncMerger extends Thread {

  private static final Logger logger = LoggerFactory
      .getLogger(AsyncMerger.class);

  private MergeJobEntry mergeJobEntry;

  AsyncMerger(MergeJobEntry mergeJobEntry) {
    this.mergeJobEntry = mergeJobEntry;
  }

  @Override
  public void run() {
    logger.info("start merge merge segment");
    logger.info(mergeJobEntry.toString());
    val config = KylinConfig.getInstanceFromEnv();
    KylinBuildEnv.clean();
    KylinBuildEnv buildEnv = KylinBuildEnv.getOrCreate(config);

    val start = System.currentTimeMillis();
    try {
      StreamingDFMergeJob merger = new StreamingDFMergeJob();
      merger.streamingMergeSegment(mergeJobEntry);
      logger.info("merge segment cost {}", System.currentTimeMillis() - start);
      logger.info("delete merged segment and change the status");
      mergeJobEntry.globalMergeTime().set(System.currentTimeMillis() - start);
      UnitOfWork.doInTransactionWithRetry(() -> {
        NDataflowManager dfMgr = NDataflowManager
            .getInstance(KylinConfig.getInstanceFromEnv(), mergeJobEntry.project());
        NDataflow copy = dfMgr.getDataflow(mergeJobEntry.dataflowId()).copy();
        val seg = copy.getSegment(mergeJobEntry.afterMergeSegment().getId());
        seg.setStatus(SegmentStatusEnum.READY);
        val dfUpdate = new NDataflowUpdate(mergeJobEntry.dataflowId());
        dfUpdate.setToUpdateSegs(seg);
        List<NDataSegment> toRemoveSegs = mergeJobEntry.unMergedSegments();
        dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(dfUpdate);
        return 0;
      }, mergeJobEntry.project());

    } catch (Exception e) {
      logger.info("merge failed reason: {} stackTrace is: {}", e.toString(), e.getStackTrace());
    }
  }
}