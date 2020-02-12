package io.kyligence.kap.engine.spark.streaming.job;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.streaming.common.MergeJobEntry;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.OffsetRangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingSegmentManager {

  private static final Logger logger = LoggerFactory
      .getLogger(io.kyligence.kap.engine.spark.streaming.job.StreamingSegmentManager.class);
  private static Integer mergeSegmentThresholds = KylinConfig.getInstanceFromEnv()
      .getStreamingSegmentMergeThresholds();

  private static List<NDataSegment> retainSegments = Lists.newArrayList();
  private static Long minTime = Long.MAX_VALUE;
  private static Long maxTime = 0L;
  private static AtomicLong  globalMergeTime = new AtomicLong(0);

  public static void setMergeSegmentThresholdsForUT(Integer threshold) {
    mergeSegmentThresholds = threshold;
  }

  public static void registMetrics(String model) {
    NMetricsGroup.newGauge(NMetricsName.RETAIN_SEGMENTS, NMetricsCategory.MODEL, model, () -> {
      return retainSegments.size();
    });
    NMetricsGroup.newGauge(NMetricsName.SEGMENTS_MERGE_THRESHOLDS, NMetricsCategory.MODEL, model, () -> {
      return mergeSegmentThresholds;
    });
    NMetricsGroup.newGauge(NMetricsName.SEGMENTS_MERGE_COST_TIME, NMetricsCategory.MODEL, model, () -> {
      return globalMergeTime.get();
    });
  }

  public static void cleanRetainSegments() {
    retainSegments = Lists.newArrayList();
  }

  public static NDataSegment allocateSegment(SparkSession ss,
      String dataflowId, String project,
      String committedOffsets, String availableOffsets, Long batchMinTime, Long batchMaxTime) {

    val sr = new SegmentRange.KafkaOffsetPartitionedSegmentRange(batchMinTime, batchMaxTime,
        OffsetRangeManager.partitionOffsets(committedOffsets),
        OffsetRangeManager.partitionOffsets(availableOffsets));

    logger.info("unmerge size is {} threshold is {}", retainSegments.size(),
        mergeSegmentThresholds);

    if (retainSegments.size() > mergeSegmentThresholds) {

      logger.info("start merge");

      Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();
      Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();

      retainSegments.forEach(seg -> {
        val range = seg.getKSRange();
        if (range.getStart() != null && range.getStart() < minTime) {
          minTime = range.getStart();
        }
        if (range.getEnd() != null && range.getEnd() > maxTime) {
          maxTime = range.getEnd();
        }

        range.getSourcePartitionOffsetStart().forEach((partition, offset) -> {
          if (!sourcePartitionOffsetStart.containsKey(partition)
              || sourcePartitionOffsetStart.get(partition) > offset) {
            sourcePartitionOffsetStart.put(partition, offset);
          }
        });
        range.getSourcePartitionOffsetEnd().forEach((partition, offset) -> {
          if (!sourcePartitionOffsetEnd.containsKey(partition)
              || sourcePartitionOffsetEnd.get(partition) < offset) {
            sourcePartitionOffsetEnd.put(partition, offset);
          }
        });
      });

      val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(minTime, maxTime,
          sourcePartitionOffsetStart, sourcePartitionOffsetEnd);

      val afterMergeSeg = UnitOfWork.doInTransactionWithRetry(() -> {
        NDataflowManager dfMgr = NDataflowManager
            .getInstance(KylinConfig.getInstanceFromEnv(), project);
        return dfMgr.mergeSegments(dfMgr.getDataflow(dataflowId), rangeToMerge, true);
      }, project);

      minTime = Long.MAX_VALUE;
      maxTime = 0L;
      logger.info("start aysc thread for merge");

      val config = KylinConfig.getInstanceFromEnv();
      val dfMgr = NDataflowManager.getInstance(config, project);
      val df = dfMgr.getDataflow(dataflowId);

      val updatedSegments = retainSegments.stream().map(seg -> {
        return df.getSegment(seg.getId());
      }).collect(Collectors.toList());

      val mergeJobEntry = new MergeJobEntry(ss, project, dataflowId, globalMergeTime, updatedSegments,
          afterMergeSeg);
      AsyncMerger asyncMerge  = new AsyncMerger(mergeJobEntry);
      asyncMerge.setDaemon(true);
      asyncMerge.setName("merge-thread");
      asyncMerge.start();

      retainSegments = Lists.newArrayList();
    }

    val newSegment = UnitOfWork.doInTransactionWithRetry(() -> {
      NDataflowManager dfMgr = NDataflowManager
          .getInstance(KylinConfig.getInstanceFromEnv(), project);
      return dfMgr.appendSegmentForStreaming(dfMgr.getDataflow(dataflowId), sr);
    }, project);
    retainSegments.add(newSegment);
    return newSegment;
  }

}
