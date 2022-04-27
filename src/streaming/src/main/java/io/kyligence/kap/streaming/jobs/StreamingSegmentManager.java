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

package io.kyligence.kap.streaming.jobs;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.OffsetRangeManager;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.request.StreamingSegmentRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import io.kyligence.kap.streaming.util.JobExecutionIdHolder;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingSegmentManager {

    private static final String SEGMENT_POST_URL = "/streaming_jobs/dataflow/segment";

    public static NDataSegment allocateSegment(SparkSession ss, String dataflowId, String project, Long batchMinTime,
            Long batchMaxTime) {
        return allocateSegment(ss, null, dataflowId, project, batchMinTime, batchMaxTime);
    }

    public static NDataSegment allocateSegment(SparkSession ss, SegmentRange.KafkaOffsetPartitionedSegmentRange sr,
            String dataflowId, String project, Long batchMinTime, Long batchMaxTime) {
        if (sr == null) {
            val offsetRange = OffsetRangeManager.currentOffsetRange(ss);
            sr = new SegmentRange.KafkaOffsetPartitionedSegmentRange(batchMinTime, batchMaxTime,
                    OffsetRangeManager.partitionOffsets(offsetRange._1),
                    OffsetRangeManager.partitionOffsets(offsetRange._2));
        }
        NDataSegment newSegment;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (config.isUTEnv()) {
            val segRange = sr;
            newSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                return dfMgr.appendSegmentForStreaming(dfMgr.getDataflow(dataflowId), segRange);
            }, project);
            return newSegment;
        } else {
            StreamingSegmentRequest req = new StreamingSegmentRequest(project, dataflowId);
            req.setSegmentRange(sr);
            req.setNewSegId(RandomUtil.randomUUIDStr());
            req.setJobType(JobTypeEnum.STREAMING_BUILD.name());
            val jobId = StreamingUtils.getJobId(dataflowId, req.getJobType());
            req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
            try(RestSupport rest = new RestSupport(config)) {
                RestResponse<String> restResponse = rest.execute(rest.createHttpPost(SEGMENT_POST_URL), req);
                String newSegId = restResponse.getData();
                StreamingUtils.replayAuditlog();
                if (!StringUtils.isEmpty(newSegId)) {
                    NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
                    newSegment = dfMgr.getDataflow(dataflowId).getSegment(newSegId);
                    for (LayoutEntity layout : newSegment.getIndexPlan().getAllLayouts()) {
                        NDataLayout ly = NDataLayout.newDataLayout(newSegment.getDataflow(), newSegment.getId(),
                                layout.getId());
                        newSegment.getLayoutsMap().put(ly.getLayoutId(), ly);
                    }
                    return newSegment;
                } else {
                    val empSeg = NDataSegment.empty();
                    empSeg.setId(StringUtils.EMPTY);
                    return empSeg;
                }
            }
        }
    }

}
