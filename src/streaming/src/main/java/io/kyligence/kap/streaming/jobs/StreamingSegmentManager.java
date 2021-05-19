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

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;

import java.util.UUID;

import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.request.SegmentMergeRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.OffsetRangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSegmentManager {

    private static final Logger logger = LoggerFactory.getLogger(StreamingSegmentManager.class);

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
        NDataSegment newSegment = null;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (config.isUTEnv()) {
            val segRange = sr;
            newSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                return dfMgr.appendSegmentForStreaming(dfMgr.getDataflow(dataflowId), segRange);
            }, project);
            return newSegment;
        } else {
            RestSupport rest = new RestSupport(config);

            String url = "/streaming_jobs/dataflow/segment";
            SegmentMergeRequest req = new SegmentMergeRequest(project, dataflowId);
            req.setSegmentRange(sr);
            req.setNewSegId(UUID.randomUUID().toString());
            try {
                RestResponse<String> restResponse = rest.execute(rest.createHttpPost(url), req);
                String newSegId = restResponse.getData();
                StreamingUtils.replayAuditlog();
                if (!StringUtils.isEmpty(newSegId)) {
                    NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
                    final NDataSegment newSeg = dfMgr.getDataflow(dataflowId).getSegment(newSegId);
                    for (LayoutEntity layout : newSeg.getIndexPlan().getAllLayouts()) {
                        NDataLayout ly = NDataLayout.newDataLayout(newSeg.getDataflow(), newSeg.getId(),
                                layout.getId());
                        newSeg.getLayoutsMap().put(ly.getLayoutId(), ly);
                    }
                    return newSeg;
                } else {
                    val empSeg = new NDataSegment();
                    empSeg.setId(StringUtils.EMPTY);
                    return empSeg;
                }
            } finally {
                rest.close();
            }
        }
        //    retainSegments.add(newSegment);
    }

}
