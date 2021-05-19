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

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.common.MergeJobEntry;
import io.kyligence.kap.streaming.request.SegmentMergeRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SyncMerger{
    private static final Logger logger = LoggerFactory
            .getLogger(SyncMerger.class);

    private MergeJobEntry mergeJobEntry;

    public SyncMerger(MergeJobEntry mergeJobEntry) {
        this.mergeJobEntry = mergeJobEntry;
    }

    public void run(StreamingDFMergeJob merger) {
        logger.info("start merge streaming segment");
        logger.info(mergeJobEntry.toString());

        val start = System.currentTimeMillis();
        try {
            merger.streamingMergeSegment(mergeJobEntry);
            logger.info("merge segment cost {}", System.currentTimeMillis() - start);
            logger.info("delete merged segment and change the status");
            mergeJobEntry.globalMergeTime().set(System.currentTimeMillis() - start);

            val config = KylinConfig.getInstanceFromEnv();
            if(config.isUTEnv()) {
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
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
            } else {
                RestSupport rest = new RestSupport(config);
                String url = "/streaming_jobs/dataflow/segment";
                SegmentMergeRequest req = new SegmentMergeRequest(mergeJobEntry.project(), mergeJobEntry.dataflowId());
                req.setRemoveSegment(mergeJobEntry.unMergedSegments());
                req.setNewSegId(mergeJobEntry.afterMergeSegment().getId());
                try{
                    rest.execute(rest.createHttpPut(url), req);
                }finally {
                    rest.close();
                }
                StreamingUtils.replayAuditlog();
            }
        } catch (Exception e) {
            logger.info("merge failed reason: {} stackTrace is: {}", e.toString(), e.getStackTrace());
            val config = KylinConfig.getInstanceFromEnv();
            if(!config.isUTEnv()) {
                RestSupport rest = new RestSupport(config);
                String url = "/streaming_jobs/dataflow/segment/deletion";
                SegmentMergeRequest req = new SegmentMergeRequest(mergeJobEntry.project(), mergeJobEntry.dataflowId());
                req.setRemoveSegment(mergeJobEntry.unMergedSegments());
                try {
                    rest.execute(rest.createHttpPost(url), req);
                } finally {
                    rest.close();
                }
                StreamingUtils.replayAuditlog();
            }
            throw new KylinException(ServerErrorCode.SEGMENT_MERGE_FAILURE, mergeJobEntry.afterMergeSegment().getId());
        }
    }

}
