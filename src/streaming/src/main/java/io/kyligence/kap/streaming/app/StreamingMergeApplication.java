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
package io.kyligence.kap.streaming.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.JobTypeEnum;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StreamingMergeApplication extends StreamingApplication {
    @Getter
    @Setter
    protected long thresholdOfSegSize;
    @Getter
    @Setter
    protected Integer numberOfSeg;

    private final Map<String, Pair<String, Long>> removeSegIds = new HashMap<>();

    protected StreamingMergeApplication() {
        this.jobType = JobTypeEnum.STREAMING_MERGE;
    }

    public void parseParams(String[] args) {
        this.project = args[0];
        this.dataflowId = args[1];
        this.thresholdOfSegSize = StreamingUtils.parseSize(args[2]);
        this.numberOfSeg = Integer.parseInt(args[3]);
        this.distMetaUrl = args[4];
        this.jobId = StreamingUtils.getJobId(dataflowId, jobType.name());

        Preconditions.checkArgument(StringUtils.isNotEmpty(distMetaUrl), "distMetaUrl should not be empty!");
    }

    public void putHdfsFile(String segId, Pair<String, Long> item) {
        removeSegIds.put(segId, item);
    }

    public void clearHdfsFiles(NDataflow dataflow, AtomicLong startTime) {
        val hdfsFileScanStartTime = startTime.get();
        long now = System.currentTimeMillis();
        val intervals = KylinConfig.getInstanceFromEnv().getStreamingSegmentCleanInterval() * 60 * 60 * 1000;
        if (now - hdfsFileScanStartTime > intervals) {
            val iter = removeSegIds.keySet().iterator();
            while (iter.hasNext()) {
                String segId = iter.next();
                if (dataflow.getSegment(segId) != null) {
                    continue;
                }
                if ((now - removeSegIds.get(segId).getValue()) > intervals * 10) {
                    iter.remove();
                } else if ((now - removeSegIds.get(segId).getValue()) > intervals) {
                    try {
                        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                                new Path(removeSegIds.get(segId).getKey()));
                        iter.remove();
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                }
            }
            startTime.set(now);
        }
    }

}
