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

import io.kyligence.kap.engine.spark.job.BuildJobInfos;
import io.kyligence.kap.engine.spark.job.DFMergeJob;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.streaming.common.MergeJobEntry;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import io.kyligence.kap.streaming.metadata.BuildLayoutWithRestUpdate;
import lombok.val;
import org.apache.kylin.common.KylinConfig;

public class StreamingDFMergeJob extends DFMergeJob {

    private MergeJobEntry mergeJobEntry;

    public StreamingDFMergeJob() {
        buildLayoutWithUpdate = new BuildLayoutWithRestUpdate();
        this.config = KylinConfig.getInstanceFromEnv();
    }

    public void streamingMergeSegment(MergeJobEntry mergeJobEntry) throws IOException {
        this.mergeJobEntry = mergeJobEntry;
        this.ss = mergeJobEntry.spark();
        this.project = mergeJobEntry.project();
        ss.sparkContext().setLocalProperty("spark.sql.execution.id", null);
        val specifiedCuboids = mergeJobEntry.afterMergeSegment().getLayoutsMap().keySet();
        this.jobId = UUID.randomUUID().toString();
        if (this.infos == null) {
            this.infos = new BuildJobInfos();
        } else {
            this.infos.clear();
        }

        this.mergeSegments(mergeJobEntry.dataflowId(), mergeJobEntry.afterMergeSegment().getId(), specifiedCuboids);
    }

    public void shutdown() {
        buildLayoutWithUpdate.shutDown();
    }

    @Override
    protected List<NDataSegment> getMergingSegments(NDataflow dataflow, NDataSegment mergedSeg) {
        return mergeJobEntry.unMergedSegments();
    }
}
