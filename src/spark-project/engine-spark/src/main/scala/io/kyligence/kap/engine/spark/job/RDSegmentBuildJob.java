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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;

import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class RDSegmentBuildJob extends SegmentJob {

    @Override
    protected final void doExecute() throws Exception {
        writeCountDistinct();
        if (isPartitioned()) {
            detectPartition();
        } else {
            detect();
        }
    }

    private void detectPartition() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            RDSegmentBuildExec exec = new RDPartitionBuildExec(this, dataSegment);
            exec.detectResource();
        }
    }

    private void detect() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            RDSegmentBuildExec exec = new RDSegmentBuildExec(this, dataSegment);
            exec.detectResource();
        }
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeCubingJobInfo();
    }

    private void writeCountDistinct() {
        ResourceDetectUtils.write(new Path(rdSharedPath, ResourceDetectUtils.countDistinctSuffix()), //
                ResourceDetectUtils.findCountDistinctMeasure(readOnlyLayouts));
    }

    public static void main(String[] args) {
        RDSegmentBuildJob segmentBuildJob = new RDSegmentBuildJob();
        segmentBuildJob.execute(args);
    }
}
