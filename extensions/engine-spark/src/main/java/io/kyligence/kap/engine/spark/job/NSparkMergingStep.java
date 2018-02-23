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

import java.util.Arrays;
import java.util.Set;

import io.kyligence.kap.engine.spark.builder.NDataflowMergeJob;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;

public class NSparkMergingStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    public NSparkMergingStep() {
        this.setSparkSubmitClassName(NDataflowMergeJob.class.getName());
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config).getDataflow(getDataflowName());
        return df.collectPrecalculationResource();
    }

    void setDataflowName(String dataflowName) {
        this.setParam(NBatchConstants.P_DATAFLOW_NAME, dataflowName);
    }

    void setJobId(String jobId) {
        this.setParam(NBatchConstants.P_JOB_ID, jobId);
    }

    public String getDataflowName() {
        return this.getParam(NBatchConstants.P_DATAFLOW_NAME);
    }

    void setSegmentId(int segmentId) {
        this.setParam(NBatchConstants.P_SEGMENT_IDS, String.valueOf(segmentId));
    }

    public int getSegmentIds() {
        return Integer.parseInt(this.getParam(NBatchConstants.P_SEGMENT_IDS));
    }

    void setCuboidLayoutIds(Set<Long> clIds) {
        this.setParam(NBatchConstants.P_CUBOID_LAYOUT_IDS, NSparkCubingUtil.ids2Str(clIds));
    }

    public Set<Long> getCuboidLayoutIds() {
        return NSparkCubingUtil.str2Longs(this.getParam(NBatchConstants.P_CUBOID_LAYOUT_IDS));
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(NSparkMergingStep.Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }
}
