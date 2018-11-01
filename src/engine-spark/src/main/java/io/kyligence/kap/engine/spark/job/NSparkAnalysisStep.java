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

import com.google.common.collect.Sets;
import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.builder.NModelAnalysisJob;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;

import java.util.Set;

public class NSparkAnalysisStep extends NSparkExecutable {

    public NSparkAnalysisStep() {
        this.setSparkSubmitClassName(NModelAnalysisJob.class.getName());
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowName());
        dumpList.addAll(df.collectPrecalculationResource());

        // dump table ext
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        for (final TableRef tableRef : df.getModel().getAllTables()) {
            final TableExtDesc tableExtDesc = tableMetadataManager.getTableExtIfExists(tableRef.getTableDesc());
            if (tableExtDesc == null) {
                continue;
            }
            dumpList.add(tableExtDesc.getResourcePath());
        }
        return dumpList;
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

    void setSegmentIds(Set<Integer> segmentIds) {
        this.setParam(NBatchConstants.P_SEGMENT_IDS, NSparkCubingUtil.ids2Str(segmentIds));
    }

    public Set<Integer> getSegmentIds() {
        return NSparkCubingUtil.str2Ints(this.getParam(NBatchConstants.P_SEGMENT_IDS));
    }

    void setCuboidLayoutIds(Set<Long> clIds) {
        this.setParam(NBatchConstants.P_CUBOID_LAYOUT_IDS, NSparkCubingUtil.ids2Str(clIds));
    }

    public Set<Long> getCuboidLayoutIds() {
        return NSparkCubingUtil.str2Longs(this.getParam(NBatchConstants.P_CUBOID_LAYOUT_IDS));
    }

}
