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

package io.kyligence.kap.engine.spark.model;

import java.util.Objects;
import java.util.Set;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;

public class MLPFlatTableDesc extends SegmentFlatTableDesc {

    private final Set<Long> partitionIds;
    private final String jobId;

    public MLPFlatTableDesc(KylinConfig config, NDataSegment dataSegment, NSpanningTree spanningTree,
                            Set<Long> partitionIds, String jobId) {
        super(config, dataSegment, spanningTree);
        Preconditions.checkNotNull(partitionIds);
        Preconditions.checkNotNull(jobId);
        this.partitionIds = partitionIds;
        this.jobId = jobId;
    }

    public Set<Long> getPartitionIDs() {
        return this.partitionIds;
    }

    @Override
    public Path getFlatTablePath() {
        return config.getJobTmpFlatTableDir(project, jobId);
    }

    @Override
    public Path getFactTableViewPath() {
        return config.getJobTmpFactTableViewDir(project, jobId);
    }

    @Override
    public boolean shouldPersistFlatTable() {
        return true;
    }

    @Override
    protected void initColumns() {
        // Ensure the partition columns were included.
        addPartitionColumns();
        // By design, only indexPlan columns would be included.
        if (isPartialBuild()) {
            addIndexPartialBuildColumns();
        } else {
            addIndexPlanColumns();
        }
    }

    private void addPartitionColumns() {
        dataModel.getMultiPartitionDesc() //
                .getColumnRefs() //
                .stream().filter(Objects::nonNull) //
                .forEach(this::addColumn);
    }
}
