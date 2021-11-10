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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree;

public class PartitionFlatTableDesc extends SegmentFlatTableDesc {

    private final String jobId;
    private final List<Long> partitions;

    public PartitionFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree, //
                                  String jobId, List<Long> partitions) {
        super(config, dataSegment, spanningTree);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(partitions);
        this.jobId = jobId;
        this.partitions = Collections.unmodifiableList(partitions);
    }

    public PartitionFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree, //
            List<String> relatedTables, String jobId, List<Long> partitions) {
        super(config, dataSegment, spanningTree, relatedTables);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(partitions);
        this.jobId = jobId;
        this.partitions = Collections.unmodifiableList(partitions);
    }

    public List<Long> getPartitions() {
        return this.partitions;
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