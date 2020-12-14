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
package io.kyligence.kap.rest.service.params;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.PartitionDesc;

import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class IncrementBuildSegmentParams extends FullBuildSegmentParams {
    private String start;
    private String end;
    private PartitionDesc partitionDesc;
    private MultiPartitionDesc multiPartitionDesc;
    private List<SegmentTimeRequest> segmentHoles;
    private String partitionColFormat;
    private List<String[]> multiPartitionValues;

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            String partitionColFormat, boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionColFormat = partitionColFormat;
        this.multiPartitionValues = multiPartitionValues;
    }

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, MultiPartitionDesc multiPartitionDesc, List<SegmentTimeRequest> segmentHoles,
            boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionDesc = partitionDesc;
        this.segmentHoles = segmentHoles;
        this.multiPartitionValues = multiPartitionValues;
        this.multiPartitionDesc = multiPartitionDesc;
    }

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, MultiPartitionDesc multiPartitionDesc, String partitionColFormat,
            List<SegmentTimeRequest> segmentHoles, boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionDesc = partitionDesc;
        this.segmentHoles = segmentHoles;
        this.partitionColFormat = partitionColFormat;
        this.multiPartitionValues = multiPartitionValues;
        this.multiPartitionDesc = multiPartitionDesc;
    }

    @Override
    public IncrementBuildSegmentParams withIgnoredSnapshotTables(Set<String> ignoredSnapshotTables) {
        this.ignoredSnapshotTables = ignoredSnapshotTables;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withPriority(int priority) {
        this.priority = priority;
        return this;
    }
}
