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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class RefreshSegmentParams extends BasicSegmentParams {
    private String[] segmentIds;
    private boolean refreshAllLayouts;
    private Set<Long> partitions;

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds) {
        super(project, modelId);
        this.segmentIds = segmentIds;
    }

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds, boolean refreshAllLayouts) {
        this(project, modelId, segmentIds, refreshAllLayouts, null);
    }

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds, boolean refreshAllLayouts,
            Set<Long> partitions) {
        this(project, modelId, segmentIds);
        this.refreshAllLayouts = refreshAllLayouts;
        this.partitions = partitions;
    }

    public RefreshSegmentParams withIgnoredSnapshotTables(Set<String> ignoredSnapshotTables) {
        this.ignoredSnapshotTables = ignoredSnapshotTables;
        return this;
    }

    public RefreshSegmentParams withPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public RefreshSegmentParams withPartialBuild(boolean partialBuild) {
        this.partialBuild = partialBuild;
        return this;
    }

    public RefreshSegmentParams withBatchIndexIds(List<Long> batchIndexIds) {
        this.batchIndexIds = batchIndexIds;
        return this;
    }

    public RefreshSegmentParams withYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
        return this;
    }

    public RefreshSegmentParams withTag(Object tag) {
        this.tag = tag;
        return this;
    }
}
