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

package org.apache.kylin.job;

import java.util.Objects;
import java.util.Set;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class SecondStorageCleanJobBuildParams extends JobFactory.JobBuildParams {
    private String project;
    private String modelId;
    private String dataflowId;
    private Set<Long> secondStorageDeleteLayoutIds;

    public SecondStorageCleanJobBuildParams(Set<NDataSegment> segments, JobParam jobParam, JobTypeEnum jobType) {
        super(segments,
                jobParam.getProcessLayouts(),
                jobParam.getOwner(),
                jobType,
                jobParam.getJobId(),
                null,
                jobParam.getIgnoredSnapshotTables(),
                null,
                null);
    }

    public String getProject() {
        return project;
    }

    public SecondStorageCleanJobBuildParams setProject(String project) {
        this.project = project;
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    public SecondStorageCleanJobBuildParams setModelId(String modelId) {
        this.modelId = modelId;
        return this;
    }

    public String getDataflowId() {
        return dataflowId;
    }

    public SecondStorageCleanJobBuildParams setDataflowId(String dataflowId) {
        this.dataflowId = dataflowId;
        return this;
    }

    public void setSecondStorageDeleteLayoutIds(Set<Long> secondStorageDeleteLayoutIds) {
        if (Objects.nonNull(secondStorageDeleteLayoutIds)) {
            this.secondStorageDeleteLayoutIds = secondStorageDeleteLayoutIds;
        }
    }

    public Set<Long> getSecondStorageDeleteLayoutIds() {
        return this.secondStorageDeleteLayoutIds;
    }
}
