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

package io.kyligence.kap.metadata.scheduler;

import java.util.Objects;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SchedulerJobInstance extends RootPersistentEntity implements Comparable<SchedulerJobInstance> {
    public static final String SCHEDULER_RESOURCE_ROOT = "/scheduler";

    @JsonProperty("name")
    private String name;

    @JsonProperty("project")
    private String project;

    @JsonProperty("related_cube")
    private String relatedCube;

    @JsonProperty("related_cube_uuid")
    private String relatedCubeUuid;

    @JsonProperty("partition_start_time")
    private long partitionStartTime;

    @JsonProperty("scheduled_run_time")
    private long scheduledRunTime;

    @JsonProperty("repeat_count")
    private long repeatCount;

    @JsonProperty("cur_repeat_count")
    private long curRepeatCount;

    @JsonProperty("repeat_interval")
    private long repeatInterval;

    @JsonProperty("partition_interval")
    private long partitionInterval;

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String schedulerJobName) {
        return SCHEDULER_RESOURCE_ROOT + "/" + schedulerJobName + ".json";
    }

    public SchedulerJobInstance(String name, String project, String relatedCube, long partitionStartTime, long scheduledRunTime, long repeatCount, long curRepeatCount, long repeatInterval, long partitionInterval) {
        this.name = name;
        this.project = project;
        this.relatedCube = relatedCube;
        this.partitionStartTime = partitionStartTime;
        this.scheduledRunTime = scheduledRunTime;
        this.repeatCount = repeatCount;
        this.curRepeatCount = curRepeatCount;
        this.repeatInterval = repeatInterval;
        this.partitionInterval = partitionInterval;
    }

    public SchedulerJobInstance() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getRelatedCube() {
        return relatedCube;
    }

    public void setRelatedCube(String cube) {
        this.relatedCube = cube;
    }

    public void setRelatedCubeUuid(String uuid) {
        this.relatedCubeUuid = uuid;
    }

    public String getRelatedCubeUuid() {
        return this.relatedCubeUuid;
    }

    public long getPartitionStartTime() {
        return partitionStartTime;
    }

    public void setPartitionStartTime(long partitionStartTime) {
        this.partitionStartTime = partitionStartTime;
    }

    public long getScheduledRunTime() {
        return scheduledRunTime;
    }

    public void setScheduledRunTime(long scheduledRunTime) {
        this.scheduledRunTime = scheduledRunTime;
    }

    public long getCurRepeatCount() {
        return curRepeatCount;
    }

    public void setCurRepeatCount(long curRepeatCount) {
        this.curRepeatCount = curRepeatCount;
    }

    public long getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(long repeatCount) {
        this.repeatCount = repeatCount;
    }

    public long getRepeatInterval() {
        return repeatInterval;
    }

    public void setRepeatInterval(long repeatInterval) {
        this.repeatInterval = repeatInterval;
    }

    public long getPartitionInterval() {
        return partitionInterval;
    }

    public void setPartitionInterval(long partitionInterval) {
        this.partitionInterval = partitionInterval;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, project, relatedCube);
    }

    @Override
    public String toString() {
        return "SchedulerJobnstance{" + "name='" + name + '\'' + ", project=" + project + ", relatedCube=" + relatedCube + '\'' + '}';
    }

    @Override
    public int compareTo(SchedulerJobInstance o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }
}