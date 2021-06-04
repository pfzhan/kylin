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

package io.kyligence.kap.streaming.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;

import java.util.Locale;
import java.util.Map;

@Setter
@Getter
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class StreamingJobMeta extends RootPersistentEntity {

    public static StreamingJobMeta create(NDataModel model, JobStatusEnum status, JobTypeEnum jobType) {
        StreamingJobMeta meta = new StreamingJobMeta();
        meta.setCreateTime(System.currentTimeMillis());
        meta.setCurrentStatus(status);
        meta.setJobType(jobType);
        meta.setModelId(model.getUuid());
        meta.setModelName(model.getAlias());
        meta.setFactTableName(model.getRootFactTableName());
        meta.setTopicName(model.getRootFactTable().getTableDesc().getKafkaConfig().getSubscribe());
        meta.setOwner(model.getOwner());
        meta.setUuid(StreamingUtils.getJobId(model.getUuid(), jobType.name()));
        initJobParams(meta, jobType);
        return meta;
    }

    private static void initJobParams(StreamingJobMeta jobMeta, JobTypeEnum jobType) {
        jobMeta.params.put(StreamingConstants.SPARK_MASTER, StreamingConstants.SPARK_MASTER_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_DRIVER_MEM, StreamingConstants.SPARK_DRIVER_MEM_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_INSTANCES,
                StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_CORES, StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_MEM, StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_SHUFFLE_PARTITIONS,
                StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT);

        if (JobTypeEnum.STREAMING_BUILD == jobType) {
            jobMeta.params.put(StreamingConstants.STREAMING_DURATION, StreamingConstants.STREAMING_DURATION_DEFAULT);
            jobMeta.params.put(StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION,
                    StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION_DEFAULT);
        } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
            jobMeta.params.put(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE,
                    StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT);
            jobMeta.params.put(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD,
                    StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT);
        }
        jobMeta.params.put(StreamingConstants.STREAMING_RETRY_ENABLE, "false");
    }

    @JsonProperty("model_alias")
    private String modelName;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("last_start_time")
    private String lastStartTime;

    @JsonProperty("last_end_time")
    private String lastEndTime;

    @JsonProperty("last_update_time")
    private String lastUpdateTime;

    @JsonProperty("last_batch_count")
    private Integer lastBatchCount;

    @JsonProperty("subscribe")
    private String topicName;

    @JsonProperty("fact_table")
    private String factTableName;

    @JsonProperty("job_status")
    private JobStatusEnum currentStatus;

    @JsonProperty("job_type")
    private JobTypeEnum jobType;

    @JsonProperty("process_id")
    private String processId;

    @JsonProperty("node_info")
    private String nodeInfo;

    @JsonProperty("yarn_app_id")
    private String yarnAppId;

    @JsonProperty("yarn_app_url")
    private String yarnAppUrl;

    @JsonProperty("params")
    private Map<String, String> params = Maps.newHashMap();

    private String project;

    @JsonProperty("skip_listener")
    private boolean skipListener;

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project, jobType.name());
    }

    public static String concatResourcePath(String name, String project, String jobType) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.STREAMING_RESOURCE_ROOT).append("/")
                .append(name).append("_" + jobType.toLowerCase(Locale.ROOT)).toString();
    }
}
