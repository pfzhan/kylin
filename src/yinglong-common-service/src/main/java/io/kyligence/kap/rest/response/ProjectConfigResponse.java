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

package io.kyligence.kap.rest.response;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProjectConfigResponse {

    @JsonProperty("project")
    private String project;
    @JsonProperty("description")
    private String description;
    @JsonProperty("default_database")
    private String defaultDatabase;
    @JsonProperty("semi_automatic_mode")
    private boolean semiAutomaticMode;

    @JsonProperty("storage_quota_size")
    private long storageQuotaSize;

    @JsonProperty("push_down_enabled")
    private boolean pushDownEnabled;
    @JsonProperty("scd2_enabled")
    private boolean scd2Enabled;
    @JsonProperty("snapshot_manual_management_enabled")
    private boolean snapshotManualManagementEnabled;
    @JsonProperty("second_storage_enabled")
    private Boolean secondStorageEnabled;
    @JsonProperty("second_storage_nodes")
    private List<SecondStorageNode> secondStorageNodes;
    @JsonProperty("runner_class_name")
    private String runnerClassName;
    @JsonProperty("converter_class_names")
    private String converterClassNames;

    @JsonProperty("auto_merge_enabled")
    private boolean autoMergeEnabled = true;
    @JsonProperty("auto_merge_time_ranges")
    private List<AutoMergeTimeEnum> autoMergeTimeRanges;
    @JsonProperty("volatile_range")
    private VolatileRange volatileRange;
    @JsonProperty("create_empty_segment_enabled")
    private boolean createEmptySegmentEnabled = false;

    @JsonProperty("retention_range")
    private RetentionRange retentionRange;

    @JsonProperty("job_error_notification_enabled")
    private boolean jobErrorNotificationEnabled;
    @JsonProperty("data_load_empty_notification_enabled")
    private boolean dataLoadEmptyNotificationEnabled;
    @JsonProperty("job_notification_emails")
    private List<String> jobNotificationEmails;

    @JsonProperty("threshold")
    private int favoriteQueryThreshold;
    @JsonProperty("tips_enabled")
    private boolean favoriteQueryTipsEnabled;

    @JsonProperty("frequency_time_window")
    private String frequencyTimeWindow = "MONTH";
    @JsonProperty("low_frequency_threshold")
    private long lowFrequencyThreshold;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @JsonProperty("expose_computed_column")
    private boolean exposeComputedColumn;

    @JsonProperty("kerberos_project_level_enabled")
    private boolean kerberosProjectLevelEnabled;

    @JsonProperty("principal")
    private String principal;

    @JsonProperty("favorite_rules")
    private Map<String, Object> favoriteRules;

    @JsonProperty("multi_partition_enabled")
    private boolean multiPartitionEnabled;

    @JsonProperty("query_history_download_max_size")
    private int queryHistoryDownloadMaxSize;

    @JsonProperty("jdbc_source_name")
    private String jdbcSourceName;
    @JsonProperty("jdbc_source_user")
    private String jdbcSourceUser;
    @JsonProperty("jdbc_source_pass")
    private String jdbcSourcePass;
    @JsonProperty("jdbc_source_connection_url")
    private String jdbcSourceConnectionUrl;
    @JsonProperty("jdbc_source_enable")
    private boolean jdbcSourceEnable;
    @JsonProperty("jdbc_source_driver")
    private String jdbcSourceDriver;

    public void setFrequencyTimeWindow(int frequencyTimeWindow) {
        switch (frequencyTimeWindow) {
        case 1:
            this.frequencyTimeWindow = "DAY";
            break;
        case 7:
            this.frequencyTimeWindow = "WEEK";
            break;
        case 30:
            this.frequencyTimeWindow = "MONTH";
            break;
        default:
            break;
        }

    }
}
