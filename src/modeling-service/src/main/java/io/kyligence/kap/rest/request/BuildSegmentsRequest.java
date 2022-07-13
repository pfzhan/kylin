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

package io.kyligence.kap.rest.request;

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import lombok.Data;

@Data
public class BuildSegmentsRequest implements ProjectInsensitiveRequest {

    private String project;

    private String start;

    private String end;

    @JsonProperty("build_all_indexes")
    private boolean buildAllIndexes = true;

    @JsonProperty("ignored_snapshot_tables")
    private Set<String> ignoredSnapshotTables;

    @JsonProperty("sub_partition_values")
    private List<String[]> subPartitionValues;

    @JsonProperty("build_all_sub_partitions")
    private boolean buildAllSubPartitions = false;

    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @JsonProperty("partial_build")
    private boolean partialBuild=false;

    @JsonProperty("batch_index_ids")
    private List<Long> batchIndexIds;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @JsonProperty("tag")
    private Object tag;

}
