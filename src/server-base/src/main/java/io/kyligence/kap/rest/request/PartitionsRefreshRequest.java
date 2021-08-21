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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import io.kyligence.kap.rest.aspect.TransactionProjectUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kylin.job.dao.ExecutablePO;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartitionsRefreshRequest implements ProjectInsensitiveRequest, TransactionProjectUnit {

    @JsonProperty("project")
    private String project;

    @JsonProperty("segment_id")
    private String segmentId;

    @JsonProperty("partition_ids")
    private Set<Long> partitionIds = Sets.newHashSet();

    @JsonProperty("sub_partition_values")
    private List<String[]> subPartitionValues = Lists.newArrayList();

    @JsonProperty("ignored_snapshot_tables")
    private Set<String> ignoredSnapshotTables;

    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @Override
    public String transactionProjectUnit() {
        return project;
    }
}
