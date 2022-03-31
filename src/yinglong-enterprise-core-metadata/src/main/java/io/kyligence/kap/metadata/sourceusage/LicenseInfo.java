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
package io.kyligence.kap.metadata.sourceusage;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.CapacityStatus;
import lombok.Data;

@Data
public class LicenseInfo implements Serializable {
    @JsonProperty("current_node")
    private int currentNode = 0;

    @JsonProperty("node")
    private int node = 0;

    @JsonProperty("node_status")
    private CapacityStatus nodeStatus = CapacityStatus.OK;

    @JsonProperty("current_capacity")
    private long currentCapacity = 0L;

    @JsonProperty("capacity")
    private long capacity = 0L;

    @JsonProperty("capacity_status")
    private CapacityStatus capacityStatus = CapacityStatus.OK;

    @JsonProperty("time")
    private long time;

    @JsonProperty("first_error_time")
    private long firstErrorTime;
}
