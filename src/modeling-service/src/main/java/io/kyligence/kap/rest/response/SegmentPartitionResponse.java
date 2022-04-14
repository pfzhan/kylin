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

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnumToDisplay;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SegmentPartitionResponse {
    @JsonProperty("id")
    private long partitionId;

    @JsonProperty("values")
    private String[] values;

    @JsonProperty("status")
    private PartitionStatusEnumToDisplay status;

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

    @JsonProperty("source_count")
    private long sourceCount;

    @JsonProperty("bytes_size")
    private long bytesSize;

    public SegmentPartitionResponse(long id, String[] values, PartitionStatusEnum status, // 
            long lastModifiedTime, long sourceCount, long bytesSize) {
        this.partitionId = id;
        this.values = values;
        this.lastModifiedTime = lastModifiedTime;
        this.sourceCount = sourceCount;
        this.bytesSize = bytesSize;
        setPartitionStatusToDisplay(status);
    }

    private void setPartitionStatusToDisplay(PartitionStatusEnum status) {
        if (PartitionStatusEnum.NEW == status) {
            this.status = PartitionStatusEnumToDisplay.LOADING;
        }

        if (PartitionStatusEnum.REFRESH == status) {
            this.status = PartitionStatusEnumToDisplay.REFRESHING;
        }

        if (PartitionStatusEnum.READY == status) {
            this.status = PartitionStatusEnumToDisplay.ONLINE;
        }
    }
}
