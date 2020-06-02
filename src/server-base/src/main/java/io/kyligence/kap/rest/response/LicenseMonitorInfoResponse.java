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
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.CapacityStatus;
import lombok.Data;

import java.io.Serializable;

@Data
public class LicenseMonitorInfoResponse implements Serializable {

    @JsonProperty("current_capacity")
    private long currentCapacity = 0L;

    @JsonProperty("capacity")
    private double capacity = 0L;

    @JsonProperty("capacity_status")
    private CapacityStatus capacityStatus = CapacityStatus.OK;

    @JsonProperty("check_time")
    private long checkTime;

    @JsonProperty("first_error_time")
    private long firstErrorTime;

    @JsonProperty("error_over_thirty_days")
    private boolean errorOverThirtyDays = false;

    @JsonProperty("error")
    private boolean error = false;

    @JsonProperty("evaluation")
    private boolean evaluation = false;
}
