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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import org.apache.kylin.common.exception.KylinException;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class GarbageCleanUpConfigRequest {
    @JsonProperty("frequency_time_window")
    private FrequencyTimeWindowEnum frequencyTimeWindow;
    @JsonProperty("low_frequency_threshold")
    private Long lowFrequencyThreshold;

    public enum FrequencyTimeWindowEnum {
        DAY, WEEK, MONTH
    }

    public int getFrequencyTimeWindow() {
        if (frequencyTimeWindow == null) {
            throw new KylinException(INVALID_PARAMETER, "parameter 'frequency_time_window' is not set");
        }
        switch (frequencyTimeWindow) {
        case DAY:
            return 1;
        case WEEK:
            return 7;
        case MONTH:
            return 30;
        default:
            throw new KylinException(INVALID_PARAMETER, "Illegal parameter 'frequency_time_window'!");
        }
    }

}
