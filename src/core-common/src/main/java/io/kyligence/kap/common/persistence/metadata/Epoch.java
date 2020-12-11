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

package io.kyligence.kap.common.persistence.metadata;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@SuppressWarnings("serial")
@AllArgsConstructor
@NoArgsConstructor
public class Epoch extends RootPersistentEntity {

    @JsonProperty("epoch_id")
    @Getter
    @Setter
    private long epochId;

    @JsonProperty("epoch_target")
    @Getter
    @Setter
    private String epochTarget;

    @JsonProperty("current_epoch_owner")
    @Getter
    @Setter
    private String currentEpochOwner;

    @JsonProperty("last_epoch_renew_time")
    @Getter
    @Setter
    private long lastEpochRenewTime;

    @JsonProperty("server_mode")
    @Getter
    @Setter
    private String serverMode;

    @JsonProperty("maintenance_mode_reason")
    @Getter
    @Setter
    private String maintenanceModeReason;

    @JsonProperty("mvcc")
    @Getter
    @Setter
    private long mvcc;

    @Override
    public String toString() {
        return "Epoch{" +
                "epochId=" + epochId +
                ", epochTarget='" + epochTarget + '\'' +
                ", currentEpochOwner='" + currentEpochOwner + '\'' +
                ", lastEpochRenewTime=" + lastEpochRenewTime +
                ", serverMode='" + serverMode + '\'' +
                ", maintenanceModeReason='" + maintenanceModeReason + '\'' +
                ", mvcc=" + mvcc +
                '}';
    }
}