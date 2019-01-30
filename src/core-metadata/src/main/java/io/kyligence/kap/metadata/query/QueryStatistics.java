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

package io.kyligence.kap.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
public class QueryStatistics implements IKeep {
    @JsonProperty("engine_type")
    @Column(name = "engine_type", tag = true)
    private String engineType;

    @JsonProperty("count")
    @Column(name = "count")
    private int count;

    @JsonProperty("ratio")
    private double ratio;

    @JsonProperty("mean")
    @Column(name = "mean")
    private double meanDuration;

    @JsonProperty("model")
    @Column(name = "model", tag = true)
    private String model;

    @JsonProperty("time")
    @Column(name = "time")
    private Instant time;

    @JsonProperty("month")
    @Column(name = "month", tag = true)
    private String month;

    public QueryStatistics(String engineType) {
        this.engineType = engineType;
    }

    public void apply(final QueryStatistics other) {
        this.count = other.count;
        this.ratio = other.ratio;
        this.meanDuration = other.meanDuration;
    }

    public void updateRatio(double amount) {
        if (amount > 0d) {
            // Keep two decimals
            this.ratio = ((double) Math.round(((double) count) / amount * 100d)) / 100d;
        }
    }
}
