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

package org.apache.kylin.job.dao;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class JobStatistics extends JobStatisticsBasic {

    @JsonProperty("date")
    private long date;
    @JsonProperty("model_stats")
    private Map<String, JobStatisticsBasic> jobStatisticsByModels = Maps.newHashMap();

    @Override
    public String resourceName() {
        return String.valueOf(date);
    }

    public JobStatistics(long date, String model, long duration, long byteSize) {
        this.date = date;
        setCount(1);
        setTotalDuration(duration);
        setTotalByteSize(byteSize);
        jobStatisticsByModels.put(model, new JobStatisticsBasic(duration, byteSize));
    }

    public JobStatistics(int count, long totalDuration, long totalByteSize) {
        setCount(count);
        setTotalDuration(totalDuration);
        setTotalByteSize(totalByteSize);
    }

    public void update(String model, long duration, long byteSize) {
        super.update(duration, byteSize);
        JobStatisticsBasic jobStatisticsByModel = jobStatisticsByModels.get(model);
        if (jobStatisticsByModel == null) {
            jobStatisticsByModel = new JobStatisticsBasic(duration, byteSize);
        } else {
            jobStatisticsByModel.update(duration, byteSize);
        }

        jobStatisticsByModels.put(model, jobStatisticsByModel);
    }
}
