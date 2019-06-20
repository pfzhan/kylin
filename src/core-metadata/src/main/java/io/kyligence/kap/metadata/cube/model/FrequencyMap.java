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
package io.kyligence.kap.metadata.cube.model;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FrequencyMap implements Serializable {

    @JsonIgnore
    private NavigableMap<Long, Integer> dateFrequency = new TreeMap<>();

    @JsonAnySetter
    public void add(String key, Integer value) {
        dateFrequency.put(Long.parseLong(key), value);
    }

    @JsonAnyGetter
    public Map<Long, Integer> getMap() {
        return dateFrequency;
    }

    public void incFrequency(long time) {
        long date = getDateInMillis(time);
        Integer freq = dateFrequency.get(date);
        if (freq != null) {
            freq++;
            dateFrequency.put(date, freq);
        } else {
            dateFrequency.put(date, 1);
        }
    }

    public FrequencyMap merge(FrequencyMap other) {
        other.getDateFrequency().forEach((k, v) -> this.dateFrequency.merge(k, v, Integer::sum));
        return this;
    }

    public void rotate(long endTime, String project) {
        long frequencyInitialDate = getDateInMillis(endTime) - getDateBeforeFrequencyTimeWindow(project);

        while (dateFrequency.size() != 0) {
            if (frequencyInitialDate <= dateFrequency.firstKey())
                break;
            dateFrequency.pollFirstEntry();
        }
    }

    @JsonIgnore
    public int getFrequency(String project) {
        long frequencyInitialCollectDate = getDateBeforeFrequencyTimeWindow(project);
        return dateFrequency.subMap(frequencyInitialCollectDate, Long.MAX_VALUE).values().stream().reduce(Integer::sum)
                .orElse(0);
    }

    @JsonIgnore
    public boolean isLowFrequency(String project) {
        int frequency = getFrequency(project);
        val prjMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        return frequency < prjMgr.getProject(project).getConfig().getFavoriteQueryLowFrequency();
    }

    private long getDateBeforeFrequencyTimeWindow(String project) {
        val prjMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        long daysInMillis = prjMgr.getProject(project).getConfig().getFavoriteQueryFrequencyTimeWindow();
        return getDateInMillis(System.currentTimeMillis()) - daysInMillis;
    }

    private long getDateInMillis(final long queryTime) {
        return TimeUtil.getDayStart(queryTime);
    }

}
