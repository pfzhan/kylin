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

package io.kyligence.kap.modeling.auto.tuner.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class AbstractAggGroupRecorder {
    Map<List<String>, AggGroupRecord> aggGroupRecords;

    public AbstractAggGroupRecorder() {
        aggGroupRecords = Maps.newHashMap();
    }

    public void addRecord(List<String> dimensions, double score) {
        if (!aggGroupRecords.containsKey(dimensions)) {
            aggGroupRecords.put(dimensions, new AggGroupRecord(dimensions, score));
        } else {
            aggGroupRecords.get(dimensions).increaseScore(score);
        }
    }

    abstract protected void internalMerge(List<Map.Entry<List<String>, AggGroupRecord>> aggGroupEntries);

    public List<List<String>> getAggGroups() {
        List<List<String>> result = Lists.newArrayList();
        List<Map.Entry<List<String>, AggGroupRecord>> aggGroupEntries = Lists.newArrayList(aggGroupRecords.entrySet());
        Collections.sort(aggGroupEntries, Collections.reverseOrder(new Comparator<Map.Entry<List<String>, AggGroupRecord>>() {
            @Override
            public int compare(Map.Entry<List<String>, AggGroupRecord> o1, Map.Entry<List<String>, AggGroupRecord> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        }));

        internalMerge(aggGroupEntries);
        for (Map.Entry<List<String>, AggGroupRecord> entry : aggGroupEntries) {
            result.add(entry.getKey());
        }
        return result;
    }

    public int size() {
        return aggGroupRecords.size();
    }
}
