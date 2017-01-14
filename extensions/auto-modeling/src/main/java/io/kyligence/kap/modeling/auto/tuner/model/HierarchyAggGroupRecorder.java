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

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HierarchyAggGroupRecorder extends AbstractAggGroupRecorder {
    @Override
    protected void internalMerge(List<Map.Entry<List<String>, AggGroupRecord>> aggGroupEntries) {
        Map<List<String>, AggGroupRecord> resultMap = Maps.newHashMap();
        outer: for (Map.Entry<List<String>, AggGroupRecord> entry : aggGroupEntries) {
            List<String> intersectKey = null;
            for (Map.Entry<List<String>, AggGroupRecord> resultEntry : resultMap.entrySet()) {
                if (CollectionUtils.containsAny(resultEntry.getKey(), entry.getKey())) {
                    if (intersectKey != null) {
                        continue outer;
                    } else {
                        intersectKey = resultEntry.getKey();
                    }
                }
            }

            if (intersectKey == null) {
                resultMap.put(entry.getKey(), entry.getValue());
            } else {
                // find hierarchy chain
                if (CollectionUtils.intersection(intersectKey, entry.getKey()).size() == 1) {
                    List<String> newResultKey = Lists.newArrayList();
                    if (intersectKey.get(intersectKey.size() - 1).equals(entry.getKey().get(0))) {
                        newResultKey.addAll(intersectKey);
                        newResultKey.remove(newResultKey.size() - 1);
                        newResultKey.addAll(entry.getKey());
                    } else if (intersectKey.get(0).equals(entry.getKey().get(entry.getKey().size() - 1))) {
                        newResultKey.addAll(entry.getKey());
                        newResultKey.remove(newResultKey.size() - 1);
                        newResultKey.addAll(intersectKey);
                    }

                    if (!newResultKey.isEmpty()) {
                        intersectKey.clear();
                        intersectKey.addAll(newResultKey);
                    }
                }
            }
        }
        aggGroupEntries.clear();
        aggGroupEntries.addAll(resultMap.entrySet());
    }
}
