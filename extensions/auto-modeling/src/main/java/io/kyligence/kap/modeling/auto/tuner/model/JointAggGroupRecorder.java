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
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Maps;

public class JointAggGroupRecorder extends AbstractAggGroupRecorder {
    @Override
    protected void internalMerge(List<Entry<List<String>, AggGroupRecord>> aggGroupEntries) {
        Map<List<String>, AggGroupRecord> resultMap = Maps.newHashMap();
        for (Entry<List<String>, AggGroupRecord> entry : aggGroupEntries) {
            //            boolean foundInteraction = false;
            //            for (Entry<List<String>, AggGroupRecord> resultEntry : resultMap.entrySet()) {
            //                if (CollectionUtils.containsAny(resultEntry.getKey(), entry.getKey())) {
            //                    foundInteraction = true;
            //                    List<String> resultKey = resultEntry.getKey();
            //                    if (resultKey.size() <= Constants.DIM_AGG_GROUP_JOINT_ELEMENTS_MAX) {
            //                        List<String> newResultKey = (List<String>) CollectionUtils.union(resultKey, entry.getKey());
            //                        resultKey.clear();
            //                        resultKey.addAll(newResultKey);
            //                        resultEntry.getValue().merge(entry.getValue());
            //                        break;
            //                    }
            //                }
            //            }
            //
            //            if (!foundInteraction) {
            //                resultMap.put(entry.getKey(), entry.getValue());
            //            }
            boolean foundInteraction = false;
            for (Entry<List<String>, AggGroupRecord> resultEntry : resultMap.entrySet()) {
                if (CollectionUtils.containsAny(resultEntry.getKey(), entry.getKey())) {
                    foundInteraction = true;
                }
            }
            if (!foundInteraction) {
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }

        aggGroupEntries.clear();
        aggGroupEntries.addAll(resultMap.entrySet());
    }
}
