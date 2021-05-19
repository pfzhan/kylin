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
package io.kyligence.kap.streaming.merge;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import lombok.val;
import lombok.var;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PeakMergePolicy extends MergePolicy {

    public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer, long thresholdOfSegSize,
            int numberOfSeg) {
        matchSegList.clear();
        int idx = findStartIndex(segList, thresholdOfSegSize);
        if (idx != -1) {
            var matched = false;
            for (int i = idx; i < segList.size(); i++) {
                matchSegList.add(segList.get(i));
                if (segList.get(i).getAdditionalInfo().getOrDefault(StreamingConstants.FILE_LAYER, "0")
                        .equals(String.valueOf(layer))) {
                    val currSegStorageSize = segList.get(i).getStorageBytesSize();
                    if (matchSegList.size() <= numberOfSeg
                            && isThresholdOfSegSizeOver(currSegStorageSize, thresholdOfSegSize)) {
                        matched = true;
                        break;
                    }
                } else if (matchSegList.size() > numberOfSeg) {
                    break;
                }
            }
            if (!matched) {
                matchSegList.clear();
            }
            return matchSegList;
        } else {
            return Collections.emptyList();
        }
    }

    public boolean matchMergeCondition(long thresholdOfSegSize) {
        return matchSegList.size() > 1 && isThresholdOfSegSizeOver(
                matchSegList.stream().mapToLong(NDataSegment::getStorageBytesSize).sum(), thresholdOfSegSize);
    }

    public void next(AtomicInteger currLayer) {
        currLayer.incrementAndGet();
    }

    public boolean isThresholdOfSegSizeOver(long totalSegSize, long thresholdOfSegSize) {
        return totalSegSize >= thresholdOfSegSize;
    }
}