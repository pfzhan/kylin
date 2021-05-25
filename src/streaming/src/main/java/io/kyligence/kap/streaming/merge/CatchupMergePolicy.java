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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CatchupMergePolicy extends MergePolicy {

    private boolean segSizeMatched = false;

    public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer, long thresholdOfSegSize,
            int numberOfSeg) {
        matchSegList.clear();
        segSizeMatched = false;
        int maxNumOfSeg = numberOfSeg * 5;
        int idx = findStartIndex(segList, thresholdOfSegSize);
        if (idx != -1) {
            List<NDataSegment> layerSegList = segList
                    .subList(idx, segList.size()).stream().filter(item -> item.getAdditionalInfo()
                            .getOrDefault(StreamingConstants.FILE_LAYER, "0").equals(String.valueOf(layer)))
                    .collect(Collectors.toList());
            if (layerSegList.size() >= maxNumOfSeg) {
                segSizeMatched = true;
                long totalThresholdOfSegSize = 0;
                for (int i = idx; i < segList.size(); i++) {
                    matchSegList.add(segList.get(i));
                    totalThresholdOfSegSize += segList.get(i).getStorageBytesSize();
                    if (isThresholdOfSegSizeOver(totalThresholdOfSegSize, thresholdOfSegSize)) {
                        break;
                    }
                }
                return matchSegList;
            } else {
                return Collections.emptyList();
            }
        } else {
            return Collections.emptyList();
        }
    }

    public boolean matchMergeCondition(long thresholdOfSegSize) {
        return segSizeMatched || (matchSegList.size() > 1 && isThresholdOfSegSizeOver(
                matchSegList.stream().mapToLong(NDataSegment::getStorageBytesSize).sum(), thresholdOfSegSize));
    }
}
