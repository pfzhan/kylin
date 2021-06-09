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
import org.apache.kylin.common.KylinConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MergePolicy {
    protected List<NDataSegment> matchSegList = new ArrayList<>();
    private double mergeRatio = KylinConfig.getInstanceFromEnv().getStreamingSegmentMergeRatio();

    protected int findStartIndex(List<NDataSegment> segList, Long thresholdOfSegSize) {
        for (int i = 0; i < segList.size(); i++) {
            if (segList.get(i).getStorageBytesSize() > thresholdOfSegSize) {
                continue;
            } else {
                return i;
            }
        }
        return -1;
    }

    public void next(AtomicInteger currLayer) {

    }

    public abstract List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList,
                                                            int layer,
                                                            long thresholdOfSegSize,
                                                            int numOfSeg);

    public abstract boolean matchMergeCondition(long thresholdOfSegSize);

    public boolean isThresholdOfSegSizeOver(long totalSegSize, long thresholdOfSegSize) {
        return totalSegSize >= thresholdOfSegSize * mergeRatio;
    }

    public List<NDataSegment> getMatchSegList() {
        return matchSegList;
    }
}