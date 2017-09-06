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

package io.kyligence.kap.rest.request;

import java.util.List;
import java.util.Map;

public class VubeManageRequest {
    private long partitionDateStart;

    private List<Long> autoMergeTimeRanges;

    private int engineType;

    private long retentionRange;

    private Map<String, String> overrideKylinProperties;

    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public List<Long> getAutoMergeTimeRanges() {
        return autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(List<Long> autoMergeTimeRanges) {
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        this.engineType = engineType;
    }

    public long getRetentionRange() {
        return retentionRange;
    }

    public void setRetentionRange(long retentionRange) {
        this.retentionRange = retentionRange;
    }

    public Map<String, String> getOverrideKylinProperties() {
        return overrideKylinProperties;
    }

    public void setOverrideKylinProperties(Map<String, String> overrideKylinProperties) {
        this.overrideKylinProperties = overrideKylinProperties;
    }
}
