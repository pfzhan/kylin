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

package io.kyligence.kap.clickhouse.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.secondstorage.SecondStorageConstants;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.P_OLD_SEGMENT_IDS;

@NotThreadSafe
public class ClickHouseRefresh extends ClickHouseLoad {
    private Map<String, String> newSegToOld = null;

    public ClickHouseRefresh() {
        this.setName(SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE);
    }

    public ClickHouseRefresh(Object notSetId) {
        super(notSetId);
    }

    private void initSegMap() {
        newSegToOld = new HashMap<>();
        String[] segmentIds = getParam(NBatchConstants.P_SEGMENT_IDS).split(",");
        String[] oldSegmentIds = getParam(P_OLD_SEGMENT_IDS).split(",");
        Preconditions.checkArgument(segmentIds.length == oldSegmentIds.length);
        for (int i = 0; i < segmentIds.length; i++) {
            newSegToOld.put(segmentIds[i], oldSegmentIds[i]);
        }
    }

    public Set<String> getOldSegmentIds() {
        if (getParam(P_OLD_SEGMENT_IDS) != null) {
            return Sets.newHashSet(org.apache.commons.lang3.StringUtils.split(getParam(P_OLD_SEGMENT_IDS), ","));
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected List<LoadInfo> preprocessLoadInfo(List<LoadInfo> infoList) {
        if (newSegToOld == null) {
            initSegMap();
        }
        infoList.forEach(info -> info.setOldSegmentId(newSegToOld.get(info.getSegmentId())));
        return infoList;
    }
}
