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

package io.kyligence.kap.metadata.cube.garbage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;

public class LowFreqLayoutGcStrategy implements IGarbageCleanerStrategy {

    @Override
    public Set<Long> collectGarbageLayouts(NDataflow dataflow) {
        ProjectInstance projectInstance = NProjectManager.getInstance(dataflow.getConfig())
                .getProject(dataflow.getProject());
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();
        long frequencyTimeWindow = projectInstance.getConfig().getFrequencyTimeWindowByTs();
        Set<Long> garbageLayouts = Sets.newHashSet();
        List<LayoutEntity> waitHandlingLayouts = projectInstance.isExpertMode() //
                ? dataflow.getIndexPlan().getWhitelistLayouts()
                : dataflow.getIndexPlan().getAllLayouts();
        waitHandlingLayouts.forEach(layout -> {
            if (System.currentTimeMillis() - layout.getUpdateTime() >= frequencyTimeWindow) {
                FrequencyMap frequencyMap = hitFrequencyMap.get(layout.getId());
                if (frequencyMap == null || frequencyMap.isLowFrequency(dataflow.getProject())) {
                    garbageLayouts.add(layout.getId());
                }
            }
        });
        return garbageLayouts;
    }
}
