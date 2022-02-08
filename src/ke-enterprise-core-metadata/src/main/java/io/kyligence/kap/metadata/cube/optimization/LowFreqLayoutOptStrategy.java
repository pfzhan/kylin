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

package io.kyligence.kap.metadata.cube.optimization;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

/**
 * LowFreqLayoutGcStrategy will search all garbage layout in inputLayouts.
 */
@Slf4j
public class LowFreqLayoutOptStrategy extends AbstractOptStrategy {

    public LowFreqLayoutOptStrategy() {
        this.setType(GarbageLayoutType.LOW_FREQUENCY);
    }

    @Override
    public Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        ProjectInstance projectInstance = NProjectManager.getInstance(dataflow.getConfig())
                .getProject(dataflow.getProject());
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();
        int days = projectInstance.getConfig().getFrequencyTimeWindowInDays();
        Set<Long> garbageLayouts = Sets.newHashSet();
        inputLayouts.forEach(layout -> {
            if (TimeUtil.minusDays(System.currentTimeMillis(), days) >= layout.getUpdateTime()) {
                FrequencyMap frequencyMap = hitFrequencyMap.get(layout.getId());
                if (frequencyMap == null) {
                    frequencyMap = new FrequencyMap();
                }

                if (frequencyMap.isLowFrequency(dataflow.getProject())) {
                    garbageLayouts.add(layout.getId());
                }
            }
        });

        if (needLog) {
            log.info("In dataflow({}), LowFreqLayoutGcStrategy found garbageLayouts: {}", dataflow.getId(),
                    garbageLayouts);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isLowFreqStrategyConsiderTableIndex()) {
            inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
    }
}
