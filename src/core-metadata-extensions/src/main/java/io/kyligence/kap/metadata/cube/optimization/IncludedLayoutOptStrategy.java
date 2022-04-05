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

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.utils.IndexPlanReduceUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * IncludedLayoutGcStrategy will search all garbage agg-layouts of the inputLayouts by default, but if
 * `kylin.garbage.remove-included-table-index` is true, it will also search garbage tableIndex-layouts.
 */
@Slf4j
public class IncludedLayoutOptStrategy extends AbstractOptStrategy {

    public IncludedLayoutOptStrategy() {
        this.setType(GarbageLayoutType.INCLUDED);
    }

    @Override
    public Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        Set<Long> garbageLayouts = Sets.newHashSet();
        val fromGarbageToAliveMap = IndexPlanReduceUtil.collectIncludedLayouts(inputLayouts, true);
        fromGarbageToAliveMap.forEach((redundant, reserved) -> garbageLayouts.add(redundant.getId()));
        shiftLayoutHitCount(fromGarbageToAliveMap, dataflow);
        if (needLog) {
            log.info("In dataflow({}), IncludedLayoutGcStrategy found garbage laoyouts: {}", dataflow.getId(),
                    fromGarbageToAliveMap);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isIncludedStrategyConsiderTableIndex()) {
            inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
    }

    /**
     * Put hit frequency of removed layouts to reserved layouts.
     */
    private void shiftLayoutHitCount(Map<LayoutEntity, LayoutEntity> removedToReservedMap, NDataflow dataflow) {
        Map<Long, FrequencyMap> layoutHitCount = dataflow.getLayoutHitCount();
        removedToReservedMap.forEach((removedLayout, reservedLayout) -> {
            FrequencyMap removedFreqMap = layoutHitCount.get(removedLayout.getId());
            if (removedFreqMap == null) {
                return;
            }
            layoutHitCount.putIfAbsent(reservedLayout.getId(), new FrequencyMap());
            layoutHitCount.get(reservedLayout.getId()).merge(removedFreqMap);
        });
        dataflow.setLayoutHitCount(layoutHitCount);
    }

}
