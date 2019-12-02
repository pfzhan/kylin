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

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractGcStrategy {

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private GarbageLayoutType type;

    /**
     * Every subclass of AbstractStrategy should override this method.
     */
    protected abstract Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow);

    public final Set<Long> collectGarbageLayouts(List<LayoutEntity> inputLayouts, NDataflow dataflow) {
        List<LayoutEntity> toHandleLayouts = beforeCollect(inputLayouts);
        Set<Long> garbages = doCollect(toHandleLayouts, dataflow);
        afterCollect(inputLayouts, garbages);
        return garbages;
    }

    private List<LayoutEntity> beforeCollect(List<LayoutEntity> inputLayouts) {
        List<LayoutEntity> layoutsToHandle = Lists.newArrayList(inputLayouts);
        skipGcTableIndex(layoutsToHandle);
        return layoutsToHandle;
    }

    protected void skipGcTableIndex(List<LayoutEntity> inputLayouts) {
        // if no need to tailor TableIndex 
        if (!KylinConfig.getInstanceFromEnv().isRemoveIncludedTableIndexEnabled()) {
            inputLayouts.removeIf(layout -> layout.getId() > IndexEntity.TABLE_INDEX_START_ID);
        }
    }

    /**
     * Put hit frequency of removed layouts to reserved layouts.
     */
    protected void shiftLayoutHitCount(Map<LayoutEntity, LayoutEntity> removedToReservedMap, NDataflow dataflow) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfManager = NDataflowManager.getInstance(kylinConfig, dataflow.getProject());
        Map<Long, FrequencyMap> layoutHitCount = dataflow.getLayoutHitCount();
        removedToReservedMap.forEach((removedLayout, reservedLayout) -> {
            FrequencyMap removedFreqMap = layoutHitCount.get(removedLayout.getId());
            if (removedFreqMap == null) {
                return;
            }
            layoutHitCount.putIfAbsent(reservedLayout.getId(), new FrequencyMap());
            Map<Long, Integer> reservedHitFreq = layoutHitCount.get(reservedLayout.getId()).getDateFrequency();
            Map<Long, Integer> removedHitFreq = removedFreqMap.getDateFrequency();
            removedHitFreq.forEach((date, hitCount) -> reservedHitFreq.merge(date, hitCount, Integer::sum));
        });
        dfManager.updateDataflow(dataflow.getUuid(), copyForWrite -> copyForWrite.setLayoutHitCount(layoutHitCount));
    }

    private void afterCollect(List<LayoutEntity> inputLayouts, Set<Long> garbages) {
        inputLayouts.removeIf(layout -> garbages.contains(layout.getId()));
    }
}
