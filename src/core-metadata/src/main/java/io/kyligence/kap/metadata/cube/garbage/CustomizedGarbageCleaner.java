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
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.val;

/**
 * CustomizedGarbageCleaner helps user to define more complex garbage cleaner strategies.
 */
public class CustomizedGarbageCleaner {

    private static final String RECOMMENDED = "RECOMMENDED";
    private static final String CUSTOM = "CUSTOM";
    private static final String ALL = "ALL";

    private static final LowFreqLayoutGcStrategy LOW_FREQ_GC_STRATEGY = new LowFreqLayoutGcStrategy();
    private static final IncludedLayoutGcStrategy INCLUDED_GC_STRATEGY = new IncludedLayoutGcStrategy();

    private CustomizedGarbageCleaner() {
    }

    public static Map<Long, GarbageLayoutType> findGarbageLayouts(NDataflow dataflow) {
        Map<Long, GarbageLayoutType> garbageMap = Maps.newHashMap();
        val readyLayouts = dataflow.extractReadyLayouts();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String includeStrategyTarget = kylinConfig.getIncludedGarbageStrategyTarget();
        String lowFreqStrategyTarget = kylinConfig.getLowFreqGarbageStrategyTarget();

        findGarbage(dataflow, garbageMap, readyLayouts, INCLUDED_GC_STRATEGY, includeStrategyTarget);
        findGarbage(dataflow, garbageMap, readyLayouts, LOW_FREQ_GC_STRATEGY, lowFreqStrategyTarget);

        return garbageMap;
    }

    private static void findGarbage(NDataflow dataflow, Map<Long, GarbageLayoutType> garbageMap,
            List<LayoutEntity> readyLayouts, AbstractGcStrategy strategy, String targetLayoutType) {
        List<LayoutEntity> manualLayouts = Lists.newArrayList();
        List<LayoutEntity> autoLayouts = Lists.newArrayList();
        for (LayoutEntity layout : readyLayouts) {
            if (layout.isManual()) {
                manualLayouts.add(layout);
            } else {
                autoLayouts.add(layout);
            }
        }

        if (targetLayoutType.contains(CustomizedGarbageCleaner.ALL)) {
            findGarbage(dataflow, strategy, garbageMap, readyLayouts);
        } else {
            if (targetLayoutType.contains(CustomizedGarbageCleaner.RECOMMENDED)) {
                findGarbage(dataflow, strategy, garbageMap, autoLayouts);
            }
            if (targetLayoutType.contains(CustomizedGarbageCleaner.CUSTOM)) {
                findGarbage(dataflow, strategy, garbageMap, manualLayouts);
            }
        }
    }

    private static void findGarbage(NDataflow dataflow, AbstractGcStrategy strategy,
            Map<Long, GarbageLayoutType> garbageLayoutIds, List<LayoutEntity> inputLayouts) {
        Set<Long> garbageIdSet = strategy.collectGarbageLayouts(inputLayouts, dataflow);
        garbageIdSet.forEach(id -> garbageLayoutIds.put(id, strategy.getType()));
        inputLayouts.removeIf(layout -> garbageIdSet.contains(layout.getId()));
    }
}
