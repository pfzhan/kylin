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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 * DefaultGarbageCleaner only choose all agg layouts to handle when `kylin.garbage.only-tailor-agg-index` is true,
 * otherwise it will choose all layouts in current model. For smart-mode LowFreqLayoutGcStrategy works, expert-mode
 * RedundantLayoutGcStrategy works, semi-mode LowFreqLayoutGcStrategy and RedundantLayoutGcStrategy work.
 */
public class DefaultGarbageCleaner {

    private static final AbstractGcStrategy[] GC_STRATEGIES = { new IncludedLayoutGcStrategy(),
            new LowFreqLayoutGcStrategy() };

    private DefaultGarbageCleaner() {
    }

    public static Map<Long, GarbageLayoutType> findGarbageLayouts(NDataflow dataflow) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Map<Long, GarbageLayoutType> garbageLayoutTypeMap = Maps.newHashMap();
        List<LayoutEntity> readyLayouts = dataflow.extractReadyLayouts();
        if (kylinConfig.isOnlyTailorAggIndex()) {
            readyLayouts.removeIf(layout -> layout.getId() > IndexEntity.TABLE_INDEX_START_ID);
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(dataflow.getProject());
        if (projectInstance.isSemiAutoMode() || projectInstance.isExpertMode()) {
            // val manual = readyLayouts.stream().filter(LayoutEntity::isManual).collect(Collectors.toList())
            // TODO another strategy to handle manual layouts
        }

        val autoLayouts = readyLayouts.stream().filter(LayoutEntity::isAuto).collect(Collectors.toList());
        for (AbstractGcStrategy strategy : GC_STRATEGIES) {
            strategy.collectGarbageLayouts(autoLayouts, dataflow)
                    .forEach(id -> garbageLayoutTypeMap.put(id, strategy.getType()));
        }

        return garbageLayoutTypeMap;
    }

    @VisibleForTesting
    public static Set<Long> findGarbageLayouts(NDataflow dataflow, AbstractGcStrategy strategy) {
        List<LayoutEntity> allLayouts = dataflow.getIndexPlan().getAllLayouts();
        return strategy.collectGarbageLayouts(allLayouts, dataflow);
    }
}
