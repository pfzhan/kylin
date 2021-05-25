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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Getter;

public class IndexOptimizer {

    // if true, every strategy will print log in details
    private boolean needLog;

    @Getter
    private final List<AbstractOptStrategy> strategiesForAuto = Lists.newArrayList();

    @Getter
    private final List<AbstractOptStrategy> strategiesForManual = Lists.newArrayList();

    public IndexOptimizer(boolean needLog) {
        this.needLog = needLog;
    }

    protected List<LayoutEntity> filterAutoLayouts(NDataflow dataflow) {
        return dataflow.extractReadyLayouts().stream() //
                .filter(layout -> !layout.isManual() && layout.isAuto()) //
                .filter(layout -> !layout.isBase())
                .collect(Collectors.toList());
    }

    protected List<LayoutEntity> filterManualLayouts(NDataflow dataflow) {
        return dataflow.extractReadyLayouts().stream() //
                .filter(LayoutEntity::isManual) //
                .filter(layout -> !layout.isBase())
                .collect(Collectors.toList());
    }

    public Map<Long, GarbageLayoutType> getGarbageLayoutMap(NDataflow dataflow) {
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()) //
                .getProject(dataflow.getProject()).isExpertMode()) {
            return Maps.newHashMap();
        }

        Map<Long, GarbageLayoutType> garbageLayoutTypeMap = Maps.newHashMap();
        for (AbstractOptStrategy strategy : getStrategiesForAuto()) {
            strategy.collectGarbageLayouts(filterAutoLayouts(dataflow), dataflow, needLog)
                    .forEach(id -> garbageLayoutTypeMap.put(id, strategy.getType()));
        }

        for (AbstractOptStrategy strategy : getStrategiesForManual()) {
            strategy.collectGarbageLayouts(filterManualLayouts(dataflow), dataflow, needLog)
                    .forEach(id -> garbageLayoutTypeMap.put(id, strategy.getType()));
        }

        return garbageLayoutTypeMap;
    }

    @VisibleForTesting
    public static Set<Long> findGarbageLayouts(NDataflow dataflow, AbstractOptStrategy strategy) {
        List<LayoutEntity> allLayouts = dataflow.getIndexPlan().getAllLayouts();
        return strategy.collectGarbageLayouts(allLayouts, dataflow, false);
    }
}
