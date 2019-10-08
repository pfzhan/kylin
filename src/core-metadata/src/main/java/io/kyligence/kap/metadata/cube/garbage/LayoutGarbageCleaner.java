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

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;

public class LayoutGarbageCleaner {

    private static IGarbageCleanerStrategy[] smartModeStrategies = { new LowFreqLayoutGcStrategy() };
    private static IGarbageCleanerStrategy[] expertModeStrategies = { new LowFreqLayoutGcStrategy() };
    private static IGarbageCleanerStrategy[] semiAutoModeStrategies = { new LowFreqLayoutGcStrategy(),
            new RedundantLayoutGcStrategy() };

    private LayoutGarbageCleaner() {
    }

    public static Set<Long> findGarbageLayouts(NDataflow dataflow) {
        Set<Long> garbageLayoutIds = Sets.newHashSet();
        final ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(dataflow.getProject());

        IGarbageCleanerStrategy[] strategies = {};
        if (projectInstance.isSemiAutoMode()) {
            strategies = semiAutoModeStrategies;
        } else if (projectInstance.isExpertMode()) {
            strategies = expertModeStrategies;
        } else if (projectInstance.isSmartMode()) {
            strategies = smartModeStrategies;
        }

        for (IGarbageCleanerStrategy strategy : strategies) {
            garbageLayoutIds.addAll(strategy.collectGarbageLayouts(dataflow));
        }
        return garbageLayoutIds;
    }

    @VisibleForTesting
    public static Set<Long> findGarbageLayouts(NDataflow dataflow, IGarbageCleanerStrategy strategy) {
        return strategy.collectGarbageLayouts(dataflow);
    }
}
