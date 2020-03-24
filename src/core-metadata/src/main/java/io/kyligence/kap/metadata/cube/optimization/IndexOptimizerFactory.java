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

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexOptimizerFactory {

    private IndexOptimizerFactory() {
    }

    private static final AbstractOptStrategy INCLUDED_OPT_STRATEGY = new IncludedLayoutOptStrategy();
    private static final AbstractOptStrategy LOW_FREQ_OPT_STRATEGY = new LowFreqLayoutOptStrategy();
    private static final AbstractOptStrategy SIMILAR_OPT_STRATEGY = new SimilarLayoutOptStrategy();

    public static IndexOptimizer getOptimizer(NDataflow dataflow, boolean needLog) {
        IndexOptimizer optimizer = new IndexOptimizer(needLog);
        final int indexOptimizationLevel = KylinConfig.getInstanceFromEnv().getIndexOptimizationLevel();
        if (indexOptimizationLevel == 1) {
            optimizer.getStrategiesForAuto().add(INCLUDED_OPT_STRATEGY);
        } else if (indexOptimizationLevel == 2) {
            optimizer.getStrategiesForAuto().addAll(Lists.newArrayList(INCLUDED_OPT_STRATEGY, LOW_FREQ_OPT_STRATEGY));
        } else if (indexOptimizationLevel == 3) {
            optimizer.getStrategiesForAuto().addAll(Lists.newArrayList(INCLUDED_OPT_STRATEGY, LOW_FREQ_OPT_STRATEGY));
            optimizer.getStrategiesForManual().add(SIMILAR_OPT_STRATEGY);
        }

        // log if needed
        printLog(needLog, indexOptimizationLevel, dataflow.getIndexPlan().isFastBitmapEnabled());
        return optimizer;
    }

    private static void printLog(boolean needLog, int indexOptimizationLevel, boolean isFastBitmapEnabled) {
        if (!needLog) {
            return;
        }

        if (indexOptimizationLevel == 3 && isFastBitmapEnabled) {
            log.info("Routing to index optimization level two for fastBitMap enabled.");
        } else if (indexOptimizationLevel == 3 || indexOptimizationLevel == 2 || indexOptimizationLevel == 1) {
            log.info("Routing to index optimization level " + indexOptimizationLevel + ".");
        } else if (indexOptimizationLevel == 0) {
            log.info("Routing to index optimization level zero, no optimization.");
        } else {
            log.error("Not supported index optimization level");
        }
    }
}
