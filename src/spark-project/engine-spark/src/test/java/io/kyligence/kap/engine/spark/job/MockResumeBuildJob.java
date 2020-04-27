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

package io.kyligence.kap.engine.spark.job;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;

public class MockResumeBuildJob extends DFBuildJob {
    // for ut only
    private volatile Set<String> breakPointLayouts;

    public static void main(String[] args) {
        MockResumeBuildJob buildJob = new MockResumeBuildJob();
        buildJob.execute(args);
    }

    @Override
    protected void onInit() {
        // for ut only
        if (Objects.isNull(config) || !config.isUTEnv()) {
            return;
        }
        final String layoutsStr = getParam(NBatchConstants.P_BREAK_POINT_LAYOUTS);
        breakPointLayouts = org.apache.commons.lang3.StringUtils.isBlank(layoutsStr) ? Sets.newHashSet()
                : Stream.of(org.apache.commons.lang3.StringUtils.split(layoutsStr, ",")).collect(Collectors.toSet());
    }

    @Override
    protected void onLayoutFinished(long layoutId) {
        // for ut only
        final int sleep_secs = 10;
        if (Objects.nonNull(config) && config.isUTEnv() && breakPointLayouts.contains(String.valueOf(layoutId))) {
            logger.info("breakpoint BUILD_LAYOUT, sleep {} secs, layout {}", sleep_secs, layoutId);
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(sleep_secs));
            } catch (InterruptedException ie) {
                logger.error("buildLayer sleeping interrupted", ie);
            } finally {
                Thread.currentThread().interrupt();
            }
        }
    }
}
