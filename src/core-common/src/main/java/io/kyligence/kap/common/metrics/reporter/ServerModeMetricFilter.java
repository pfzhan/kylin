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

package io.kyligence.kap.common.metrics.reporter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.KylinConfig;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.common.util.AddressUtil;

@Slf4j
public class ServerModeMetricFilter implements MetricFilter {

    private static final String GLOBAL = "_global";

    private static final String SERVER_MODE;
    private static final String SERVICE_INFO;

    static {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        SERVER_MODE = config.getServerMode();
        SERVICE_INFO = AddressUtil.getLocalInstance();
    }

    @Override
    public boolean matches(String name, Metric metric) {
        boolean isLeader = false;

        Epoch epoch = null;
        try {
            epoch = EpochStore.getEpochStore(KylinConfig.getInstanceFromEnv()).getEpoch(GLOBAL);
        } catch (Exception e) {
            log.warn("Get global epoch failed", e);
        }
        if (epoch != null) {
            String currentEpochOwner = epoch.getCurrentEpochOwner();
            if (currentEpochOwner != null && currentEpochOwner.split("\\|")[0].equals(SERVICE_INFO)) {
                isLeader = true;
            }
        }
        String[] split = name.split(":");
        if (split.length > 1) {
            String metricName = split[0];
            NMetricsName metricsName = NMetricsName.getMetricsName(metricName);
            if (metricsName != null) {
                return metricsName.support(SERVER_MODE, isLeader);
            }
        }
        return true;
    }
}
