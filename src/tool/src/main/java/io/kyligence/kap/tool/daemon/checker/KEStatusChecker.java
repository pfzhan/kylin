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
package io.kyligence.kap.tool.daemon.checker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import io.kyligence.kap.common.util.SecretKeyUtil;
import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import io.kyligence.kap.tool.util.ToolUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

public class KEStatusChecker extends AbstractHealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHealthChecker.class);

    public static final String PERMISSION_DENIED = "Check permission failed!";

    private int failCount = 0;

    public KEStatusChecker() {
        setPriority(100000);
    }

    /**
     * KE restart, KG have to restart.
     * @return
     */
    private byte[] getEncryptedTokenForKAPHealth() throws Exception {
        try {
            if (null == getKgSecretKey()) {
                setKgSecretKey(SecretKeyUtil.readKGSecretKeyFromFile());
            }

            if (null == getKE_PID()) {
                setKEPid(ToolUtil.getKylinPid());
            }
            return SecretKeyUtil.generateEncryptedTokenWithPid(getKgSecretKey(), getKE_PID());
        } catch (Exception e) {
            logger.error("Read KG secret key from file failed.", e);
            throw e;
        }
    }

    @VisibleForTesting
    public EnvelopeResponse<Status> getHealthStatus() throws Exception {
        TypeReference<EnvelopeResponse<Status>> typeRef = new TypeReference<EnvelopeResponse<Status>>() {
        };
        byte[] encryptedToken = getEncryptedTokenForKAPHealth();
        return getRestClient().getKapHealthStatus(typeRef, encryptedToken);
    }

    @Override
    CheckResult doCheck() {
        try {
            EnvelopeResponse<Status> response = getHealthStatus();
            if (!ResponseCode.CODE_SUCCESS.equals(response.code)) {
                if (PERMISSION_DENIED.equals(response.getMsg())) {
                    setKgSecretKey(null);
                }

                throw new RuntimeException("Get KE health status failed: " + response.msg);
            }

            Status status = response.getData();

            StringBuilder sb = new StringBuilder();

            boolean sparkRestart = false;
            boolean slowQueryRestart = false;

            SparkStatus sparkStatus = status.getSparkStatus();
            if (getKylinConfig().isSparkFailRestartKeEnabled()
                    && sparkStatus.getFailureTimes() >= getKylinConfig().getGuardianSparkFailThreshold()) {
                sparkRestart = true;
                sb.append(String.format(Locale.ROOT,
                        "Spark restart failure reach %s times, last restart failure time %s. ",
                        getKylinConfig().getGuardianSparkFailThreshold(), sparkStatus.getLastFailureTime()));
            }

            List<CanceledSlowQueryStatus> slowQueryStatusList = status.getCanceledSlowQueryStatus();
            if (CollectionUtils.isNotEmpty(slowQueryStatusList)) {
                long failedKillQueries = slowQueryStatusList.stream().filter(slowQueryStatus -> slowQueryStatus
                        .getCanceledTimes() >= getKylinConfig().getGuardianSlowQueryKillFailedThreshold()).count();

                if (getKylinConfig().isSlowQueryKillFailedRestartKeEnabled() && failedKillQueries > 0) {
                    slowQueryRestart = true;
                    sb.append(String.format(Locale.ROOT, "Have slowQuery be canceled reach %s times. ",
                            getKylinConfig().getGuardianSparkFailThreshold()));
                }
            }

            if (sparkRestart || slowQueryRestart) {
                return new CheckResult(CheckStateEnum.RESTART, sb.toString());
            }

            failCount = 0;
            return new CheckResult(CheckStateEnum.NORMAL);
        } catch (Exception e) {
            logger.info("Check KE status failed! ", e);

            if (++failCount >= getKylinConfig().getGuardianApiFailThreshold()) {
                return new CheckResult(CheckStateEnum.RESTART, String.format(Locale.ROOT,
                        "Instance is in inaccessible status, API failed count reach %d", failCount));
            } else {
                return new CheckResult(CheckStateEnum.WARN, e.getMessage());
            }
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EnvelopeResponse<T> {
        protected String code;
        protected T data;
        protected String msg;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Status {
        @JsonProperty("spark_status")
        private SparkStatus sparkStatus;

        @JsonProperty("slow_queries_status")
        private List<CanceledSlowQueryStatus> canceledSlowQueryStatus;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SparkStatus {
        @JsonProperty("restart_failure_times")
        private int failureTimes;
        @JsonProperty("last_restart_failure_time")
        private long lastFailureTime;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CanceledSlowQueryStatus {
        @JsonProperty("query_id")
        private String queryId;
        @JsonProperty("canceled_times")
        private int canceledTimes;
        @JsonProperty("last_canceled_time")
        private long lastCanceledTime;
        @JsonProperty("duration_time")
        private float queryDurationTime;
    }
}
