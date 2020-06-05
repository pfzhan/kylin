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

import com.google.common.annotations.VisibleForTesting;
import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class FullGCDurationChecker extends AbstractHealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(FullGCDurationChecker.class);

    private AtomicLong accumulator = new AtomicLong(0);

    private FullGCRecord[] ringBuffer;

    private int factor;
    private long guardianCheckInterval;

    private boolean restartEnabled;
    private double ratioThreshold;

    private boolean busyEnabled;
    private double lowWatermark;
    private double highWatermark;

    public FullGCDurationChecker() {
        setPriority(10000);

        factor = getKylinConfig().getGuardianFullGCCheckFactor();
        guardianCheckInterval = getKylinConfig().getGuardianCheckInterval();

        restartEnabled = getKylinConfig().isFullGCRatioBeyondRestartEnabled();
        ratioThreshold = getKylinConfig().getGuardianFullGCRatioThreshold();

        busyEnabled = getKylinConfig().isDowngradeOnFullGCBusyEnable();
        lowWatermark = getKylinConfig().getGuardianFullGCLowWatermark();
        highWatermark = getKylinConfig().getGuardianFullGCHighWatermark();

        lowWatermark = Double.min(highWatermark, lowWatermark);

        ringBuffer = new FullGCRecord[factor];
    }

    @Override
    CheckResult doCheck() {
        CheckResult result = new CheckResult(CheckStateEnum.NORMAL);

        try {
            double fgcTime = getGCTime();
            long now = getNowTime();
            double fullGCRatio = 0.0;

            int index = (int) (accumulator.get() % ringBuffer.length);
            if (null != ringBuffer[index]) {
                FullGCRecord oldRecord = ringBuffer[index];
                long duration = (now - oldRecord.checkTime) / 1000;
                fullGCRatio = (fgcTime - oldRecord.fgcTime) / duration * 100;

                if (restartEnabled && fullGCRatio >= ratioThreshold) {
                    result = new CheckResult(CheckStateEnum.RESTART,
                            String.format("Full gc time duration ratio in %d seconds is more than %.2f%%",
                                    factor * guardianCheckInterval, ratioThreshold));
                } else if (busyEnabled) {
                    if (fullGCRatio >= highWatermark) {
                        result = new CheckResult(CheckStateEnum.QUERY_DOWNGRADE,
                                String.format("Full gc time duration ratio in %d seconds is more than %.2f%%",
                                        factor * guardianCheckInterval, highWatermark));
                    } else if (fullGCRatio < lowWatermark) {
                        result = new CheckResult(CheckStateEnum.QUERY_UPGRADE,
                                String.format("Full gc time duration ratio in %d seconds is less than %.2f%%",
                                        factor * guardianCheckInterval, lowWatermark));
                    }
                }
            }

            logger.info("Full gc time duration ratio in {} seconds is {}%, full gc time: {}",
                    factor * guardianCheckInterval, fullGCRatio, fgcTime);

            ringBuffer[index] = new FullGCRecord(now, fgcTime);
            accumulator.incrementAndGet();
        } catch (Exception e) {
            logger.error("Guardian Process: check full gc time failed!", e);
            result = new CheckResult(CheckStateEnum.WARN, e.getMessage());
        }

        return result;
    }

    @VisibleForTesting
    public long getNowTime() {
        return System.currentTimeMillis();
    }

    @VisibleForTesting
    public double getGCTime() throws ShellException {
        String cmd = "sh " + getKylinHome() + "/sbin/guardian-get-fgc-time.sh";

        CliCommandExecutor.CliCmdExecResult result = getCommandExecutor().execute(cmd, null);
        return Double.parseDouble(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));
    }

    @Getter
    @Setter
    @AllArgsConstructor
    private class FullGCRecord {
        private long checkTime;
        private double fgcTime;
    }
}
