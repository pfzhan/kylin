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

package io.kyligence.kap.secondstorage;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Only used for concurrent integration test
 */
public class SecondStorageConcurrentTestUtil {
    // for load data to clickhouse
    public static final String WAIT_PAUSED = "WAIT_PAUSED";
    public static final String WAIT_BEFORE_COMMIT = "WAIT_BEFORE_COMMIT";

    private static final Map<String, Integer> latchMap = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> waitingMap = new ConcurrentHashMap<>();

    public static void wait(String point) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            try {
                if (latchMap.containsKey(point)) {
                    waitingMap.put(point, true);
                    Thread.sleep(latchMap.get(point));
                    latchMap.remove(point);
                    waitingMap.remove(point);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void registerWaitPoint(String point, int ms) {
        Preconditions.checkState(!latchMap.containsKey(point));
        latchMap.put(point, ms);
    }

    public static boolean existWaitPoint(String point) {
        return latchMap.containsKey(point);
    }

    public static boolean isWaiting(String point) {
        return waitingMap.containsKey(point);
    }

    private SecondStorageConcurrentTestUtil() {
    }
}
