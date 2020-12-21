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
package io.kyligence.kap.common.util;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

import lombok.val;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

public final class SystemInfoCollector {

    private static final long KIBI = 1L << 10;
    private static final long MEBI = 1L << 20;
    private static final long GIBI = 1L << 30;
    private static final long TEBI = 1L << 40;
    private static final long PEBI = 1L << 50;
    private static final long EXBI = 1L << 60;

    private static HardwareAbstractionLayer hal = null;

    private static OperatingSystem os = null;

    static {
        init();
    }

    public static void init() {
        SystemInfo si = new SystemInfo();
        hal = si.getHardware();
        os = si.getOperatingSystem();
    }

    public static Integer getAvailableMemoryInfo() {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return 6192;
        }
        val mem = hal.getMemory();
        return mem.getAvailable() % MEBI == 0
                ? Integer.parseInt(String.format(Locale.ROOT, "%d", mem.getAvailable() / MEBI))
                : Integer.parseInt(String.format(Locale.ROOT, "%.0f", (double) mem.getAvailable() / MEBI));
    }
}
