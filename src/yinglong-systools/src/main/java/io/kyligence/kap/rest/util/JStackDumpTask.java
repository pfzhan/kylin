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
package io.kyligence.kap.rest.util;

import java.io.File;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.tool.util.ToolUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JStackDumpTask implements Runnable {
    private final File outputDir;

    public JStackDumpTask() {
        outputDir = new File(KylinConfig.getKylinHome(), "logs");
    }

    public JStackDumpTask(File path) {
        this.outputDir = path;
    }

    @Override
    public void run() {
        log.trace("start dump stack");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TimestampedRollingFileOutputDir rollingFileOutputDir = new TimestampedRollingFileOutputDir(outputDir,
                "jstack.timed.log", kylinConfig.getJStackDumpTaskLogsMaxNum());
        try {
            ToolUtil.dumpKylinJStack(rollingFileOutputDir.newOutputFile());
            log.trace("dump jstack successful");
        } catch (Exception e) {
            log.error("Error dump jstack info", e);
        }

    }
}
