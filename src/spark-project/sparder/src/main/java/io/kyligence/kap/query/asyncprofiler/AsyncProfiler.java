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

package io.kyligence.kap.query.asyncprofiler;

import io.kyligence.kap.common.obf.IKeep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AsyncProfiler implements IKeep {

    private static final String LOCAL_DEV_LIB_PATH =
            "../spark-project/sparder/src/main/resources/async-profiler-lib/macOS/libasyncProfiler.so";
    private static final String LIB_PATH = "/async-profiler-lib/linux64/libasyncProfiler.so";

    private static final Logger logger = LoggerFactory.getLogger(AsyncProfiler.class);

    private static AsyncProfiler profiler;
    private boolean loaded = false;

    public static synchronized AsyncProfiler getInstance() {
        if (profiler == null) {
            profiler = new AsyncProfiler();
        }
        return profiler;
    }

    private AsyncProfiler() {
        try {
            boolean isTestingOnLocalMac = System.getProperty("os.name", "").contains("Mac")
                    || System.getProperty("os.name", "").contains("OS X");
            if (isTestingOnLocalMac) {
                System.load(new java.io.File(LOCAL_DEV_LIB_PATH).getAbsolutePath());
            } else {
                final java.nio.file.Path tmpLib = java.io.File.createTempFile("libasyncProfiler", ".so").toPath();
                java.nio.file.Files.copy(
                        AsyncProfilerTool.class.getResourceAsStream(LIB_PATH),
                        tmpLib,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                System.load(tmpLib.toAbsolutePath().toString());
            }
            loaded = true;
        } catch (Throwable e) {
            logger.error("async lib loading failed.", e);
        }
    }

    public boolean isLoaded() {
        return loaded;
    }

    public void stop() throws IllegalStateException {
        if (loaded) {
            stop0();
        } else {
            logger.error("invalid operation stop(). async lib loading failed.");
        }
    }

    public String execute(String command) throws IllegalArgumentException, IllegalStateException, IOException {
        if (loaded) {
            return execute0(command);
        } else {
            logger.error("invalid operation execute(). async lib loading failed.");
            return "";
        }
    }

    public native void start0(String event, long interval, boolean reset) throws IllegalStateException;
    public native void stop0() throws IllegalStateException;
    public native String execute0(String command) throws IllegalArgumentException, IllegalStateException, IOException;
    public native void filterThread0(Thread thread, boolean enable);
}
