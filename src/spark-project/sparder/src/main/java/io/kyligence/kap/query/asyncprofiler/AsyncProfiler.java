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

import java.io.IOException;

public class AsyncProfiler {

    private static final String LOCAL_DEV_LIB_PATH =
            "../spark-project/sparder/src/main/resources/async-profiler-lib/macOS/libasyncProfiler.so";
    private static final String LIB_PATH = "/async-profiler-lib/linux64/libasyncProfiler.so";

    private static AsyncProfiler profiler;

    public static synchronized AsyncProfiler getInstance() throws IOException {
        if (profiler == null) {
            boolean isLocalDev = Boolean.parseBoolean(System.getProperty("async.profiler.dev", "false"));
            profiler = new AsyncProfiler(isLocalDev);
        }
        return profiler;
    }

    private AsyncProfiler(boolean localDev) throws IOException {
        if (localDev) {
            System.load(new java.io.File(LOCAL_DEV_LIB_PATH).getAbsolutePath());
        } else {
            final java.nio.file.Path tmpLib = java.io.File.createTempFile("libasyncProfiler", ".so").toPath();
            java.nio.file.Files.copy(
                    AsyncProfilerTool.class.getResourceAsStream(LIB_PATH),
                    tmpLib,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            System.load(tmpLib.toAbsolutePath().toString());
        }
    }

    public native void start0(String event, long interval, boolean reset) throws IllegalStateException;
    public native void stop0() throws IllegalStateException;
    public native String execute0(String command) throws IllegalArgumentException, IllegalStateException, IOException;
    public native void filterThread0(Thread thread, boolean enable);
}
