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
package org.apache.kylin.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.Map;

public class JobProcessContext {

    static Map<String, Process> runningProcess = Maps.newConcurrentMap();

    public static void registerProcess(String jobId, Process process) {
        runningProcess.put(jobId, process);
    }

    public static Process getProcess(String jobId) {
        return runningProcess.get(jobId);
    }

    public static void removeProcess(String jobId) {
        runningProcess.remove(jobId);
    }

    public static int getPid(Process process) throws IllegalAccessException, NoSuchFieldException {
        String className = process.getClass().getName();
        Preconditions.checkState(className.equals("java.lang.UNIXProcess"));
        Field f = process.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        return f.getInt(process);
    }
    
}