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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.execution;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import lombok.Getter;

/**
 */
public final class ExecuteResult {

    public enum State {
        SUCCEED, ERROR
    }

    private final State state;
    private final String output;
    private final Throwable throwable;

    //extra
    @Getter
    private Map<String, String> extraInfo = Maps.newHashMap();

    private ExecuteResult(State state, String output, Throwable throwable) {
        Preconditions.checkArgument(state != null, "state cannot be null");

        if (state == State.SUCCEED) {
            Preconditions.checkNotNull(output);
            Preconditions.checkState(throwable == null);
        } else if (state == State.ERROR) {
            Preconditions.checkNotNull(throwable);
            Preconditions.checkState(output == null);
        } else {
            throw new IllegalStateException();
        }

        this.state = state;
        this.output = output;
        this.throwable = throwable;
    }

    public static ExecuteResult createSucceed() {
        return new ExecuteResult(State.SUCCEED, "succeed", null);
    }

    public static ExecuteResult createSucceed(String output) {
        return new ExecuteResult(State.SUCCEED, output, null);
    }

    public static ExecuteResult createError(Throwable throwable) {
        Preconditions.checkArgument(throwable != null, "throwable cannot be null");
        return new ExecuteResult(State.ERROR, null, throwable);
    }

    public State state() {
        return state;
    }

    public boolean succeed() {
        return state == State.SUCCEED;
    }

    public String output() {
        return output;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getErrorMsg() {
        if (succeed()) {
            return null;
        }

        if (throwable != null) {
            return Throwables.getStackTraceAsString(throwable);
        } else if (StringUtils.isNotEmpty(output)) {
            return output;
        } else {
            return "error";
        }
    }

    public String getShortErrMsg() {
        if (succeed()) {
            return null;
        }
        if (throwable != null) {
            String msg = Throwables.getRootCause(throwable).getMessage();
            if (msg != null && msg.length() > 1000) {
                return msg.substring(0, 997) + "...";
            }
            return msg;
        } else {
            return null;
        }
    }
}
