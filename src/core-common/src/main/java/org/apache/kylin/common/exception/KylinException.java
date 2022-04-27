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
package org.apache.kylin.common.exception;

import java.util.Collection;
import java.util.Locale;

import org.apache.kylin.common.exception.code.ErrorCodeProducer;

import lombok.Getter;

@Getter
public class KylinException extends RuntimeException {

    public final static String CODE_SUCCESS = "000";
    public final static String CODE_UNAUTHORIZED = "401";
    public final static String CODE_UNDEFINED = "999";

    private final ErrorCode errorCode;
    // for example 999
    private final String code;
    private Object data;
    private boolean throwTrace = true;

    private transient ErrorCodeProducer errorCodeProducer;
    private transient Object[] args;

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg) {
        super(msg);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = CODE_UNDEFINED;
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg, boolean throwTrace) {
        super(msg);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = CODE_UNDEFINED;
        this.throwTrace = throwTrace;
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, Throwable cause) {
        this(errorCodeSupplier, cause.getMessage(), cause);
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg, Throwable cause) {
        super(msg, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = CODE_UNDEFINED;
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg, Collection<? extends Throwable> causes) {
        super(msg);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = CODE_UNDEFINED;
        causes.forEach(this::addSuppressed);
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg, String code) {
        super(msg);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = code;
    }

    public KylinException(ErrorCodeSupplier errorCodeSupplier, String msg, String code, Throwable cause) {
        super(msg, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.code = code;
    }

    public KylinException(ErrorCodeProducer errorCodeProducer, Object... args) {
        super(errorCodeProducer.getMsg(args));

        // old
        this.errorCode = new ErrorCode(errorCodeProducer.getErrorCode().getCode());
        this.code = CODE_UNDEFINED;

        //new
        this.args = args;
        this.errorCodeProducer = errorCodeProducer;
    }

    public KylinException(ErrorCodeProducer errorCodeProducer, Throwable cause, Object... args) {
        super(errorCodeProducer.getMsg(args), cause);

        // old
        this.errorCode = new ErrorCode(errorCodeProducer.getErrorCode().getCode());
        this.code = CODE_UNDEFINED;

        //new
        this.args = args;
        this.errorCodeProducer = errorCodeProducer;
    }

    public String getSuggestionString() {
        return (errorCodeProducer == null) ? null : errorCodeProducer.getErrorSuggest().getLocalizedString();
    }

    public String getErrorCodeString() {
        return (errorCodeProducer == null) ? errorCode.getCodeString() : errorCodeProducer.getErrorCode().getCode();
    }

    public KylinException withData(Object data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        //for log
        if (errorCodeProducer != null) {
            return errorCodeProducer.getCodeMsg(this.args);
        }
        return String.join(" \n", errorCode.getString(), super.toString());
    }

    @Override
    public String getLocalizedMessage() {
        //for front
        if (errorCodeProducer != null) {
            return errorCodeProducer.getCodeMsg(this.args);
        }
        return String.format(Locale.ROOT, "%s%s", errorCode.getLocalizedString(), super.getLocalizedMessage());
    }

}
