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

package org.apache.kylin.rest.response;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_PARSE_JSON;
import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;

import org.apache.kylin.common.exception.KylinException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.base.Throwables;

/**
 * response to client when the return HTTP code is not 200
 */
public class ErrorResponse extends EnvelopeResponse {

    //stacktrace of the exception
    public String stacktrace;

    //same as EnvelopeResponse.msg, kept for legacy reasons
    public String exception;

    //request URL, kept from legacy codes
    public String url;

    public String suggestion;

    @JsonProperty("error_code")
    public String errorCode;

    public ErrorResponse(String url, Throwable exception) {
        super();
        this.url = url;
        this.exception = exception.getLocalizedMessage();
        this.data = null;

        if (exception instanceof KylinException) {
            this.msg = exception.getLocalizedMessage();
            KylinException kylinException = (KylinException) exception;
            this.code = kylinException.getCode();
            this.suggestion = kylinException.getSuggestionString();
            this.errorCode = kylinException.getErrorCodeString();
            if (kylinException.isThrowTrace()) {
                this.stacktrace = Throwables.getStackTraceAsString(exception);
            }
            this.data = kylinException.getData();
        } else {
            String errorCodeString = UNKNOWN_ERROR_CODE.toErrorCode().getLocalizedString();
            if (exception.getClass() == JsonParseException.class) {
                errorCodeString = FAILED_PARSE_JSON.toErrorCode().getLocalizedString();
            }

            this.msg = errorCodeString + " " + exception.getLocalizedMessage();
            this.code = KylinException.CODE_UNDEFINED;
            this.stacktrace = Throwables.getStackTraceAsString(exception);
        }
    }

}
