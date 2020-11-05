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
package org.apache.kylin.query.exception;

import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ErrorCodeSupplier;

public enum QueryErrorCode implements ErrorCodeSupplier {

    // 20002XXX model
    SCD2_DUPLICATE_JOIN_COL("KE-20002001"), //
    SCD2_DUPLICATE_FK_PK_PAIR("KE-20002002"), //
    SCD2_EMPTY_EQUI_JOIN("KE-20002003"), //
    SCD2_DUPLICATE_CONDITION("KE-20002004"), //
    SCD2_COMMON_ERROR("KE-20002005"), //
    SCD2_SAVE_MODEL_WHEN_DISABLED("KE-20002006"), //
    CC_EXPRESSION_ILLEGAL("KE-20002007"),

    // 20003XXX user
    USER_STOP_QUERY("KE-20003001"), //

    // 20007XXX table
    EMPTY_TABLE("KE-20007001"), //

    // 20029XXX optimization rule
    UNSUPPORTED_SUM_CASE_WHEN("KE-20029001"), //

    // 20030XXX push down
    INVALID_PARAMETER_PUSH_DOWN("KE-20030001"), //
    NO_AUTHORIZED_COLUMNS("KE-20030002"), //

    // 20032XXX query busy
    BUSY_QUERY("KE-20032001"), //

    // 20040XXX async query
    ASYNC_QUERY_ILLEGAL_PARAM("KE-20040001"), //

    ;

    private final ErrorCode errorCode;

    QueryErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
