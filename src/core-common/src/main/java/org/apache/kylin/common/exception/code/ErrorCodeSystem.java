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
package org.apache.kylin.common.exception.code;

public enum ErrorCodeSystem implements ErrorCodeProducer {

    // 400052XX password
    PASSWORD_INVALID_ENCODER("KE-040005201"),
    PASSWORD_INIT_ENCODER_FAILED("KE-040005202"),

    // 400212XX epoch
    EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE("KE-040021201"),

    // 400232XX HA
    JOB_NODE_API_INVALID("KE-040023201"),
    JOB_NODE_QUERY_API_INVALID("KE-040023202"),
    QUERY_NODE_API_INVALID("KE-040023203"),

    // 400242XX maintenance mode
    MAINTENANCE_MODE_WRITE_FAILED("KE-040024201"),
    MAINTENANCE_MODE_ENTER_FAILED("KE-040024202"),
    MAINTENANCE_MODE_LEAVE_FAILED("KE-040024203"),

    // 400252XX system config
    SYSTEM_PROFILE_ABNORMAL_DATA("KE-040026201");

    private final ErrorCode errorCode;
    private final ErrorMsg errorMsg;
    private final ErrorSuggestion errorSuggestion;

    ErrorCodeSystem(String keCode) {
        this.errorCode = new ErrorCode(keCode);
        this.errorMsg = new ErrorMsg(this.errorCode.getCode());
        this.errorSuggestion = new ErrorSuggestion(this.errorCode.getCode());
    }

    @Override
    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    @Override
    public ErrorMsg getErrorMsg() {
        return this.errorMsg;
    }

    @Override
    public ErrorSuggestion getErrorSuggest() {
        return this.errorSuggestion;
    }
}
