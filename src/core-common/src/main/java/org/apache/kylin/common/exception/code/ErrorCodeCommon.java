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

import java.util.Locale;

import lombok.Getter;

@Getter
public enum ErrorCodeCommon implements ErrorCodeProducer {

    NON_KE_EXCEPTION("KE-060100201");

    private final ErrorCode errorCode;
    private final ErrorMsg errorMsg;
    private final ErrorSuggestion errorSuggestion;

    ErrorCodeCommon(String keCode) {
        this.errorCode = new ErrorCode(keCode);
        this.errorMsg = new ErrorMsg(this.errorCode.getCode());
        this.errorSuggestion = new ErrorSuggestion(this.errorCode.getCode());
    }

    @Override
    public ErrorSuggestion getErrorSuggest() {
        return this.errorSuggestion;
    }

    @Override
    public String getMsg(Object... args) {
        return String.format(Locale.ROOT, this.errorMsg.getLocalizedString(), args);
    }

    @Override
    public String getCodeMsg(Object... args) {
        return String.format(Locale.ROOT, CODE_MSG_FORMAT, errorCode.getCode(), getMsg(args));
    }
}
