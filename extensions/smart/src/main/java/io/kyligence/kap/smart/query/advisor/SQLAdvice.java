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
package io.kyligence.kap.smart.query.advisor;

import java.io.Serializable;

import io.kyligence.kap.common.obf.IKeep;

public class SQLAdvice implements IKeep, Serializable {
    private static final long serialVersionUID = -1L;
    private final String incapableReason;
    private final String suggestion;

    public static SQLAdvice build(String reason, String suggest) {
        return new SQLAdvice(reason, suggest);
    }

    public String getIncapableReason() {
        return incapableReason;
    }

    public String getSuggestion() {
        return suggestion;
    }

    private SQLAdvice(String incapableReason, String suggestion) {
        this.incapableReason = incapableReason;
        this.suggestion = suggestion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SQLAdvice that = (SQLAdvice) o;

        if (incapableReason != null ? !incapableReason.equals(that.incapableReason) : that.incapableReason != null)
            return false;
        return suggestion != null ? suggestion.equals(that.suggestion) : that.suggestion == null;
    }

    @Override
    public int hashCode() {
        int result = incapableReason != null ? incapableReason.hashCode() : 0;
        result = 31 * result + (suggestion != null ? suggestion.hashCode() : 0);
        return result;
    }
}
