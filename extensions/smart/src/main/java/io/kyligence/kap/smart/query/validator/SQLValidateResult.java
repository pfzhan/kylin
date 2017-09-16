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
package io.kyligence.kap.smart.query.validator;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;

public class SQLValidateResult implements IKeep, Serializable {
    private static final long serialVersionUID = -1L;
    private boolean capable;

    private Set<SQLAdvice> SQLAdvices = Sets.newHashSet();

    public boolean isCapable() {
        return capable;
    }

    public void setCapable(boolean capable) {
        this.capable = capable;
    }

    public Set<SQLAdvice> getSQLAdvices() {
        return SQLAdvices;
    }

    public void setSQLAdvices(Set<SQLAdvice> SQLAdvices) {
        this.SQLAdvices = SQLAdvices;
    }

    public static SQLValidateResult successStats() {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(true);
        return stats;
    }

    public static SQLValidateResult failedStats() {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(false);
        return stats;
    }

    public static SQLValidateResult failedStats(SQLAdvice SQLAdvice) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(false);
        stats.addSqlAdvisor(SQLAdvice);
        return stats;
    }

    public void addSqlAdvisor(SQLAdvice SQLAdvice) {
        this.SQLAdvices.add(SQLAdvice);
    }

    public static SQLValidateResult failedStats(List<SQLAdvice> SQLAdvices) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(false);
        stats.setSQLAdvices(Sets.<SQLAdvice> newHashSet(SQLAdvices));
        return stats;
    }
    
    @Override
    public String toString() {
        String message = "";
        for (SQLAdvice sqlAdvice : getSQLAdvices()) {
            message += String.format("\n  reason:\"%s\", suggest:\"%s\"",
                    sqlAdvice.getIncapableReason().replace(",", "，"), sqlAdvice.getSuggestion().replace(",", "，"));
        }
        return String.format("capable:%s %s", isCapable(), message);
    }
}
