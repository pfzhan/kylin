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
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SQLValidateResult implements IKeep, Serializable {

    private static final long serialVersionUID = -1L;
    private boolean capable;
    private SQLResult result;
    private Set<SQLAdvice> sqlAdvices = Sets.newHashSet();

    static SQLValidateResult successStats(SQLResult sqlResult) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(true);
        stats.setResult(sqlResult);
        return stats;
    }

    static SQLValidateResult failedStats(List<SQLAdvice> sqlAdvices, SQLResult sqlResult) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(false);
        stats.setSqlAdvices(Sets.newHashSet(sqlAdvices));
        stats.setResult(sqlResult);
        return stats;
    }

    @Override
    public String toString() {
        StringBuilder message = new StringBuilder();
        getSqlAdvices().forEach(sqlAdvice -> {
            message.append("\n");
            message.append(String.format("reason: %s", sqlAdvice.getIncapableReason().replace(",", "，")));
            message.append(";");
            message.append(String.format("suggest: %s", sqlAdvice.getSuggestion().replace(",", "，")));
        });

        return String.format("capable:%s %s", isCapable(), message.toString());
    }
}
