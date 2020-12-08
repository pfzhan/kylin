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

package io.kyligence.kap.query.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryModelPriorities {

    private QueryModelPriorities() {
    }

    private static final Pattern MODEL_PRIORITY_PATTERN = Pattern.compile("SELECT\\W+/\\*\\+\\W*(.+)\\*/");

    private static String getHint(String sql) {
        Matcher matcher = MODEL_PRIORITY_PATTERN.matcher(sql.toUpperCase());
        if (matcher.find()) {
            return matcher.group(1).trim();
        } else {
            return "";
        }
    }

    public static String[] getModelPrioritiesFromComment(String sql) {
        String hint = getHint(sql).toUpperCase();
        if (hint.isEmpty() || hint.indexOf("MODEL_PRIORITY(") != 0) {
            return new String[0];
        }

        String[] modelHints = hint.replace("MODEL_PRIORITY(", "").replace(")", "").split(",");
        for (int i = 0; i < modelHints.length; i++) {
            modelHints[i] = modelHints[i].trim();
        }
        return modelHints;
    }
}
