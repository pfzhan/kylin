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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeep;

public class CognosParenthesesEscape implements QueryUtil.IQueryTransformer, IKeep {
    private static final Pattern FROM_PATTERN = Pattern.compile("\\s+from\\s+(\\s*\\(\\s*)+(?!\\s*select\\s)",
            Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KapConfig.getInstanceFromEnv().isCognosParenthesesEscapeEnabled()) {
            return sql;
        }

        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        return completion(sql);
    }

    String completion(String sql) {
        Map<Integer, Integer> parenthesesPairs = findParenthesesPairs(sql);
        if (parenthesesPairs.isEmpty()) {
            // parentheses not found
            return sql;
        }

        List<Integer> parentheses = Lists.newArrayList();
        StringBuilder result = new StringBuilder(sql);

        Matcher m;
        while (true) {
            m = FROM_PATTERN.matcher(sql);
            if (!m.find()) {
                break;
            }

            int i = m.end() - 1;
            while (i > m.start()) {
                if (sql.charAt(i) == '(') {
                    parentheses.add(i);
                }
                i--;
            }

            if (m.end() < sql.length()) {
                sql = sql.substring(m.end());
            } else {
                break;
            }
        }

        Collections.sort(parentheses);
        for (int i = 0; i < parentheses.size(); i++) {
            result.deleteCharAt(parentheses.get(i) - i);
            result.deleteCharAt(parenthesesPairs.get(parentheses.get(i)) - i - 1);
        }
        return result.toString();
    }

    private Map<Integer, Integer> findParenthesesPairs(String sql) {
        Map<Integer, Integer> result = new HashMap<>();
        if (sql.length() > 1) {
            Stack<Integer> lStack = new Stack<>();
            boolean inStrVal = false;
            for (int i = 0; i < sql.length(); i++) {
                switch (sql.charAt(i)) {
                case '(':
                    if (!inStrVal) {
                        lStack.push(i);
                    }
                    break;
                case ')':
                    if (!inStrVal && !lStack.empty()) {
                        result.put(lStack.pop(), i);
                    }
                    break;
                case '\'':
                    inStrVal = !inStrVal;
                    break;
                default:
                    break;
                }
            }
        }
        return result;
    }
}
