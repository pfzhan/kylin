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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import com.google.common.collect.Lists;

public class CognosParenthesesEscapeTransformer implements QueryUtil.IQueryTransformer, IPushDownConverter {
    private static final Pattern FROM_PATTERN = Pattern.compile("\\bfrom(\\s*\\()+(?!\\s*select\\s)",
            Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return StringUtils.isEmpty(sql) ? sql : completion(sql);
    }

    String completion(String sql) {
        Map<Integer, Integer> parenthesesPairs = findParenthesesPairs(sql);
        if (parenthesesPairs.isEmpty()) {
            // parentheses not found
            return sql;
        }

        List<Integer> parentheses = Lists.newArrayList();
        String originSql = sql;
        Matcher m;
        int offset = 0; // use this to locate the index of matched parentheses in the pattern in original sql
        while (true) {
            m = FROM_PATTERN.matcher(sql);
            if (!m.find()) {
                break;
            }

            int i = m.end() - 1;
            while (i > m.start()) {
                if (sql.charAt(i) == '(') {
                    parentheses.add(i + offset);
                }
                i--;
            }

            if (m.end() < sql.length()) {
                offset += m.end();
                sql = sql.substring(m.end());
            } else {
                break;
            }
        }

        List<Integer> indices = Lists.newArrayList();
        parentheses.forEach(index -> {
            indices.add(index);
            indices.add(parenthesesPairs.get(index));
        });
        indices.sort(Integer::compareTo);

        StringBuilder builder = new StringBuilder();
        int lastIndex = 0;
        for (Integer i : indices) {
            builder.append(originSql, lastIndex, i);
            lastIndex = i + 1;
        }
        builder.append(originSql, lastIndex, originSql.length());
        return builder.toString();
    }

    private Map<Integer, Integer> findParenthesesPairs(String sql) {
        Map<Integer, Integer> result = new HashMap<>();
        if (sql.length() > 1) {
            Deque<Integer> lStack = new ArrayDeque<>();
            boolean inStrVal = false;
            for (int i = 0; i < sql.length(); i++) {
                switch (sql.charAt(i)) {
                case '(':
                    if (!inStrVal) {
                        lStack.push(i);
                    }
                    break;
                case ')':
                    if (!inStrVal && !lStack.isEmpty()) {
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

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return transform(originSql, project, defaultSchema);
    }
}
