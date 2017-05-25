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

package io.kyligence.kap.storage.parquet.adhoc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//TODO: Some workaround ways to make sql readable by spark sql, should replaced it with a more well-designed way
public class SqlConvertUtil {

    private static final Logger logger = LoggerFactory.getLogger(SqlConvertUtil.class);

    private static final Pattern EXTRACT_PATTERN = Pattern.compile("\\s+extract\\s*(\\()\\s*(.*?)\\s*from(\\s+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_PATTERN = Pattern.compile("\\s+from\\s+(\\()\\s*select\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern CAST_PATTERN = Pattern.compile("CAST\\((.*?) (?i)AS\\s*(.*?)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONCAT_PATTERN = Pattern.compile("(['_a-z0-9A-Z]+)\\|\\|(['_a-z0-9A-Z]+)", Pattern.CASE_INSENSITIVE);

    public static String replaceString(String originString, String fromString, String toString) {
        return originString.replace(fromString, toString);
    }

    public static String extractReplace(String originString) {
        Matcher extractMatcher = EXTRACT_PATTERN.matcher(originString);
        String replacedString = originString;
        Map<Integer, Integer> parenthesesPairs = null;

        while (extractMatcher.find()) {
            if (parenthesesPairs == null) {
                parenthesesPairs = findParenthesesPairs(originString);
            }

            String functionStr = extractMatcher.group(2);
            int startIdx = extractMatcher.end(3);
            int endIdx = parenthesesPairs.get(extractMatcher.start(1));
            String extractInner = originString.substring(startIdx, endIdx);
            int originStart = extractMatcher.start(0) + 1;
            int originEnd = endIdx + 1;

            replacedString = replaceString(replacedString, originString.substring(originStart, originEnd), functionStr + "(" + extractInner + ")");
        }

        return replacedString;
    }

    public static String castRepalce(String originString) {
        Matcher castMatcher = CAST_PATTERN.matcher(originString);
        String replacedString = originString;

        while (castMatcher.find()) {
            String castStr = castMatcher.group();
            String type = castMatcher.group(2);
            String supportedType = "";
            switch (type.toUpperCase()) {
                case "INTEGER":
                    supportedType = "int";
                    break;
                case "SHORT":
                    supportedType = "smallint";
                    break;
                case "LONG":
                    supportedType = "bigint";
                    break;
                default:
                    supportedType = type;
            }

            if (!supportedType.equals(type)) {
                String replacedCastStr = castStr.replace(type, supportedType);
                replacedString = replaceString(replacedString, castStr, replacedCastStr);
            }
        }

        return replacedString;
    }

    public static String subqueryRepalce(String originString) {
        Matcher subqueryMatcher = FROM_PATTERN.matcher(originString);
        String replacedString = originString;
        Map<Integer, Integer> parenthesesPairs = null;

        while (subqueryMatcher.find()) {
            if (parenthesesPairs == null) {
                parenthesesPairs = findParenthesesPairs(originString);
            }

            int startIdx = subqueryMatcher.start(1);
            int endIdx = parenthesesPairs.get(startIdx) + 1;

            replacedString = replaceString(replacedString, originString.substring(startIdx, endIdx), originString.substring(startIdx, endIdx) + " as alias");
        }

        return replacedString;
    }

    public static String concatReplace(String originString) {
        Matcher concatMatcher = CONCAT_PATTERN.matcher(originString);
        String replacedString = originString;

        while (concatMatcher.find()) {
            String leftString = concatMatcher.group(1);
            String rightString = concatMatcher.group(2);
            replacedString = replaceString(replacedString, leftString + "||" + rightString, "concat(" + leftString + "," + rightString + ")");
        }

        return replacedString;
    }

    public static String doConvert(String originStr) {
        // Step1.Replace " with `
        String convertedSql = replaceString(originStr, "\"", "`");

        // Step2.Replace extract functions
        convertedSql = extractReplace(convertedSql);

        // Step3.Replace cast type string
        convertedSql = castRepalce(convertedSql);

        // Step4.Replace sub query
        convertedSql = subqueryRepalce(convertedSql);

        // Step5.Replace char_length with length
        convertedSql = replaceString(convertedSql, "char_length", "length");

        // Step6.Replace "||" with concat
        convertedSql = concatReplace(convertedSql);

        return convertedSql;
    }

    private static Map<Integer, Integer> findParenthesesPairs(String sql) {
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
                    default:
                        break;
                }
            }
        }
        return result;
    }
}