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
package org.apache.kylin.source.adhocquery;

import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;

//TODO: Some workaround ways to make sql readable by hive parser, should replaced it with a more well-designed way
public class HivePushDownConverter implements IPushDownConverter, IKeep {

    private static final Logger logger = LoggerFactory.getLogger(HivePushDownConverter.class);

    private static final Pattern SELECT_PATTERN = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(limit\\s+[0-9;]+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern GROUPING_SETS_PATTERN = Pattern
            .compile("group\\s+by\\s+(grouping\\s+sets\\s*\\(([`_a-z0-9A-Z(),\\s]+)\\))", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("[`_a-z0-9A-Z]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern OVERLAY_PLACING_FROM_PATTERN = Pattern.compile(
            "\\b(overlay)\\s*\\(\\s*([^,]+)(\\s+placing\\s+)([^,]+)(\\s+from\\s+)([^,]+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern OVERLAY_PLACING_FROM_FOR_PATTERN = Pattern.compile(
            "\\b(overlay)\\s*\\(\\s*([^,]+)(\\s+placing\\s+)([^,]+)(\\s+from\\s+)([^,]+)(\\s+for\\s+)([^,]+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);

    public static String replaceString(String originString, String fromString, String toString) {
        return originString.replace(fromString, toString);
    }

    /***
     * convert for overlay(string1 PLACING string2 FROM integer [ FOR integer2 ])
     * @param originString
     * @return
     */
    public static String overlayReplace(String originString) {
        Matcher overlayMatcher = OVERLAY_PLACING_FROM_PATTERN.matcher(originString);
        Matcher overlayForMatcher = OVERLAY_PLACING_FROM_FOR_PATTERN.matcher(originString);
        String result = originString;

        while (overlayMatcher.find()) {
            String string1 = overlayMatcher.group(2);
            String string2 = overlayMatcher.group(4);
            String integer = overlayMatcher.group(6);

            String tmpString = "concat(substring(" + string1 + ",0," + integer + "-1)," + string2 + ",substring("
                    + string1 + "," + integer + "+" + "char_length(" + string2 + ")))";
            if (overlayForMatcher.find()) {
                integer = overlayForMatcher.group(6);
                String integer2 = overlayForMatcher.group(8);
                tmpString = "concat(substring(" + string1 + ",0," + integer + "-1)," + string2 + ",substring(" + string1
                        + "," + integer + "+" + integer2 + "))";
                result = replaceString(result, overlayForMatcher.group(), tmpString);
            } else {
                result = replaceString(result, overlayMatcher.group(), tmpString);
            }
            overlayMatcher = OVERLAY_PLACING_FROM_PATTERN.matcher(result);
            overlayForMatcher = OVERLAY_PLACING_FROM_FOR_PATTERN.matcher(result);
        }
        return result;
    }

    public static String addLimit(String originString) {
        Matcher selectMatcher = SELECT_PATTERN.matcher(originString);
        Matcher limitMatcher = LIMIT_PATTERN.matcher(originString);
        String replacedString = originString;

        if (selectMatcher.find() && !limitMatcher.find()) {
            if (originString.endsWith(";")) {
                replacedString = originString.replaceAll(";+$", "");
            }

            replacedString = replacedString.concat(" limit 1");
        }

        return replacedString;
    }

    public static String groupingSetsReplace(String originString) {
        Matcher groupingSetsMatcher = GROUPING_SETS_PATTERN.matcher(originString);
        String replacedString = originString;

        if (groupingSetsMatcher.find()) {
            String toBeReplaced = groupingSetsMatcher.group(1);
            String columns = groupingSetsMatcher.group(2);
            Matcher columnMatcher = COLUMN_NAME_PATTERN.matcher(columns);
            LinkedHashSet<String> columnSet = new LinkedHashSet<>();

            while (columnMatcher.find()) {
                columnSet.add(columnMatcher.group());
            }

            replacedString = replacedString.replace(toBeReplaced, String.join(",", columnSet) + " " + toBeReplaced);
        }

        return replacedString;
    }

    private static String doConvert(String originStr, boolean isPrepare) {

        // convert for overlay(string1 PLACING string2 FROM integer [ FOR integer2 ])
        String convertedSql = overlayReplace(originStr);

        // Add limit 1 for prepare select sql to speed up
        if (isPrepare) {
            convertedSql = addLimit(convertedSql);
        }

        // Support grouping sets with none group by
        convertedSql = groupingSetsReplace(convertedSql);

        return convertedSql;
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema, boolean isPrepare) {
        String sql = doConvert(originSql, isPrepare);
        logger.info("After transformed by HivePushDownConverter, the corrected sql is :{}", sql);
        return sql;
    }
}
