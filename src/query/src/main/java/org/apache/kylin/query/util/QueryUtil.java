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

package org.apache.kylin.query.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kyligence.kap.metadata.project.NProjectManager;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class QueryUtil {

    protected static final Logger logger = LoggerFactory.getLogger(QueryUtil.class);

    static List<IQueryTransformer> queryTransformers = Collections.emptyList();

    public interface IQueryTransformer {
        String transform(String sql, String project, String defaultSchema);
    }

    public static String massageSql(String sql, String project, int limit, int offset, String defaultSchema) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        KylinConfig kylinConfig = projectInstance.getConfig();
        return massageSql(kylinConfig, sql, project, limit, offset, defaultSchema);
    }

    static String massageSql(KylinConfig kylinConfig, String sql, String project, int limit, int offset,
            String defaultSchema) {
        sql = sql.trim();
        sql = sql.replace("\r", " ").replace("\n", System.getProperty("line.separator"));

        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        if (limit > 0 && !sql.toLowerCase().contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sql.toLowerCase().contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && !sql.toLowerCase().contains("limit")
                && sql.toLowerCase().matches("^select\\s+\\*\\p{all}*")) {
            sql += ("\nLIMIT " + kylinConfig.getForceLimit());
        }

        // customizable SQL transformation
        initQueryTransformersIfNeeded(kylinConfig);
        for (IQueryTransformer t : queryTransformers) {
            sql = t.transform(sql, project, defaultSchema);
        }
        return sql;
    }

    static void initQueryTransformersIfNeeded(KylinConfig kylinConfig) {
        String[] currentTransformers = queryTransformers.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configTransformers = kylinConfig.getQueryTransformers();
        boolean skipInit = Objects.deepEquals(currentTransformers, configTransformers);
        if (skipInit) {
            return;
        }

        List<IQueryTransformer> transformers = Lists.newArrayList();
        for (String clz : configTransformers) {
            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);
                transformers.add(t);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init query transformer", e);
            }
        }

        queryTransformers = Collections.unmodifiableList(transformers);
    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().getName().contains("ParseException")) {
                msg = cause.getMessage();
                break;
            }

            if (cause.getClass().getName().contains("ArithmeticException")) {
                msg = "ArithmeticException: " + cause.getMessage();
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String massagePushdownSql(String sql, String project, String defaultSchema, boolean isPrepare) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        for (String converterName : kylinConfig.getPushDownConverterClassNames()) {
            IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(converterName);
            String convertedSql = converter.convert(sql, project, defaultSchema, isPrepare);
            if (!sql.equals(convertedSql)) {
                logger.info("The query is converted to {} after applying converter {}", convertedSql, converterName);
                sql = convertedSql;
            }
        }
        return sql;
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        try {
            // make one line
            errorMsg = errorMsg.replaceAll("\\s", " ");

            // move cause to be ahead of sql, calcite creates the message pattern below
            Pattern pattern = Pattern.compile("Error while executing SQL \"(.*)\":(.*)");
            Matcher matcher = pattern.matcher(errorMsg);
            if (matcher.find()) {
                return matcher.group(2).trim() + "\nwhile executing SQL: \"" + matcher.group(1).trim() + "\"";
            } else
                return errorMsg;
        } catch (Exception e) {
            return errorMsg;
        }
    }

    public static boolean isSelectStatement(String sql) {
        String sql1 = sql.toLowerCase();
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();
        return sql1.startsWith("select") || (sql1.startsWith("with") && sql1.contains("select"))
                || (sql1.startsWith("explain") && sql1.contains("select"));
    }

    public static String removeCommentInSql(String sql1) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        final String[] commentPatterns = new String[] { "--[^\r\n]*", "/\\*[^\\*/]*" };
        final int[] endOffset = new int[] { 0, 2 };

        for (int i = 0; i < commentPatterns.length; i++) {
            String commentPattern = commentPatterns[i];
            Pattern pattern = Pattern.compile(commentPattern);
            Matcher matcher = pattern.matcher(sql1);

            while (matcher.find()) {
                if (matcher.start() == 0) {
                    sql1 = sql1.substring(matcher.end() + endOffset[i]).trim();
                } else if ((matcher.start() > 0 && sql1.charAt(matcher.start() - 1) != '\'')) {
                    sql1 = (sql1.substring(0, matcher.start()) + sql1.substring(matcher.end() + endOffset[i])).trim();
                }
                matcher = pattern.matcher(sql1);
            }
        }

        return sql1;
    }
}
