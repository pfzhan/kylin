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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.security.TransformWithAcl;
import io.kyligence.kap.query.util.CommentParser;
import io.kyligence.kap.query.util.RestoreFromComputedColumn;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryUtil {

    private static final Pattern SELECT_PATTERN = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(limit\\s+[0-9;]+)$", Pattern.CASE_INSENSITIVE);

    static List<IQueryTransformer> queryTransformers = Collections.emptyList();
    static List<IPushDownConverter> pushDownConverters = Collections.emptyList();

    public interface IQueryTransformer {
        String transform(String sql, String project, String defaultSchema);
    }

    public static String massageSql(QueryParams queryParams) {
        String massagedSql = normalMassageSql(queryParams.getKylinConfig(), queryParams.getSql(),
                queryParams.getLimit(), queryParams.getOffset());
        queryParams.setSql(massagedSql);
        massagedSql = transformSql(queryParams);
        QueryContext.current().record("massage");
        return massagedSql;
    }

    public static String massageSqlAndExpandCC(QueryParams queryParams) {
        String massaged = massageSql(queryParams);
        return new RestoreFromComputedColumn().convert(massaged, queryParams.getProject(),
                queryParams.getDefaultSchema());
    }

    private static String transformSql(QueryParams queryParams) {
        // customizable SQL transformation
        initQueryTransformersIfNeeded(queryParams.getKylinConfig(), queryParams.isCCNeeded());
        String sql = queryParams.getSql();
        for (IQueryTransformer t : queryTransformers) {
            if (Thread.currentThread().isInterrupted()) {
                log.error("SQL transformation is timeout and interrupted before {}", t.getClass());
                if (SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).isStopByUser()) {
                    throw new UserStopQueryException("");
                }
                QueryContext.current().getQueryTagInfo().setTimeout(true);
                throw new KylinTimeoutException("SQL transformation is timeout");
            }
            if (t instanceof TransformWithAcl) {
                ((TransformWithAcl) t).setAclInfo(queryParams.getAclInfo());
            }
            sql = t.transform(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        return sql;
    }

    public static String normalMassageSql(KylinConfig kylinConfig, String sql, int limit, int offset) {
        sql = sql.trim();
        sql = sql.replace("\r", " ").replace("\n", System.getProperty("line.separator"));

        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        //Split keywords and variables from sql by punctuation and whitespace character
        List<String> sqlElements = Lists.newArrayList(sql.toLowerCase().split("(?![\\._])\\p{P}|\\s+"));
        if (limit > 0 && !sqlElements.contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sqlElements.contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && !sql.toLowerCase().contains("limit")
                && sql.toLowerCase().matches("^select\\s+\\*\\p{all}*")) {
            sql += ("\nLIMIT " + kylinConfig.getForceLimit());
        }
        return sql;
    }

    static void initQueryTransformersIfNeeded(KylinConfig kylinConfig, boolean isCCNeeded) {
        String[] currentTransformers = queryTransformers.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configTransformers = kylinConfig.getQueryTransformers();
        boolean containsCCTransformer = Arrays.stream(configTransformers)
                .anyMatch(t -> t.equals("io.kyligence.kap.query.util.ConvertToComputedColumn"));
        boolean transformersEqual = Objects.deepEquals(currentTransformers, configTransformers);
        if (transformersEqual && (isCCNeeded || !containsCCTransformer)) {
            return;
        }

        List<IQueryTransformer> transformers = Lists.newArrayList();
        for (String clz : configTransformers) {
            if (!isCCNeeded && clz.equals("io.kyligence.kap.query.util.ConvertToComputedColumn"))
                continue;

            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);

                transformers.add(t);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init query transformer", e);
            }
        }

        queryTransformers = Collections.unmodifiableList(transformers);
        log.debug("SQL transformer: {}", queryTransformers);
    }

    public static String massagePushDownSql(QueryParams queryParams) {
        String sql = queryParams.getSql();
        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);
        initPushDownConvertersIfNeeded(queryParams.getKylinConfig());
        for (IPushDownConverter converter : pushDownConverters) {
            if (Thread.currentThread().isInterrupted()) {
                log.error("Push-down SQL conver transformation is timeout and interrupted before {}",
                        converter.getClass());
                if (SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).isStopByUser()) {
                    throw new UserStopQueryException("");
                }
                QueryContext.current().getQueryTagInfo().setTimeout(true);
                throw new KylinTimeoutException("Push-down SQL convert is timeout");
            }
            if (converter instanceof TransformWithAcl) {
                ((TransformWithAcl) converter).setAclInfo(queryParams.getAclInfo());
            }
            sql = converter.convert(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        return sql;
    }

    static void initPushDownConvertersIfNeeded(KylinConfig kylinConfig) {
        String[] currentConverters = pushDownConverters.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configConverters = kylinConfig.getPushDownConverterClassNames();
        boolean skipInit = Objects.deepEquals(currentConverters, configConverters);

        if (skipInit) {
            return;
        }

        List<IPushDownConverter> converters = Lists.newArrayList();
        for (String clz : configConverters) {
            try {
                IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(clz);
                converters.add(converter);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init pushdown converter", e);
            }
        }
        pushDownConverters = Collections.unmodifiableList(converters);
    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().getName().contains("ParseException") || cause instanceof NoSuchTableException
                    || cause instanceof NoSuchDatabaseException || cause instanceof AccessDeniedException) {
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

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        try {
            errorMsg = errorMsg.trim();

            // move cause to be ahead of sql, calcite creates the message pattern below
            Pattern pattern = Pattern.compile("Error while executing SQL ([\\s\\S]*):(.*):(.*)");
            Matcher matcher = pattern.matcher(errorMsg);
            if (matcher.find()) {
                return matcher.group(2).trim() + ": " + matcher.group(3).trim() + "\nwhile executing SQL: "
                        + matcher.group(1).trim();
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
        while (sql1.startsWith("(")) {
            sql1 = sql1.substring(1).trim();
        }
        return sql1.startsWith("select") || (sql1.startsWith("with") && sql1.contains("select"))
                || (sql1.startsWith("explain") && sql1.contains("select"));
    }

    public static String removeCommentInSql(String sql) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        try {
            return new CommentParser(sql).Input();
        } catch (Exception ex) {
            log.error("Something unexpected while removing comments in the query, return original query", ex);
            return sql;
        }
    }

    public static List<String> splitBySemicolon(String s) {
        List<String> r = Lists.newArrayList();
        StringBuilder sb = new StringBuilder();
        boolean inQuota = false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\'') {
                inQuota = !inQuota;
            }
            if (s.charAt(i) == ';' && !inQuota) {
                if (sb.length() != 0) {
                    r.add(sb.toString());
                    sb = new StringBuilder();
                }
                continue;
            }
            sb.append(s.charAt(i));
        }
        if (sb.length() != 0) {
            r.add(sb.toString());
        }
        return r;
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

    public static KylinConfig getKylinConfig(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        return projectInstance.getConfig();
    }
}