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
package org.apache.kylin.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecAndComp {

    static ForkJoinPool pool = new ForkJoinPool(2);

    public static String changeJoinType(String sql, String targetType) {
        if (targetType.equalsIgnoreCase("default"))
            return sql;

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left"))
                    && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase(Locale.ROOT);
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));
        log.info("The actual sql executed is: " + ret);

        return ret;
    }

    // TODO: udf/calcite function return type should be same as sparksql.
    protected static boolean inToDoList(String fullPath) {
        final String[] toDoList = new String[] {
                // array
                "query/sql_array/query00.sql", "query/sql_array/query01.sql",
                // TODO ifnull()
                "query/sql_function/sql_function_nullHandling/query00.sql",
                "query/sql_function/sql_function_nullHandling/query01.sql",
                "query/sql_function/sql_function_nullHandling/query02.sql",
                "query/sql_function/sql_function_nullHandling/query03.sql",
                "query/sql_function/sql_function_nullHandling/query04.sql",
                "query/sql_computedcolumn/sql_computedcolumn_nullHandling/query00.sql",
                "query/sql_computedcolumn/sql_computedcolumn_nullHandling/query01.sql",
                "query/sql_computedcolumn/sql_computedcolumn_nullHandling/query02.sql",
                "query/sql_computedcolumn/sql_computedcolumn_nullHandling/query03.sql",
                // TODO date_part()
                "query/sql_function/sql_function_DateUDF/query00.sql",
                "query/sql_function/sql_function_DateUDF/query02.sql",
                "query/sql_computedcolumn/sql_computedcolumn_DateUDF/query00.sql",
                // TODO date_trunc()
                "query/sql_computedcolumn/sql_computedcolumn_DateUDF/query04.sql",
                "query/sql_function/sql_function_DateUDF/query06.sql",
                // TODO divde: spark -> 3/2 = 1.5    calcite -> 3/2 = 1
                "query/sql_timestamp/query27.sql",
                // TODO percentile_approx()
                "semi_auto/measures/query00.sql" };
        String[] pathArray = fullPath.split("src/kap-it/src/test/resources/");
        if (pathArray.length < 2)
            return false;
        String relativePath = pathArray[1];
        if (Arrays.asList(toDoList).contains(relativePath)) {
            log.info("\"{}\" is in TODO List, skipmetadata check.", fullPath);
            return true;
        }
        return false;
    }

    @SneakyThrows
    public static QueryResult queryWithSpark(String prj, String originSql, String joinType, String sqlPath) {
        int index = sqlPath.lastIndexOf('/');
        String resultFilePath = "";
        String schemaFilePath = "";
        if (index > 0) {
            resultFilePath = sqlPath.substring(0, index) + "/result-" + joinType + sqlPath.substring(index) + ".json";
            schemaFilePath = sqlPath.substring(0, index) + "/result-" + joinType + sqlPath.substring(index) + ".schema";
        }
        try {
            if (index > 0 && Files.exists(Paths.get(resultFilePath)) && Files.exists(Paths.get(schemaFilePath))) {
                StructType schema = StructType.fromDDL(new String(Files.readAllBytes(Paths.get(schemaFilePath))));
                val structs = Arrays.stream(schema.fields()).map(SparderTypeUtil::convertSparkFieldToJavaField)
                        .collect(Collectors.toList());
                val lines = Files.readAllLines(Paths.get(resultFilePath)).stream().map(s -> {
                    List<String> result = Lists.newArrayList();
                    try {
                        val tree = JsonUtil.readValueAsTree(s);
                        for (StructField structField : structs) {
                            val node = tree.get(structField.getName());
                            if (node == null) {
                                result.add(null);
                            } else if (structField.getDataTypeName().startsWith("ARRAY")) {
                                result.add(node.toString());
                            } else {
                                result.add(node.asText());
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return result;
                }).collect(Collectors.toList());
                return new QueryResult(lines, lines.size(), structs);
            }
        } catch (Exception e) {
            log.warn("try to use cache failed, compare with spark {}", sqlPath, e);
        }
        String compareSql = getCompareSql(sqlPath);
        if (StringUtils.isEmpty(compareSql)) {
            compareSql = changeJoinType(originSql, joinType);
        } else {
            compareSql = changeJoinType(compareSql, joinType);
        }

        QueryParams queryParams = new QueryParams(prj, compareSql, "default", false);
        queryParams.setKylinConfig(NProjectManager.getProjectConfig(prj));
        String afterConvert = PushDownUtil.massagePushDownSql(queryParams);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = removeDataBaseInSql(afterConvert);
        val ds = querySparkSql(sqlForSpark);
        try {
            if (StringUtils.isNotEmpty(resultFilePath)) {
                Files.deleteIfExists(Paths.get(resultFilePath));
                ds.coalesce(1).write().json(resultFilePath);
                Files.deleteIfExists(Paths.get(schemaFilePath));
                Files.write(Paths.get(schemaFilePath), ds.schema().toDDL().getBytes());
            }
        } catch (Exception e) {
            log.warn("persist {} failed", sqlPath, e);
        }
        val rows = ds.collectAsList();
        val structs = Arrays.stream(ds.schema().fields()).map(SparderTypeUtil::convertSparkFieldToJavaField)
                .collect(Collectors.toList());
        return new QueryResult(rows.stream().map(r -> {
            List<String> result = Lists.newArrayList();
            for (int i = 0; i < r.size(); i++) {
                val structField = structs.get(i);
                val node = r.get(i);
                if (node == null) {
                    result.add(null);
                } else {
                    result.add(node.toString());
                }
            }
            return result;
        }).collect(Collectors.toList()), rows.size(), structs);
    }

    public static String removeDataBaseInSql(String originSql) {
        return originSql.replaceAll("(?i)edw\\.", "") //
                .replaceAll("`edw`\\.", "") //
                .replaceAll("\"EDW\"\\.", "") //
                .replaceAll("`EDW`\\.", "") //
                .replaceAll("`SSB`\\.", "") //
                .replaceAll("`ssb`\\.", "") //
                .replaceAll("\"SSB\"\\.", "") //
                .replaceAll("(?i)SSB\\.", "") //
                .replaceAll("(?i)default\\.", "") //
                .replaceAll("`default`\\.", "") //
                .replaceAll("\"DEFAULT\"\\.", "") //
                .replaceAll("`DEFAULT`\\.", "") //
                .replaceAll("(?i)TPCH\\.", "") //
                .replaceAll("`TPCH`\\.", "") //
                .replaceAll("`tpch`\\.", "") //
                .replaceAll("(?i)TDVT\\.", "") //
                .replaceAll("\"TDVT\"\\.", "") //
                .replaceAll("`TDVT`\\.", "") //
                .replaceAll("\"POPHEALTH_ANALYTICS\"\\.", "") //
                .replaceAll("`POPHEALTH_ANALYTICS`\\.", "") //
                .replaceAll("(?i)ISSUES\\.", "");
    }

    public static List<Pair<String, String>> fetchQueries(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls(sqlFolder);
    }

    public static List<Pair<String, String>> fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFolder = new File(folder);
        List<Pair<String, String>> originalSqls = retrieveITSqls(sqlFolder);
        if (end > originalSqls.size()) {
            end = originalSqls.size();
        }
        return originalSqls.subList(start, end);
    }

    @SuppressWarnings("unused")
    private static List<Pair<String, String>> retrieveAllQueries(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles((dir, name) -> name.startsWith("sql_"));
            }
        }
        List<Pair<String, String>> allSqls = new ArrayList<>();
        for (File file : Objects.requireNonNull(sqlFiles)) {
            allSqls.addAll(retrieveITSqls(file));
        }
        return allSqls;
    }

    private static List<Pair<String, String>> retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null && file.exists() && file.listFiles() != null) {
            sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
        }
        List<Pair<String, String>> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, (o1, o2) -> {
            final String idxStr1 = o1.getName().replaceAll("\\D", "");
            final String idxStr2 = o2.getName().replaceAll("\\D", "");
            if (idxStr1.isEmpty() || idxStr2.isEmpty()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
            return Integer.parseInt(idxStr1) - Integer.parseInt(idxStr2);
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex)
                    : sqlStatement;
            ret.add(Pair.newPair(sqlFile.getCanonicalPath(), sql + '\n'));
        }
        return ret;
    }

    public static List<Pair<String, String>> doFilter(List<Pair<String, String>> sources,
            final Set<String> exclusionList) {
        Preconditions.checkArgument(sources != null);
        Set<String> excludes = Sets.newHashSet(exclusionList);
        return sources.stream().filter(pair -> {
            final String[] splits = pair.getFirst().split(File.separator);
            return !excludes.contains(splits[splits.length - 1]);
        }).collect(Collectors.toList());
    }

    public static Dataset<Row> queryModelWithoutCompute(String prj, String sql) {
        return queryModelWithoutCompute(prj, sql, null);
    }

    @SneakyThrows
    public static Dataset<Row> queryModelWithoutCompute(String prj, String sql, List<String> parameters) {
        try {
            SparderEnv.skipCompute();
            QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(prj), sql, prj, 0, 0, "DEFAULT",
                    true);
            sql = QueryUtil.massageSql(queryParams);
            List<String> parametersNotNull = parameters == null ? new ArrayList<>() : parameters;
            return queryModel(prj, sql, parametersNotNull);
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    public static Dataset<Row> queryModel(String prj, String sql) throws SQLException {
        return queryModel(prj, sql, null);
    }

    public static Dataset<Row> queryModel(String prj, String sql, List<?> parameters) throws SQLException {
        queryModelWithMeta(prj, sql, parameters);
        return SparderEnv.getDF();
    }

    public static EnhancedQueryResult queryModelWithOlapContext(String prj, String joinType, String sql) {
        return queryModelWithOlapContext(prj, joinType, sql, null);
    }

    public static EnhancedQueryResult queryModelWithOlapContext(String prj, String joinType, String sql,
            List<String> parameters) {
        QueryResult queryResult = queryModelWithMassage(prj, changeJoinType(sql, joinType), parameters);
        val ctxs = OLAPContext.getThreadLocalContexts();
        OLAPContext.clearThreadLocalContexts();
        return new EnhancedQueryResult(queryResult, ctxs);
    }

    public static QueryResult queryModelWithMassage(String prj, String sqlText, List<String> parameters) {
        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(prj), sqlText, prj, 0, 0, "DEFAULT",
                true);
        sqlText = QueryUtil.massageSql(queryParams);
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            long startTs = System.currentTimeMillis();
            QueryResult queryResult = queryModelWithMeta(prj, sqlText, parameters);
            log.info("Query with Model Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return queryResult;
        } catch (Throwable e) {
            log.error("There is no cube can be used for query [{}]", sqlText);
            log.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    private static QueryResult queryModelWithMeta(String prj, String sql, List<?> parameters) throws SQLException {
        SparderEnv.setDF(null); // clear last df
        // if this config is on
        // SQLS like "where 1<>1" will be optimized and run locally and no dataset will be returned
        String prevRunLocalConf = Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE");
        try {
            QueryExec queryExec = new QueryExec(prj, NProjectManager.getProjectConfig(prj), true);
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    queryExec.setPrepareParam(i, parameters.get(i));
                }
            }
            return queryExec.executeQuery(sql);
        } finally {
            if (prevRunLocalConf != null) {
                Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf);
            } else {
                Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
            }
        }
    }

    public static Dataset<Row> querySparkSql(String sqlText) {
        return SparderEnv.getSparkSession().sql(sqlText);
    }

    private static String getCompareSql(String originSqlPath) {
        if (!originSqlPath.endsWith(".sql")) {
            return "";
        }
        File file = new File(originSqlPath + ".expected");
        if (!file.exists())
            return "";

        try {
            return FileUtils.readFileToString(file, Charset.defaultCharset());
        } catch (IOException e) {
            log.error("meet error when reading compared spark sql from {}", file.getAbsolutePath());
            return "";
        }
    }

    public static List<List<String>> queryCubeWithJDBC(String prj, String sql) throws Exception {
        return new QueryExec(prj, KylinConfig.getInstanceFromEnv(), true).executeQuery(sql).getRows();
    }

    public static void execAndCompareQueryList(List<String> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        List<Pair<String, String>> transformed = queries.stream().map(q -> Pair.newPair("", q))
                .collect(Collectors.toList());
        execAndCompare(transformed, prj, compareLevel, joinType);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        execAndCompare(queries, prj, compareLevel, joinType, null);
    }

    @SneakyThrows
    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType, Pair<String, String> views) {
        QueryContext.current().close();
        QueryContext.current().setProject(prj);
        pool.submit(() -> queries.parallelStream().forEach(query -> {
            try (val qc = QueryContext.current()) {
                log.info("Exec and compare query ({}) :{}", joinType, query.getFirst());
                String sql = changeJoinType(query.getSecond(), joinType);
                long startTime = System.currentTimeMillis();
                EnhancedQueryResult modelResult = queryModelWithOlapContext(prj, joinType, sql);
                List<StructField> cubeColumns = modelResult.getColumns();
                if (compareLevel != CompareLevel.NONE) {
                    String newSql = sql;
                    if (views != null) {
                        newSql = sql.replaceAll(views.getFirst(), views.getSecond());
                    }
                    long startTs = System.currentTimeMillis();
                    val sparkResult = queryWithSpark(prj, newSql, joinType, query.getFirst());
                    if ((compareLevel == CompareLevel.SAME || compareLevel == CompareLevel.SAME_ORDER)
                            && sparkResult.getColumns().size() != cubeColumns.size()) {
                        log.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType,
                                query, cubeColumns, sparkResult.getColumns());
                        throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
                    }
                    if (!inToDoList(query.getFirst()) && compareLevel == CompareLevel.SAME) {
                        QueryResultComparator.compareColumnType(cubeColumns, sparkResult.getColumns());
                    }

                    log.info("Query with Spark Duration(ms): {}", System.currentTimeMillis() - startTs);

                    startTs = System.currentTimeMillis();
                    if (!QueryResultComparator.compareResults(sparkResult, modelResult.getQueryResult(),
                            compareLevel)) {
                        log.error("Failed on compare query ({}) :{}", joinType, query);
                        throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                    }
                    log.info("Compare Duration(ms): {}", System.currentTimeMillis() - startTs);
                } else {
                    log.info("result comparison is not available");
                }
                log.info("The query ({}) : {} cost {} (ms)", joinType, query.getFirst(),
                        System.currentTimeMillis() - startTime);
            }
        })).get();
    }

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ORDER, // exec and compare order
        SAME_ROWCOUNT, //
        SUBSET, //
        NONE, // batch execute

    }

    @Data
    @AllArgsConstructor
    public static class EnhancedQueryResult {

        @Delegate
        QueryResult queryResult;

        public Collection<OLAPContext> olapContexts;

        public QueryResult getQueryResult() {
            return queryResult;
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class SparkResult {

        String schema;

        List<String> resultData;
    }
}
