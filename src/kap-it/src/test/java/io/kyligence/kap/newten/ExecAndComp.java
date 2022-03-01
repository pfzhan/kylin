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
package io.kyligence.kap.newten;

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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.common.SparderQueryTest;
import org.apache.spark.sql.types.StructType;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.util.RecAndQueryCompareUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecAndComp {
    private static final int COMPARE_DEVIATION = 5;

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

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, String prj, String joinType,
            Map<String, CompareEntity> recAndQueryResult) {
        queries.parallelStream().forEach(query -> {
            log.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);
            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
            }
            Dataset<Row> kapResult = queryWithKap(prj, joinType, sqlAndAddedLimitSql, recAndQueryResult);
            addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst(), query.getFirst());
            List<Row> kapRows = SparderQueryTest.castDataType(kapResult, sparkResult).collectAsList();
            List<Row> sparkRows = sparkResult.collectAsList();
            if (!compareResults(normRows(sparkRows), normRows(kapRows), CompareLevel.SUBSET)) {
                throw new IllegalArgumentException("Result not match");
            }
        });
    }

    public static void execAndCompareDynamic(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType, Map<String, CompareEntity> recAndQueryResult) {
        queries.parallelStream().forEach(path2Sql -> {
            try {
                log.info("Exec and compare query ({}) :{}", joinType, path2Sql.getFirst());
                String sql = changeJoinType(path2Sql.getSecond(), joinType);
                long startTime = System.currentTimeMillis();
                List<String> params = KylinTestBase.getParameterFromFile(new File(path2Sql.getFirst().trim()));

                recAndQueryResult.putIfAbsent(path2Sql.getSecond(), new CompareEntity());
                final CompareEntity entity = recAndQueryResult.get(path2Sql.getSecond());
                entity.setSql(path2Sql.getSecond());
                Dataset<Row> cubeResult = queryModelWithoutCompute(prj, sql, params);
                Dataset<Row> sparkResult = queryWithSpark(prj, path2Sql.getSecond(), joinType,
                        path2Sql.getFirst());

                List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();
                List<Row> sparkRows = sparkResult.collectAsList();
                if (!compareResults(normRows(sparkRows), normRows(kapRows), compareLevel)) {
                    log.error("Failed on compare query ({}) :{}", joinType, sql);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + sql + " result not match");
                }

                entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
                OLAPContext.clearThreadLocalContexts();
                addQueryPath(recAndQueryResult, path2Sql, sql);
                log.info("The query ({}) : {} cost {} (ms)", joinType, sql, System.currentTimeMillis() - startTime);

            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "meet ERROR when running query (" + joinType + ") :\n" + path2Sql.getSecond(), e);
            }
        });
    }

    public static void execAndCompareQueryList(List<String> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        List<Pair<String, String>> transformed = queries.stream().map(q -> Pair.newPair("", q))
                .collect(Collectors.toList());
        execAndCompare(transformed, prj, compareLevel, joinType);
    }

    // TODO: udf/calcite function return type should be same as sparksql.
    private static boolean inToDoList(String fullPath) {
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

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        execAndCompare(queries, prj, compareLevel, joinType, null, null);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType, Map<String, CompareEntity> recAndQueryResult, Pair<String, String> views) {
        queries.parallelStream().forEach(query -> {
            log.info("Exec and compare query ({}) :{}", joinType, query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);
            long startTime = System.currentTimeMillis();
            EnhancedQueryResult modelResult = queryModelWithOlapContext(prj, joinType, sql);
            if (recAndQueryResult != null) {
                val entity = recAndQueryResult.computeIfAbsent(query.getSecond(), k -> new CompareEntity());
                entity.setSql(sql);
                entity.setOlapContexts(modelResult.olapContexts);
            }
            List<StructField> cubeColumns = modelResult.getColumns();
            Dataset<Row> cubeResult = SparderEnv.getDF();
            addQueryPath(recAndQueryResult, query, sql);
            if (compareLevel != CompareLevel.NONE) {
                String newSql = sql;
                if (views != null) {
                    newSql = sql.replaceAll(views.getFirst(), views.getSecond());
                }
                long startTs = System.currentTimeMillis();
                Dataset<Row> sparkResult = queryWithSpark(prj, newSql, joinType, query.getFirst());
                if ((compareLevel == CompareLevel.SAME || compareLevel == CompareLevel.SAME_ORDER)
                        && sparkResult.schema().fields().length != cubeResult.schema().fields().length) {
                    log.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType,
                            query, cubeResult.schema().fieldNames(), sparkResult.schema().fieldNames());
                    throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
                }
                if (!inToDoList(query.getFirst()) && compareLevel == CompareLevel.SAME) {
                    SparderQueryTest.compareColumnTypeWithCalcite(cubeColumns, sparkResult.schema());
                }

                List<Row> sparkRows = sparkResult.collectAsList();
                log.info("Query with Spark Duration(ms): {}", System.currentTimeMillis() - startTs);

                startTs = System.currentTimeMillis();
                List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();
                log.info("Collect again Duration(ms): {}", System.currentTimeMillis() - startTs);

                startTs = System.currentTimeMillis();
                if (!compareResults(normRows(sparkRows), normRows(kapRows), compareLevel)) {
                    log.error("Failed on compare query ({}) :{}", joinType, query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
                log.info("Compare Duration(ms): {}", System.currentTimeMillis() - startTs);
            } else {
                cubeResult.persist();
                log.debug("result comparision is not available, part of the cube results: " + cubeResult.count());
                log.debug(cubeResult.showString(10, 20, false));
                cubeResult.unpersist();
            }
            log.info("The query ({}) : {} cost {} (ms)", joinType, query, System.currentTimeMillis() - startTime);
        });
    }

    public static boolean execAndCompareQueryResult(Pair<String, String> queryForKap,
            Pair<String, String> queryForSpark, String joinType, String prj,
            Map<String, CompareEntity> recAndQueryResult) {
        String sqlForSpark = changeJoinType(queryForSpark.getSecond(), joinType);
        addQueryPath(recAndQueryResult, queryForSpark, sqlForSpark);
        Dataset<Row> sparkResult = queryWithSpark(prj, queryForSpark.getSecond(), joinType,
                queryForSpark.getFirst());
        List<Row> sparkRows = sparkResult.collectAsList();

        String sqlForKap = changeJoinType(queryForKap.getSecond(), joinType);
        Dataset<Row> cubeResult = queryWithKap(prj, joinType, Pair.newPair(sqlForKap, sqlForKap), null);
        List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();

        return sparkRows.equals(kapRows);
    }

    public static List<Row> normRows(List<Row> rows) {
        List<Row> rowList = Lists.newArrayList();
        rows.forEach(row -> rowList.add(SparderQueryTest.prepareRow(row)));
        return rowList;
    }

    public static void addQueryPath(Map<String, CompareEntity> recAndQueryResult, Pair<String, String> query,
            String modifiedSql) {
        if (recAndQueryResult == null) {
            return;
        }

        Preconditions.checkState(recAndQueryResult.containsKey(modifiedSql));
        recAndQueryResult.get(modifiedSql).setFilePath(query.getFirst());
    }

    private static Dataset<Row> queryWithKap(String prj, String joinType, Pair<String, String> pair,
            Map<String, CompareEntity> compareEntityMap) {
        Dataset<Row> rowDataset;
        if (compareEntityMap != null) {
            compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
            final CompareEntity entity = compareEntityMap.get(pair.getFirst());
            entity.setSql(pair.getFirst());
            rowDataset = queryModelWithoutCompute(prj, changeJoinType(pair.getSecond(), joinType));
            entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
            OLAPContext.clearThreadLocalContexts();
        } else {
            rowDataset = queryModelWithoutCompute(prj, changeJoinType(pair.getSecond(), joinType));
        }
        return rowDataset;
    }

    @SneakyThrows
    public static Dataset<Row> queryWithSpark(String prj, String originSql, String joinType, String sqlPath) {
        int index = sqlPath.lastIndexOf('/');
        String resultFilePath = "";
        String schemaFilePath = "";
        if (index > 0) {
            resultFilePath = sqlPath.substring(0, index) + "/result-" + joinType + sqlPath.substring(index) + ".json";
            schemaFilePath = sqlPath.substring(0, index) + "/result-" + joinType + sqlPath.substring(index) + ".schema";
        }
        try {
            if (Files.exists(Paths.get(resultFilePath)) && Files.exists(Paths.get(schemaFilePath))) {
                StructType schema = StructType.fromDDL(new String(Files.readAllBytes(Paths.get(schemaFilePath))));
                return SparderEnv.getSparkSession().read().schema(schema).json(resultFilePath);
            }
        } catch (Exception e) {
            log.warn("try to use cache failed, compare with spark {}", sqlPath, e);
        }
        String compareSql = getCompareSql(sqlPath);
        if (StringUtils.isEmpty(compareSql))
            compareSql = originSql;

        QueryParams queryParams = new QueryParams(prj, compareSql, "default", false);
        queryParams.setKylinConfig(QueryUtil.getKylinConfig(prj));
        String afterConvert = QueryUtil.massagePushDownSql(queryParams);
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
        return ds;
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

    public static boolean compareResults(List<Row> expectedResult, List<Row> actualResult, CompareLevel compareLevel) {
        boolean good = true;

        switch (compareLevel) {
        case SAME_ORDER:
            good = expectedResult.equals(actualResult);
            break;
        case SAME:
            good = compareResultInLevelSame(expectedResult, actualResult);
            break;
        case SAME_ROWCOUNT:
            good = expectedResult.size() == actualResult.size();
            break;
        case SUBSET: {
            good = compareResultInLevelSubset(expectedResult, actualResult);
            break;
        }
        case SAME_WITH_DEVIATION_ALLOWED:
            good = compareResultInLevelSame(expectedResult, actualResult)
                    || compareTwoRowsWithDeviationAllowed(expectedResult, actualResult);
            break;
        default:
            break;

        }

        if (!good) {
            log.error("Result not match");
            printRows("expected", expectedResult);
            printRows("actual", actualResult);
        }
        return good;
    }

    private static boolean compareResultInLevelSubset(List<Row> expectedResult, List<Row> actualResult) {
        for (Row eRow : actualResult) {
            if (!expectedResult.contains(eRow)) {
                return false;
            }
        }

        return true;
    }

    private static boolean compareResultInLevelSame(List<Row> expectedResult, List<Row> actualResult) {
        if (expectedResult.size() == actualResult.size()) {
            if (expectedResult.size() > 15000) {
                throw new RuntimeException(
                        "please modify the sql to control the result size that less than 15000 and it has "
                                + actualResult.size() + " rows");
            }
            for (Row eRow : expectedResult) {
                if (!actualResult.contains(eRow)) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    private static boolean compareTwoRowsWithDeviationAllowed(List<Row> expectedRows, List<Row> actualRows) {
        for (int i = 0; i < expectedRows.size(); i++) {
            val expectedRow = expectedRows.get(i);
            val actualRow = actualRows.get(i);

            for (int j = 0; j < expectedRow.size(); j++) {
                val expectedValue = expectedRow.get(j);
                val actualValue = actualRow.get(j);

                if (expectedValue instanceof Double && actualValue instanceof Double) {
                    double distance = (double) expectedValue - (double) actualValue;
                    if (Math.abs(distance) > COMPARE_DEVIATION) {
                        return false;
                    }
                } else if (!String.valueOf(expectedValue).equals(String.valueOf(actualValue))) {
                    return false;
                }
            }
        }

        return true;
    }

    private static void printRows(String source, List<Row> rows) {
        log.debug("***********" + source + " start**********");
        rows.forEach(row -> log.info(row.mkString(" | ")));
        log.debug("***********" + source + " end**********");
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
            QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(prj), sql, prj, 0, 0, "DEFAULT", true);
            sql = QueryUtil.massageSql(queryParams);
            List<Object> parametersNotNull = parameters == null ? new ArrayList<>() : new ArrayList<>(parameters);
            return queryModel(prj, sql, parametersNotNull);
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    public static Dataset<Row> queryModel(String prj, String sql) throws SQLException {
        return queryModel(prj, sql, null);
    }

    public static Dataset<Row> queryModel(String prj, String sql, List<Object> parameters) throws SQLException {
        queryModelWithMeta(prj, sql, parameters);
        return SparderEnv.getDF();
    }

    private static EnhancedQueryResult queryModelWithOlapContext(String prj, String joinType, String sql) {
        QueryResult queryResult = queryModelWithMassage(prj, changeJoinType(sql, joinType));
        val ctxs = OLAPContext.getThreadLocalContexts();
        OLAPContext.clearThreadLocalContexts();
        return new EnhancedQueryResult(queryResult, ctxs);
    }

    private static QueryResult queryModelWithMassage(String prj, String sqlText) {
        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(prj), sqlText, prj, 0, 0, "DEFAULT", true);
        sqlText = QueryUtil.massageSql(queryParams);
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            long startTs = System.currentTimeMillis();
            QueryResult queryResult = queryModelWithMeta(prj, sqlText, null);
            log.info("Cool! This sql hits cube...");
            log.info("Query with Model Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return queryResult;
        } catch (Throwable e) {
            log.error("There is no cube can be used for query [{}]", sqlText);
            log.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    private static QueryResult queryModelWithMeta(String prj, String sql, List<Object> parameters) throws SQLException {
        SparderEnv.setDF(null); // clear last df
        // if this config is on
        // SQLS like "where 1<>1" will be optimized and run locally and no dataset will be returned
        String prevRunLocalConf = Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE");
        try {
            QueryExec queryExec = new QueryExec(prj,
                    NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(prj).getConfig(), true);
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

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ORDER, // exec and compare order
        SAME_ROWCOUNT, //
        SUBSET {
        }, //
        NONE {
        }, // batch execute
        SAME_WITH_DEVIATION_ALLOWED;

    }

    @Data
    @AllArgsConstructor
    public static class EnhancedQueryResult {

        @Delegate
        QueryResult queryResult;

        Collection<OLAPContext> olapContexts;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class SparkResult {

        String schema;

        List<String> resultData;
    }

    @Getter
    @Setter
    public static class CompareEntity {

        private String sql;
        @ToString.Exclude
        private Collection<OLAPContext> olapContexts;
        @ToString.Exclude
        private AccelerateInfo accelerateInfo;
        private String accelerateLayouts;
        private String queryUsedLayouts;
        private RecAndQueryCompareUtil.AccelerationMatchedLevel level;
        private String filePath;

        @Override
        public String toString() {
            return "CompareEntity{\n\tsql=[" + QueryUtil.removeCommentInSql(sql) + "],\n\taccelerateLayouts="
                    + accelerateLayouts + ",\n\tqueryUsedLayouts=" + queryUsedLayouts + "\n\tfilePath=" + filePath
                    + ",\n\tlevel=" + level + "\n}";
        }

        public boolean ignoredCompareLevel() {
            return getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.SNAPSHOT_QUERY
                    || getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.SIMPLE_QUERY
                    || getLevel() == RecAndQueryCompareUtil.AccelerationMatchedLevel.CONSTANT_QUERY;
        }
    }
}
