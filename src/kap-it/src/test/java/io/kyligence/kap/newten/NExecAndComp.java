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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.common.SparderQueryTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;
import lombok.val;

public class NExecAndComp {
    private static final Logger logger = LoggerFactory.getLogger(NExecAndComp.class);
    private static final int COMPARE_DEVIATION = 5;

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ORDER, // exec and compare order
        SAME_ROWCOUNT, SUBSET, NONE, // batch execute
        SAME_SQL_COMPARE, SAME_WITH_DEVIATION_ALLOWED
    }

    static void execLimitAndValidate(List<Pair<String, String>> queries, String prj, String joinType) {
        execLimitAndValidateNew(queries, prj, joinType, null);
    }

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, String prj, String joinType,
            Map<String, CompareEntity> recAndQueryResult) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {
            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
                appendLimitQueries++;
            }

            Dataset<Row> kapResult = (recAndQueryResult == null) ? queryWithKap(prj, joinType, sqlAndAddedLimitSql)
                    : queryWithKap(prj, joinType, sqlAndAddedLimitSql, recAndQueryResult);
            addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
            List<Row> kapRows = SparderQueryTest.castDataType(kapResult, sparkResult).collectAsList();
            List<Row> sparkRows = sparkResult.collectAsList();
            if (!compareResults(normRows(sparkRows), normRows(kapRows), CompareLevel.SUBSET)) {
                throw new IllegalArgumentException("Result not match");
            }
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        execAndCompareNew(queries, prj, compareLevel, joinType, null);
    }

    public static void execAndCompareDynamic(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType, Map<String, CompareEntity> recAndQueryResult) {

        for (Pair<String, String> path2Sql : queries) {
            try {
                logger.info("Exec and compare query ({}) :{}", joinType, path2Sql.getFirst());
                String sql = KylinTestBase.changeJoinType(path2Sql.getSecond(), joinType);
                long startTime = System.currentTimeMillis();
                List<String> params = KylinTestBase.getParameterFromFile(new File(path2Sql.getFirst().trim()));

                recAndQueryResult.putIfAbsent(path2Sql.getSecond(), new CompareEntity());
                final CompareEntity entity = recAndQueryResult.get(path2Sql.getSecond());
                entity.setSql(path2Sql.getSecond());
                Dataset<Row> cubeResult = queryFromCube(prj, sql, params);

                Dataset<Row> sparkResult = queryWithSpark(prj, path2Sql.getSecond(), path2Sql.getFirst());
                List<Row> sparkRows = sparkResult.collectAsList();
                List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();
                if (!compareResults(normRows(sparkRows), normRows(kapRows), compareLevel)) {
                    logger.error("Failed on compare query ({}) :{}", joinType, sql);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + sql + " result not match");
                }

                entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
                OLAPContext.clearThreadLocalContexts();
                addQueryPath(recAndQueryResult, path2Sql, sql);
                logger.info("The query ({}) : {} cost {} (ms)", joinType, sql, System.currentTimeMillis() - startTime);

            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "meet ERROR when running query (" + joinType + ") :\n" + path2Sql.getSecond(), e);
            }
        }
    }

    public static void execAndCompareQueryList(List<String> queries, String prj, CompareLevel compareLevel,
            String joinType) {
        List<Pair<String, String>> transformed = queries.stream().map(q -> Pair.newPair("", q))
                .collect(Collectors.toList());
        execAndCompareNew(transformed, prj, compareLevel, joinType, null);
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
            logger.info("\"{}\" is in TODO List, skipmetadata check.", fullPath);
            return true;
        }
        return false;
    }

    public static void execAndCompareNew(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
            String joinType, Map<String, CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query ({}) :{}", joinType, query.getFirst());

            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            long startTime = System.currentTimeMillis();
            QueryResult cubeQueryResult = queryWithKapWithMeta(prj, joinType, Pair.newPair(sql, sql),
                    recAndQueryResult);
            List<StructField> cubeColumns = cubeQueryResult.getColumns();
            Dataset<Row> cubeResult = SparderEnv.getDF();
            addQueryPath(recAndQueryResult, query, sql);
            if (compareLevel != CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
                if ((compareLevel == CompareLevel.SAME || compareLevel == CompareLevel.SAME_ORDER)
                        && sparkResult.schema().fields().length != cubeResult.schema().fields().length) {
                    logger.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType,
                            query, cubeResult.schema().fieldNames(), sparkResult.schema().fieldNames());
                    throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
                }
                if (!inToDoList(query.getFirst()) && compareLevel == CompareLevel.SAME) {
                    SparderQueryTest.compareColumnTypeWithCalcite(cubeColumns, sparkResult.schema());
                }
                List<Row> sparkRows = sparkResult.collectAsList();
                logger.error("OCC on compare query ({}) :{}", joinType, query);
                List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();
                if (!compareResults(normRows(sparkRows), normRows(kapRows), compareLevel)) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else {
                cubeResult.persist();
                logger.debug("result comparision is not available, part of the cube results: " + cubeResult.count());
                logger.debug(cubeResult.showString(10, 20, false));
                cubeResult.unpersist();
            }
            logger.info("The query ({}) : {} cost {} (ms)", joinType, query, System.currentTimeMillis() - startTime);
        }
    }

    public static void execAndFail(List<Pair<String, String>> queries, String prj, String joinType,
            Map<String, CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query ({}) :{}", joinType, query.getFirst());

            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            try {
                if (recAndQueryResult == null) {
                    queryWithKap(prj, joinType, Pair.newPair(sql, sql));
                } else {
                    queryWithKap(prj, joinType, Pair.newPair(sql, sql), recAndQueryResult);
                }

            } catch (RuntimeException e) {
                if ((e.getCause().getCause() instanceof NoRealizationFoundException)) {
                    logger.info("The query ({}) : {} fail", joinType, query);
                    continue;
                }
            }
            throw new IllegalStateException("query " + query + " not fail");
        }
    }

    public static boolean execAndCompareQueryResult(Pair<String, String> queryForKap,
            Pair<String, String> queryForSpark, String joinType, String prj,
            Map<String, CompareEntity> recAndQueryResult) {
        String sqlForSpark = KylinTestBase.changeJoinType(queryForSpark.getSecond(), joinType);
        addQueryPath(recAndQueryResult, queryForSpark, sqlForSpark);
        Dataset<Row> sparkResult = queryWithSpark(prj, queryForSpark.getSecond(), queryForSpark.getFirst());
        List<Row> sparkRows = sparkResult.collectAsList();

        String sqlForKap = KylinTestBase.changeJoinType(queryForKap.getSecond(), joinType);
        Dataset<Row> cubeResult = queryWithKap(prj, joinType, Pair.newPair(sqlForKap, sqlForKap));
        List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();

        return sparkRows.equals(kapRows);
    }

    public static List<Row> normRows(List<Row> rows) {
        List<Row> rowList = Lists.newArrayList();
        rows.forEach(row -> {
            rowList.add(SparderQueryTest.prepareRow(row));
        });
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

    @Deprecated
    static void execCompareQueryAndCompare(List<Pair<String, String>> queries, String prj, String joinType) {
        throw new IllegalStateException(
                "The method has deprecated, please call io.kyligence.kap.newten.NExecAndComp.execAndCompareNew");
    }

    public static Dataset<Row> queryWithKap(String prj, String joinType, Pair<String, String> pair,
            Map<String, CompareEntity> compareEntityMap) {

        compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
        final CompareEntity entity = compareEntityMap.get(pair.getFirst());
        entity.setSql(pair.getFirst());
        Dataset<Row> rowDataset = queryFromCube(prj, KylinTestBase.changeJoinType(pair.getSecond(), joinType));
        entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
        OLAPContext.clearThreadLocalContexts();
        return rowDataset;
    }

    public static Dataset<Row> queryWithKap(String prj, String joinType, Pair<String, String> sql) {
        return queryFromCube(prj, KylinTestBase.changeJoinType(sql.getSecond(), joinType));
    }

    public static QueryResult queryWithKapWithMeta(String prj, String joinType, Pair<String, String> pair,
            Map<String, CompareEntity> compareEntityMap) {
        if (compareEntityMap == null)
            return queryFromCubeWithMeta(prj, KylinTestBase.changeJoinType(pair.getSecond(), joinType));
        compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
        final CompareEntity entity = compareEntityMap.get(pair.getFirst());
        entity.setSql(pair.getFirst());
        QueryResult queryResult = queryFromCubeWithMeta(prj, KylinTestBase.changeJoinType(pair.getSecond(), joinType));
        entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
        OLAPContext.clearThreadLocalContexts();
        return queryResult;
    }

    public static Dataset<Row> queryWithSpark(String prj, String originSql, String sqlPath) {
        String compareSql = getCompareSql(sqlPath);
        if (StringUtils.isEmpty(compareSql))
            compareSql = originSql;

        QueryParams queryParams = new QueryParams(prj, compareSql, "default", false);
        queryParams.setKylinConfig(QueryUtil.getKylinConfig(prj));
        String afterConvert = QueryUtil.massagePushDownSql(queryParams);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = removeDataBaseInSql(afterConvert);
        return querySparkSql(sqlForSpark);
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
            logger.error("Result not match");
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
        logger.debug("***********" + source + " start**********");
        rows.forEach(row -> logger.info(row.mkString(" | ")));
        logger.debug("***********" + source + " end**********");
    }

    private static void compareResults(Dataset<Row> expectedResult, Dataset<Row> actualResult,
            CompareLevel compareLevel) {
        Preconditions.checkArgument(expectedResult != null);
        Preconditions.checkArgument(actualResult != null);

        try {
            expectedResult.persist();
            actualResult.persist();

            boolean good = true;

            if (compareLevel == CompareLevel.SAME) {
                long count1 = expectedResult.except(actualResult).count();
                long count2 = actualResult.except(expectedResult).count();
                if (count1 != 0 || count2 != 0) {
                    good = false;
                }
            }

            if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
                long count1 = expectedResult.count();
                long count2 = actualResult.count();
                good = count1 == count2;
            }

            if (compareLevel == CompareLevel.SUBSET) {
                long count1 = actualResult.except(expectedResult).count();
                good = count1 == 0;
            }

            if (!good) {
                logger.error("Result not match");
                logger.info(expectedResult.showString(10000, 20, false));
                logger.info(actualResult.showString(10000, 20, false));
                throw new IllegalStateException();
            }
        } finally {
            expectedResult.unpersist();
            actualResult.unpersist();
        }
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

    public static Dataset<Row> queryFromCube(String prj, String sqlText, List<String> parameters) {
        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(prj), sqlText, prj, 0, 0, "DEFAULT", true);
        sqlText = QueryUtil.massageSql(queryParams);
        return sql(prj, sqlText, parameters);
    }

    public static Dataset<Row> queryFromCube(String prj, String sqlText) {
        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(prj), sqlText, prj, 0, 0, "DEFAULT", true);
        sqlText = QueryUtil.massageSql(queryParams);
        return sql(prj, sqlText, null);
    }

    public static QueryResult queryFromCubeWithMeta(String prj, String sqlText) {
        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(prj), sqlText, prj, 0, 0, "DEFAULT", true);
        sqlText = QueryUtil.massageSql(queryParams);
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            logger.info("Try to query from cube....");
            long startTs = System.currentTimeMillis();
            QueryResult queryResult = queryCubeWithMeta(prj, sqlText);
            logger.info("Cool! This sql hits cube...");
            logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return queryResult;
        } catch (Throwable e) {
            logger.error("There is no cube can be used for query [{}]", sqlText);
            logger.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    public static Dataset<Row> querySparkSql(String sqlText) {
        logger.info("Fallback this sql to original engine...");
        long startTs = System.currentTimeMillis();
        Dataset<Row> r = SparderEnv.getSparkSession().sql(sqlText);
        logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
        return r;
    }

    public static Dataset<Row> sql(String prj, String sqlText) {
        return sql(prj, sqlText, null);
    }

    public static Dataset<Row> sql(String prj, String sqlText, List<String> parameters) {
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            logger.info("Try to query from cube....");
            long startTs = System.currentTimeMillis();
            Dataset<Row> dataset = queryCubeAndSkipCompute(prj, sqlText, parameters);
            logger.info("Cool! This sql hits cube...");
            logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return dataset;
        } catch (Throwable e) {
            logger.error("There is no cube can be used for query [{}]", sqlText);
            logger.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    static Dataset<Row> queryCubeAndSkipCompute(String prj, String sql, List<String> parameters) throws Exception {
        try {
            SparderEnv.skipCompute();
            List<Object> parametersNotNull = parameters == null ? new ArrayList<>() : new ArrayList<>(parameters);
            Dataset<Row> df = queryCube(prj, sql, parametersNotNull);
            return df;
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    static Dataset<Row> queryCubeAndSkipCompute(String prj, String sql) throws Exception {
        try {
            SparderEnv.skipCompute();
            Dataset<Row> df = queryCube(prj, sql, null);
            return df;
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    public static Dataset<Row> queryCube(String prj, String sql) throws SQLException {
        return queryCube(prj, sql, null);
    }

    public static Dataset<Row> queryCube(String prj, String sql, List<Object> parameters) throws SQLException {
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
            queryExec.executeQuery(sql);
        } finally {
            if (prevRunLocalConf != null) {
                Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf);
            } else {
                Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
            }
        }
        return SparderEnv.getDF();
    }

    public static QueryResult queryCubeWithMeta(String prj, String sql) throws SQLException {
        SparderEnv.setDF(null); // clear last df
        // if this config is on
        // SQLS like "where 1<>1" will be optimized and run locally and no dataset will be returned
        String prevRunLocalConf = Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE");
        try {
            QueryExec queryExec = new QueryExec(prj,
                    NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(prj).getConfig(), true);
            return queryExec.executeQuery(sql);
        } finally {
            if (prevRunLocalConf != null) {
                Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf);
            } else {
                Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
            }
        }
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
            logger.error("meet error when reading compared spark sql from {}", file.getAbsolutePath());
            return "";
        }
    }

    public static List<List<String>> queryCubeWithJDBC(String prj, String sql) throws Exception {
        //      SparderEnv.skipCompute();
        return new QueryExec(prj, KylinConfig.getInstanceFromEnv(), true).executeQuery(sql).getRows();
    }
}
