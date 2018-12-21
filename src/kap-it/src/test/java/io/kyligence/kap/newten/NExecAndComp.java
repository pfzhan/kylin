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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.CompareQueryBySuffix;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.common.SparderQueryTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.spark.KapSparkSession;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.AccelerationMatchedLevel;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;

public class NExecAndComp {
    private static final Logger logger = LoggerFactory.getLogger(NExecAndComp.class);

    private static final String CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv";

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ROWCOUNT, SUBSET, NONE, // batch execute
        SAME_SQL_COMPARE
    }

    static void execLimitAndValidate(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            String joinType) {
        execLimitAndValidateNew(queries, kapSparkSession, joinType, null);
    }

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            String joinType, Map<String, CompareEntity> recAndQueryResult) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {
            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase().contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
                appendLimitQueries++;
            }

            Dataset<Row> kapResult = (recAndQueryResult == null)
                    ? queryWithKap(kapSparkSession, joinType, sqlAndAddedLimitSql)
                    : queryWithKap(kapSparkSession, joinType, sqlAndAddedLimitSql, recAndQueryResult);
            Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);
            List<Row> kapRows = SparderQueryTest.castDataType(kapResult, sparkResult).toJavaRDD().collect();
            List<Row> sparkRows = sparkResult.toJavaRDD().collect();
            if (!compareResults(sparkRows, kapRows, CompareLevel.SUBSET)) {
                throw new IllegalArgumentException("Result not match");
            }
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            CompareLevel compareLevel, String joinType) {
        execAndCompareNew(queries, kapSparkSession, compareLevel, joinType, null);
    }

    public static void execAndCompareNew(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            CompareLevel compareLevel, String joinType, Map<String, CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query (" + joinType + ") :" + query.getFirst());

            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            long startTime = System.currentTimeMillis();
            Dataset<Row> cubeResult = (recAndQueryResult == null)
                    ? queryWithKap(kapSparkSession, joinType, Pair.newPair(sql, sql))
                    : queryWithKap(kapSparkSession, joinType, Pair.newPair(sql, sql), recAndQueryResult);
            if (compareLevel != CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);
                List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).toJavaRDD().collect();
                List<Row> sparkRows = sparkResult.toJavaRDD().collect();
                if (!compareResults(sparkRows, kapRows, compareLevel)) {
                    logger.error("Failed on compare query (" + joinType + ") :" + query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else {
                cubeResult.persist();
                System.out.println(
                        "result comparision is not available, part of the cube results: " + cubeResult.count());
                cubeResult.show();
                cubeResult.unpersist();
            }
            logger.info("The query (" + joinType + ") : {} cost {} (ms)", query,
                    System.currentTimeMillis() - startTime);
        }
    }

    static void execCompareQueryAndCompare(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            String joinType) {
        for (Pair<String, String> query : queries) {

            logger.info("Exec CompareQuery and compare on query: " + query.getFirst());
            String sql1 = KylinTestBase.changeJoinType(query.getSecond(), joinType);
            String sql2 = CompareQueryBySuffix.INSTANCE.transform(new File(query.getFirst()));

            Dataset<Row> kapResult = queryWithKap(kapSparkSession, joinType, Pair.newPair(sql1, sql1));
            Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql2);

            compareResults(sparkResult, kapResult, CompareLevel.SAME);
        }
    }

    static void execAndCompareOld(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
            CompareLevel compareLevel, String joinType) {

        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query (" + joinType + ") :" + query.getFirst());

            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            Dataset<Row> cubeResult = queryWithKap(kapSparkSession, joinType, new Pair<>(sql, sql));

            if (compareLevel != CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);
                compareResults(sparkResult, cubeResult, compareLevel);
            } else {
                cubeResult.persist();
                System.out
                        .println("result comparision is not available, part of the cube results:" + cubeResult.count());
                cubeResult.show();
                cubeResult.unpersist();
            }
        }
    }

    private static Dataset<Row> queryWithKap(KapSparkSession kapSparkSession, String joinType,
            Pair<String, String> pair, Map<String, CompareEntity> compareEntityMap) {

        compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
        final CompareEntity entity = compareEntityMap.get(pair.getFirst());
        entity.setSql(pair.getFirst());
        Dataset<Row> rowDataset = null;
        try {
            rowDataset = kapSparkSession.queryFromCube(KylinTestBase.changeJoinType(pair.getSecond(), joinType));
            entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
            OLAPContext.clearThreadLocalContexts();
        } catch (Exception e) {
            entity.setLevel(AccelerationMatchedLevel.FAILED_QUERY);
        }
        return rowDataset;
    }

    private static Dataset<Row> queryWithKap(KapSparkSession kapSparkSession, String joinType,
            Pair<String, String> sql) {
        return kapSparkSession.queryFromCube(KylinTestBase.changeJoinType(sql.getSecond(), joinType));
    }

    private static Dataset<Row> queryWithSpark(KapSparkSession kapSparkSession, String sql) {
        String afterConvert = QueryUtil.massagePushDownSql(sql, kapSparkSession.project(), "default", false);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = afterConvert.replaceAll("edw\\.", "")
                .replaceAll("`edw`\\.", "")
                .replaceAll("\"EDW\"\\.", "")
                .replaceAll("EDW\\.", "")
                .replaceAll("`EDW`\\.", "")
                .replaceAll("default\\.", "")
                .replaceAll("`default`\\.", "")
                .replaceAll("DEFAULT\\.", "")
                .replaceAll("\"DEFAULT\"\\.", "")
                .replaceAll("`DEFAULT`\\.", "")
                .replaceAll("TPCH\\.", "")
                .replaceAll("`TPCH`\\.", "")
                .replaceAll("tpch\\.", "")
                .replaceAll("`tpch`\\.", "")
                .replaceAll("TDVT\\.", "")
                .replaceAll("\"TDVT\"\\.", "")
                .replaceAll("`TDVT`\\.", "")
                .replaceAll("\"POPHEALTH_ANALYTICS\"\\.", "")
                .replaceAll("`POPHEALTH_ANALYTICS`\\.", "");
        return kapSparkSession.querySparkSql(sqlForSpark);
    }

    public static List<Pair<String, String>> fetchQueries(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls(sqlFolder);
    }

    public static List<Pair<String, String>> fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFolder = new File(folder);
        List<Pair<String, String>> originalSqls = retrieveITSqls(sqlFolder);
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
        Arrays.sort(sqlFiles, (o1, o2) -> String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName()));
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex)
                    : sqlStatement;
            ret.add(Pair.newPair(sqlFile.getCanonicalPath(), sql + '\n'));
        }
        return ret;
    }

    private static boolean compareResults(List<Row> expectedResult, List<Row> actualResult, CompareLevel compareLevel) {
        boolean good = true;
        if (compareLevel == CompareLevel.SAME) {
            if (expectedResult.size() == actualResult.size()) {
                if (expectedResult.size() > 15000) {
                    throw new RuntimeException(
                            "please modify the sql to control the result size that less than 15000 and it has "
                                    + actualResult.size() + " rows");
                }
                for (Row eRow : expectedResult) {
                    if (!actualResult.contains(eRow)) {
                        good = false;
                        break;
                    }
                }
            } else {
                good = false;
            }
        }

        if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
            long count1 = expectedResult.size();
            long count2 = actualResult.size();
            good = count1 == count2;
        }

        if (compareLevel == CompareLevel.SUBSET) {
            for (Row eRow : actualResult) {
                if (!expectedResult.contains(eRow)) {
                    good = false;
                    break;
                }
            }
        }

        if (!good) {
            logger.error("Result not match");
            printRows("expected", expectedResult);
            printRows("actual", actualResult);
        }
        return good;
    }

    private static void printRows(String source, List<Row> rows) {
        System.out.println("***********" + source + " start**********");
        rows.forEach(row -> System.out.println(row.mkString(" | ")));
        System.out.println("***********" + source + " end**********");
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
                expectedResult.show(10000);
                actualResult.show(10000);
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
}
