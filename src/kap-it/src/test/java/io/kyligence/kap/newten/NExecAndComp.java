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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.source.adhocquery.HivePushDownConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import io.kyligence.kap.spark.KapSparkSession;

public class NExecAndComp {
    private static final Logger logger = LoggerFactory.getLogger(NExecAndComp.class);

    private static final String CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv";

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ROWCOUNT, SUBSET, NONE // batch execute
    }

    public static void execLimitAndValidate(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                            String joinType) {
        execLimitAndValidateNew(queries, kapSparkSession, joinType);
    }

    public static void execLimitAndValidateOld(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                               String joinType) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {

            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);

            String sqlWithLimit;
            if (sql.toLowerCase().contains("limit ")) {
                sqlWithLimit = sql;
            } else {
                sqlWithLimit = sql + " limit 5";
                appendLimitQueries++;
            }

            Dataset<Row> kapResult = queryWithKap(kapSparkSession, joinType, sqlWithLimit);
            Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);

            compareResults(sparkResult, kapResult, CompareLevel.SUBSET);
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                               String joinType) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {

            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);

            String sqlWithLimit;
            if (sql.toLowerCase().contains("limit ")) {
                sqlWithLimit = sql;
            } else {
                sqlWithLimit = sql + " limit 5";
                appendLimitQueries++;
            }

            Dataset<Row> kapResult = queryWithKap(kapSparkSession, joinType, sqlWithLimit);
            List<Row> kapRows = kapResult.toJavaRDD().collect();
            Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);
            List<Row> sparkRows = sparkResult.toJavaRDD().collect();
            compareResults(sparkRows, kapRows, CompareLevel.SUBSET);
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }


    public static void execAndCompare(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                      CompareLevel compareLevel, String joinType) {
//        execAndCompareOld(queries, kapSparkSession, compareLevel, joinType);
        execAndCompareNew(queries, kapSparkSession, compareLevel, joinType);
    }

    public static void execAndCompareNew(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                         CompareLevel compareLevel, String joinType) {
        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query (" + joinType + ") :" + query.getFirst());

            String sql = changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            Dataset<Row> cubeResult = queryWithKap(kapSparkSession, joinType, sql);
            List<Row> kapRows = cubeResult.toJavaRDD().collect();
            if (compareLevel != CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(kapSparkSession, sql);
                List<Row> sparkRows = sparkResult.toJavaRDD().collect();
                compareResults(sparkRows, kapRows, compareLevel);
            } else {
                cubeResult.persist();
                System.out
                        .println("result comparision is not available, part of the cube results: " + cubeResult.count());
                cubeResult.show();
                cubeResult.unpersist();
            }
        }
    }

    public static void execAndCompareOld(List<Pair<String, String>> queries, KapSparkSession kapSparkSession,
                                         CompareLevel compareLevel, String joinType) {

        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query (" + joinType + ") :" + query.getFirst());

            String sql = changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            Dataset<Row> cubeResult = queryWithKap(kapSparkSession, joinType, sql);

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

    private static Dataset<Row> queryWithKap(KapSparkSession kapSparkSession, String joinType, String sql) {
        return kapSparkSession.queryFromCube(changeJoinType(sql, joinType));
    }

    private static Dataset<Row> queryWithSpark(KapSparkSession kapSparkSession, String sql) {
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = sql.replaceAll("edw\\.", "").replaceAll("\"EDW\"\\.", "").replaceAll("EDW\\.", "")
                .replaceAll("default\\.", "").replaceAll("DEFAULT\\.", "").replaceAll("\"DEFAULT\"\\.", "")
                .replaceAll("TPCH\\.", "").replaceAll("tpch\\.", "");
        HivePushDownConverter converter = new HivePushDownConverter();
        String afterConvert = converter.convert(sqlForSpark, "default", "default", false);

        return kapSparkSession.querySparkSql(afterConvert);
    }

    public static String changeJoinType(String sql, String targetType) {

        if (targetType.equalsIgnoreCase("default"))
            return sql;

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left"))
                    && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase();
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));
        logger.info("The actual sql executed is: " + ret);

        return ret;
    }

    public static List<Pair<String, String>> fetchQueries(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls(sqlFolder);
    }

    @SuppressWarnings("SameParameterValue")
    static List<Pair<String, String>> fetchPartialQueries(String folder, int start, int end) throws IOException {
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
                sqlFiles = new File(baseDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.startsWith("sql_");
                    }
                });
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
        if (file != null) {
            if (file.exists() && file.listFiles() != null) {
                sqlFiles = file.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".sql");
                    }
                });
            }
        }
        List<Pair<String, String>> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex) : sqlStatement;
            ret.add(Pair.newPair(sqlFile.getCanonicalPath(), sql + '\n'));
        }
        return ret;
    }

    private static void compareResults(List<Row> expectedResult, List<Row> actualResult,
                                       CompareLevel compareLevel) {
        boolean good = true;
        if (compareLevel == CompareLevel.SAME) {
            if (expectedResult.size() == actualResult.size()) {
                for (Row eRow: expectedResult){
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
            for(Row eRow: expectedResult){
                if(!actualResult.contains(eRow)){
                    good = false;
                    break;
                }
            }
        }

        if (!good) {
            logger.error("Result not match");
            printRows("expected", expectedResult);
            printRows("actual", actualResult);
            throw new IllegalStateException("Result not match");
        }
    }

    private static void printRows(String source, List<Row> rows){
        System.out.println("***********" + source + " start**********");
        for (Row row: rows){
            System.out.println(row.mkString("|"));
        }
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

    static List<Pair<String, String>> doFilter(List<Pair<String, String>> sources, final String[] exclusionList) {
        Preconditions.checkArgument(sources != null);
        return Lists.newArrayList(Collections2.filter(sources, new Predicate<Pair<String, String>>() {
            @Override
            public boolean apply(Pair<String, String> input) {
                String fullPath = input.getFirst();
                for (String excludeFile : exclusionList) {
                    if (fullPath.endsWith(excludeFile))
                        return false;
                }
                return true;
            }
        }));
    }
}
