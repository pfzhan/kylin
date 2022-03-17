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
package io.kyligence.kap.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.spark.sql.common.SparderQueryTest;
import org.junit.Assert;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.data.QueryResult;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryResultComparator {

    public static void compareColumnType(List<StructField> modelSchema, List<StructField> sparkSchema) {
        val cubeSize = modelSchema.size();
        val sparkSize = sparkSchema.size();
        Assert.assertEquals(cubeSize + " did not equal " + sparkSize, sparkSize, cubeSize);
        for (int i = 0; i < cubeSize; i++) {
            val cubeStructField = modelSchema.get(i);
            val sparkStructField = sparkSchema.get(i);
            Assert.assertTrue(
                    cubeStructField.getDataTypeName() + " did not equal " + sparkSchema.get(i).getDataTypeName(),
                    SparderQueryTest.isSameDataType(cubeStructField, sparkStructField));
        }
    }

    public static boolean compareResults(QueryResult expectedResult, QueryResult actualResult,
            ExecAndComp.CompareLevel compareLevel) {
        boolean good = true;

        val expectedRows = normalizeResult(expectedResult.getRows(), actualResult.getColumns());
        val actualRows = normalizeResult(actualResult.getRows(), actualResult.getColumns());

        switch (compareLevel) {
        case SAME_ORDER:
            good = expectedRows.equals(actualRows);
            break;
        case SAME:
            good = compareResultInLevelSame(expectedRows, actualRows);
            break;
        case SAME_ROWCOUNT:
            good = expectedRows.size() == actualRows.size();
            break;
        case SUBSET: {
            good = compareResultInLevelSubset(expectedRows, actualRows);
            break;
        }
        default:
            break;
        }

        if (!good) {
            log.error("Result not match");
            printRows("expected", expectedRows);
            printRows("actual", actualRows);
        }
        return good;
    }

    private static List<String> normalizeResult(List<List<String>> rows, List<StructField> columns) {
        List<String> resultRows = Lists.newArrayList();
        for (List<String> row : rows) {
            StringBuilder normalizedRow = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                StructField column = columns.get(i);
                if (row.get(i) == null || row.get(i).equals("null")) {
                    normalizedRow.append("");
                } else if (row.get(i).equals("NaN") || row.get(i).contains("Infinity")) {
                    normalizedRow.append(row.get(i));
                } else if (column.getDataTypeName().equals("DOUBLE")
                        || column.getDataTypeName().startsWith("DECIMAL")) {
                    normalizedRow.append(new BigDecimal(row.get(i)).setScale(2, RoundingMode.HALF_UP));
                } else if (column.getDataTypeName().equals("DATE")) {
                    val mills = DateFormat.stringToMillis(row.get(i));
                    normalizedRow.append(DateFormat.formatToDateStr(mills));
                } else if (column.getDataTypeName().equals("TIMESTAMP")) {
                    val millis = DateFormat.stringToMillis(row.get(i));
                    normalizedRow.append(DateFormat.castTimestampToString(millis));
                } else if (column.getDataTypeName().equals("ANY")) {
                    // bround udf return ANY
                    try {
                        normalizedRow.append(new BigDecimal(row.get(i)).setScale(2, RoundingMode.HALF_UP).toString());
                    } catch (Exception e) {
                        log.warn("try to cast to decimal failed", e);
                        normalizedRow.append(row.get(i));
                    }
                } else {
                    normalizedRow.append(row.get(i));
                }
                normalizedRow.append(" | ");
            }
            resultRows.add(normalizedRow.toString());
        }
        return resultRows;
    }

    private static boolean compareResultInLevelSubset(List<String> expectedResult, List<String> actualResult) {
        return CollectionUtils.isSubCollection(actualResult, expectedResult);
    }

    private static boolean compareResultInLevelSame(List<String> expectedResult, List<String> actualResult) {
        return CollectionUtils.isEqualCollection(expectedResult, actualResult);
    }

    private static void printRows(String source, List<String> rows) {
        log.info("***********" + source + " start, only show top 100 result**********");
        rows.stream().limit(100).forEach(log::info);
        log.info("***********" + source + " end**********");
    }
}