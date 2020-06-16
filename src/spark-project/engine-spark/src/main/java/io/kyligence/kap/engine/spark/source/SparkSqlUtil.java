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
package io.kyligence.kap.engine.spark.source;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;

public class SparkSqlUtil {
    public static Dataset<Row> query(SparkSession ss, String sql) {
        return ss.sql(sql);
    }

    public static List<Row> queryForList(SparkSession ss, String sql) {
        return ss.sql(sql).collectAsList();
    }

    public static List<Row> queryAll(SparkSession ss, String table){
        String sql = String.format("select * from %s", table);
        return queryForList(ss, sql);
    }

    public static Set<String> getViewOrignalTables(String viewName) throws ParseException {
        val spark = SparderEnv.getSparkSession();
        String viewText = spark.sql("desc formatted " + viewName).where("col_name = 'View Text'").head().getString(1);
        val logicalPlan = spark.sessionState().sqlParser().parsePlan(viewText);
        Set<String> viewTables = Sets.newHashSet();
        scala.collection.JavaConverters.seqAsJavaListConverter(logicalPlan.collectLeaves()).asJava().stream()
                .forEach(l -> {
                    if (l instanceof UnresolvedRelation) {
                        viewTables.add(((UnresolvedRelation) l).tableName());
                    }
                });
        return viewTables;
    }
}
