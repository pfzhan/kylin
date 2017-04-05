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

package io.kyligence.kap.job;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

/**
 * Created by luwei on 17-3-24.
 */
public class SampleCubeJoinedFlatTable extends JoinedFlatTable {

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc, JobEngineConfig engineConfig) {
        StringBuilder sql = new StringBuilder();

        sql.append(generateHiveSetStatements(engineConfig));
        sql.append("INSERT OVERWRITE TABLE " + flatDesc.getTableName() + " " + generateSelectDataStatement(flatDesc));
        appendAdvancedHiveStatement(flatDesc, sql);
        sql.append(";\n");

        return sql.toString();
    }

    private static void appendAdvancedHiveStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        final KylinConfig kylinConfig = ((CubeSegment) flatDesc.getSegment()).getConfig();
        final KapConfig kapConfig = KapConfig.wrap(kylinConfig);

        if (kapConfig.isAdvancedFlatTableByRowNum()) {
            int rowNum = kapConfig.getAdvancedFlatTableRowNum();
            sql.append("DISTRIBUTE BY RAND() SORT BY RAND() LIMIT " + rowNum).append(";\n");
        } else {
            int percentage = kapConfig.getAdvancedFlatTablePercentage();
            double percent = (double) percentage / 100;
            if (sql.toString().contains("WHERE")) {
                sql.delete(sql.lastIndexOf(")"), sql.length());
                sql.append(" AND ");
            } else
                sql.append("WHERE (");
            sql.append("RAND() < " + percent).append(");\n");
        }
    }
}