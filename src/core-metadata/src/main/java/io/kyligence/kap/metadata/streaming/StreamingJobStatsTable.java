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

package io.kyligence.kap.metadata.streaming;

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

public class StreamingJobStatsTable extends SqlTable {

    public final SqlColumn<Long> id = column("id", JDBCType.BIGINT);
    public final SqlColumn<String> jobId = column("job_id", JDBCType.VARCHAR);
    public final SqlColumn<String> projectName = column("project_name", JDBCType.VARCHAR);
    public final SqlColumn<Long> batchRowNum = column("batch_row_num", JDBCType.BIGINT);
    public final SqlColumn<Double> rowsPerSecond = column("rows_per_second", JDBCType.DOUBLE);
    public final SqlColumn<Long> processingTime = column("processing_time", JDBCType.BIGINT);
    public final SqlColumn<Long> minDataLatency = column("min_data_latency", JDBCType.BIGINT);
    public final SqlColumn<Long> maxDataLatency = column("max_data_latency", JDBCType.BIGINT);
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);

    public StreamingJobStatsTable(String tableName) {
        super(tableName);
    }

}
