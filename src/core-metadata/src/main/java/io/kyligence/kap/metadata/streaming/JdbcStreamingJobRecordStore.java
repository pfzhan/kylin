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

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.streaming.util.StreamingJobRecordStoreUtil;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcStreamingJobRecordStore {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String TOTAL_ROW_COUNT = "count";

    private final StreamingJobRecordTable table;

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String tableName;

    public JdbcStreamingJobRecordStore(KylinConfig config) throws Exception {
        StorageURL url = config.getStreamingStatsUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        tableName = StorageURL.replaceUrl(url) + "_" + StreamingJobRecord.STREAMING_JOB_RECORD_SUFFIX;
        table = new StreamingJobRecordTable(tableName);
        sqlSessionFactory = StreamingJobRecordStoreUtil.getSqlSessionFactory(dataSource, tableName);
    }

    public List<StreamingJobRecord> queryByJobId(String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(-1, jobId);
            return mapper.selectMany(statementProvider);
        }
    }

    public StreamingJobRecord getLatestOneByJobId(String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(1, jobId);
            return mapper.selectOne(statementProvider);
        }
    }

    public void dropTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(
                            String.format(Locale.ROOT, "drop table %s;", tableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    public int insert(StreamingJobRecord streamingJobRecord) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobRecordMapper mapper = session.getMapper(StreamingJobRecordMapper.class);
            InsertStatementProvider<StreamingJobRecord> insertStatement = getInsertProvider(streamingJobRecord);
            int rows = mapper.insert(insertStatement);
            if (rows > 0) {
                log.debug("Insert one streaming job record(job id:{}, time:{}) into database.",
                        streamingJobRecord.getJobId(), streamingJobRecord.getCreateTime());
            }
            session.commit();
            return rows;
        }
    }

    public void deleteStreamingJobRecord(long timeline) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            DeleteStatementProvider deleteStatement;
            if (timeline < 0) {
                deleteStatement = SqlBuilder.deleteFrom(table) //
                        .build().render(RenderingStrategies.MYBATIS3);
            } else {
                deleteStatement = SqlBuilder.deleteFrom(table) //
                        .where(table.createTime, isLessThan(timeline)) //
                        .build().render(RenderingStrategies.MYBATIS3);
            }
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            if (deleteRows > 0) {
                log.info("Delete {} row streaming job stats takes {} ms", deleteRows,
                        System.currentTimeMillis() - startTime);
            }
        }
    }

    SelectStatementProvider getSelectByJobIdStatementProvider(int limitSize, String jobId) {
        val builder = select(getSelectFields(table)) //
                .from(table) //
                .where(table.jobId, isEqualTo(jobId)); //
        if (limitSize > 0) {
            return builder.orderBy(table.createTime.descending()).limit(limitSize) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else {
            return builder.orderBy(table.createTime.descending()) //
                    .build().render(RenderingStrategies.MYBATIS3);
        }
    }

    InsertStatementProvider<StreamingJobRecord> getInsertProvider(StreamingJobRecord record) {
        return SqlBuilder.insert(record).into(table).map(table.id).toPropertyWhenPresent("id", record::getId) //
                .map(table.jobId).toPropertyWhenPresent("jobId", record::getJobId) //
                .map(table.project).toPropertyWhenPresent("project", record::getProject) //
                .map(table.action).toPropertyWhenPresent("action", record::getAction) //
                .map(table.createTime).toPropertyWhenPresent("createTime", record::getCreateTime) //
                .map(table.updateTime).toPropertyWhenPresent("updateTime", record::getUpdateTime) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(StreamingJobRecordTable recordTable) {
        return BasicColumn.columnList(//
                recordTable.id, //
                recordTable.jobId, //
                recordTable.project, //
                recordTable.action, //
                recordTable.createTime, //
                recordTable.updateTime);
    }

}
