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

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import lombok.extern.slf4j.Slf4j;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@Slf4j
public class StreamingJobRecordManager {

    private final JdbcStreamingJobRecordStore jdbcRawRecStore;

    // CONSTRUCTOR
    public static StreamingJobRecordManager getInstance() {
        return Singletons.getInstance(StreamingJobRecordManager.class);
    }

    private StreamingJobRecordManager() throws Exception {
        this.jdbcRawRecStore = new JdbcStreamingJobRecordStore(KylinConfig.getInstanceFromEnv());
    }

    public int insert(StreamingJobRecord record) {
        return jdbcRawRecStore.insert(record);
    }

    public void dropTable() throws SQLException {
        jdbcRawRecStore.dropTable();
    }

    public void deleteStreamingJobRecord() {
        jdbcRawRecStore.deleteStreamingJobRecord(-1L);
    }

    public void deleteStreamingJobRecord(long startTime) {
        jdbcRawRecStore.deleteStreamingJobRecord(startTime);
    }

    public void deleteIfRetainTimeReached() {
        val retainTime = new Date(System.currentTimeMillis()
                - KylinConfig.getInstanceFromEnv().getStreamingJobStatsSurvivalThreshold() * 24 * 60 * 60 * 1000)
                        .getTime();
        jdbcRawRecStore.deleteStreamingJobRecord(retainTime);
    }

    public List<StreamingJobRecord> queryByJobId(String jobId) {
        return jdbcRawRecStore.queryByJobId(jobId);
    }

    public StreamingJobRecord getLatestOneByJobId(String jobId) {
        return jdbcRawRecStore.getLatestOneByJobId(jobId);
    }
}
