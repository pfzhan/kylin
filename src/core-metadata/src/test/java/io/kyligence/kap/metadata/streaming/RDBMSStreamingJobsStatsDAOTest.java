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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.streaming;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;

@RunWith(TimeZoneTestRunner.class)

public class RDBMSStreamingJobsStatsDAOTest extends NLocalFileMetadataTestCase {

    public static final String JOB_ID = "12345";
    public static final String PROJECT_NAME = "some_name";

    private RDBMSStreamingJobStatsDAO streamingJobsStatsDAO;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        streamingJobsStatsDAO = RDBMSStreamingJobStatsDAO.getInstance();
    }

    @After
    public void destroy() throws Exception {
        streamingJobsStatsDAO.deleteAllStreamingJobStats();
        cleanupTestMetadata();
        streamingJobsStatsDAO.dropTable();
    }

    @Test
    public void testInsert() {
        // Test insert one at a time
        streamingJobsStatsDAO.insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, 2000L));
        streamingJobsStatsDAO.insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, 4500L));
        streamingJobsStatsDAO.insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, 6500L));
        streamingJobsStatsDAO.insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, 7200L));
        streamingJobsStatsDAO.insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, 8400L));

        StreamingStatistics statistics1 = streamingJobsStatsDAO.getStreamingStatistics(5000L, JOB_ID);
        Assert.assertEquals(10.22, (double) statistics1.getMinRate(), 0.001);
        Assert.assertEquals(52.11, (double) statistics1.getMaxRate(), 0.001);
        Assert.assertEquals(340, statistics1.getCount());

        StreamingStatistics statistics2 = streamingJobsStatsDAO.getStreamingStatistics(4000L, JOB_ID);
        Assert.assertEquals(8.17, (double) statistics2.getMinRate(), 0.001);
        Assert.assertEquals(52.11, (double) statistics2.getMaxRate(), 0.001);
        Assert.assertEquals(460, statistics2.getCount());

        StreamingStatistics statistics3 = streamingJobsStatsDAO.getStreamingStatistics(9000L, JOB_ID);
        Assert.assertNull(statistics3);
    }

    @Test
    public void testInsertList() {
        // Test insert a list
        List<StreamingJobStats> statsList = new ArrayList<>();
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, 2000L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, 4500L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, 6500L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, 7200L));
        statsList.add(new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, 8400L));
        streamingJobsStatsDAO.insert(statsList);

        StreamingStatistics statistics1 = streamingJobsStatsDAO.getStreamingStatistics(5000L, JOB_ID);
        Assert.assertEquals(10.22, (double) statistics1.getMinRate(), 0.001);
        Assert.assertEquals(52.11, (double) statistics1.getMaxRate(), 0.001);
        Assert.assertEquals(340, statistics1.getCount());

        StreamingStatistics statistics2 = streamingJobsStatsDAO.getStreamingStatistics(4000L, JOB_ID);
        Assert.assertEquals(8.17, (double) statistics2.getMinRate(), 0.001);
        Assert.assertEquals(52.11, (double) statistics2.getMaxRate(), 0.001);
        Assert.assertEquals(460, statistics2.getCount());
    }

    @Test
    public void testGetSJSMetricMeasurement() {
        String metricMeasurement = streamingJobsStatsDAO.getSJSMetricMeasurement();
        Assert.assertEquals("test_streaming_job_stats", metricMeasurement);
    }

    @Test
    public void testGetQueryRowDetailByTime() {
        streamingJobsStatsDAO
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 32.22, 1200L, getCurrentTime() + 2000L));
        streamingJobsStatsDAO
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 120L, 8.17, 3200L, getCurrentTime() + 4500L));
        streamingJobsStatsDAO
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 130L, 10.22, 4200L, getCurrentTime() + 6500L));
        streamingJobsStatsDAO
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 100L, 25.56, 4700L, getCurrentTime() + 7200L));
        streamingJobsStatsDAO
                .insert(new StreamingJobStats(JOB_ID, PROJECT_NAME, 110L, 52.11, 3100L, getCurrentTime() + 8400L));
        List<RowCountDetailByTime> detail = streamingJobsStatsDAO.queryRowCountDetailByTime(getCurrentTime() + 5000L,
                JOB_ID);

        Assert.assertEquals(3, detail.size());
        Assert.assertEquals(100L, (long) detail.get(1).getBatchRowNum());
    }

    private long getCurrentTime() {
        return new Date(System.currentTimeMillis()).getTime();
    }

    @Test
    public void testDropTable() {
        try {
            streamingJobsStatsDAO.dropTable();
            streamingJobsStatsDAO.getStreamingStatistics(getCurrentTime(), JOB_ID);
        } catch(PersistenceException e) {
            Assert.assertEquals("Table \"TEST_STREAMING_JOB_STATS\" not found", e.getCause().getMessage().substring(0, 42));
        } catch(Exception e1) {
            return;
        } finally {
            try {
                setup();
            } catch(Exception e) {
                return;
            }
        }

    }
}
