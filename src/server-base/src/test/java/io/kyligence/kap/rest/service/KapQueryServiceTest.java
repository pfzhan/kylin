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

package io.kyligence.kap.rest.service;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryStatistics;
import io.kyligence.kap.rest.response.QueryEngineStatisticsResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

public class KapQueryServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private KapQueryService kapQueryService = Mockito.spy(new KapQueryService());

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    @Test
    public void testGetQueryStatistics() {
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        QueryStatistics queryStatistics = new QueryStatistics();
        queryStatistics.setEngineType("RDBMS");
        queryStatistics.setCount(7);
        queryStatistics.setMeanDuration(1108.7142857142858);
        Mockito.doReturn(Lists.newArrayList(queryStatistics)).when(queryHistoryDAO).getQueryEngineStatistics(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(queryHistoryDAO).when(kapQueryService).getQueryHistoryDao("default");

        final QueryEngineStatisticsResponse actual = kapQueryService.getQueryStatisticsByEngine("default", 0L, Long.MAX_VALUE);

        Assert.assertEquals(7, actual.getAmount());

        final QueryStatistics hive = actual.getHive();
        Assert.assertEquals("HIVE", hive.getEngineType());
        Assert.assertEquals(0, hive.getCount());
        Assert.assertEquals(0d, hive.getMeanDuration(), 0.1);
        Assert.assertEquals(0d, hive.getRatio(), 0.01);

        final QueryStatistics rdbms = actual.getRdbms();
        Assert.assertEquals("RDBMS", rdbms.getEngineType());
        Assert.assertEquals(7, rdbms.getCount());
        Assert.assertEquals(1108.71d, rdbms.getMeanDuration(), 0.1);
        Assert.assertEquals(1d, rdbms.getRatio(), 0.01);

        final QueryStatistics nativeQuery = actual.getNativeQuery();
        Assert.assertEquals("NATIVE", nativeQuery.getEngineType());
        Assert.assertEquals(0, nativeQuery.getCount());
        Assert.assertEquals(0d, nativeQuery.getMeanDuration(), 0.1);
        Assert.assertEquals(0d, nativeQuery.getRatio(), 0.01);
    }
}
