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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import io.kyligence.kap.shaded.influxdb.okhttp3.Interceptor;
import io.kyligence.kap.shaded.influxdb.okhttp3.MediaType;
import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.okhttp3.Protocol;
import io.kyligence.kap.shaded.influxdb.okhttp3.Request;
import io.kyligence.kap.shaded.influxdb.okhttp3.Response;
import io.kyligence.kap.shaded.influxdb.okhttp3.ResponseBody;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import org.apache.kylin.common.KapConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

public class KapQueryServiceTest extends NLocalFileMetadataTestCase {

    private KapQueryService kapQueryService = new KapQueryService();

    private final String mockData = "{\"results\":[{\"series\":[{\"name\":\"query_metric\",\"tags\":{\"engine_type\":\"RDBMS\"},\"columns\":[\"time\",\"count\",\"mean\"],\"values\":[[\"1970-01-01T00:00:00Z\",7.0,1108.7142857142858]]}],\"error\":null}],\"error\":null}\n";

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Before
    public void setup() {
        ReflectionTestUtils.setField(kapQueryService, "influxDB", mockInfluxDB());
        final KapConfig kapConfig = (KapConfig) ReflectionTestUtils.getField(kapQueryService, "kapConfig");
        kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
    }

    @Test
    public void testGetQueryStatistics() {
        final QueryStatisticsResponse actual = kapQueryService.getQueryStatistics(0L, Long.MAX_VALUE);

        Assert.assertEquals(7, actual.getAmount());

        final QueryStatisticsResponse.QueryStatistics hive = actual.getHive();
        Assert.assertEquals("HIVE", hive.getEngineType());
        Assert.assertEquals(0, hive.getCount());
        Assert.assertEquals(0d, hive.getMeanDuration(), 0.1);
        Assert.assertEquals(0d, hive.getRatio(), 0.01);

        final QueryStatisticsResponse.QueryStatistics rdbms = actual.getRdbms();
        Assert.assertEquals("RDBMS", rdbms.getEngineType());
        Assert.assertEquals(7, rdbms.getCount());
        Assert.assertEquals(1108.71d, rdbms.getMeanDuration(), 0.1);
        Assert.assertEquals(1d, rdbms.getRatio(), 0.01);

        final QueryStatisticsResponse.QueryStatistics aggIndex = actual.getAggIndex();
        Assert.assertEquals("Agg Index", aggIndex.getEngineType());
        Assert.assertEquals(0, aggIndex.getCount());
        Assert.assertEquals(0d, aggIndex.getMeanDuration(), 0.1);
        Assert.assertEquals(0d, aggIndex.getRatio(), 0.01);

        final QueryStatisticsResponse.QueryStatistics tableIndex = actual.getTableIndex();
        Assert.assertEquals("Table Index", tableIndex.getEngineType());
        Assert.assertEquals(0, tableIndex.getCount());
        Assert.assertEquals(0d, tableIndex.getMeanDuration(), 0.1);
        Assert.assertEquals(0d, tableIndex.getRatio(), 0.01);

    }

    private InfluxDB mockInfluxDB() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                final Request request = chain.request();
                return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                        .addHeader("Content-Type", "application/json").message("ok")
                        .body(ResponseBody.create(MediaType.parse("application/json"), mockData)).build();
            }
        });

        return InfluxDBFactory.connect("http://localhost:8096", "username", "password", client);
    }
}
