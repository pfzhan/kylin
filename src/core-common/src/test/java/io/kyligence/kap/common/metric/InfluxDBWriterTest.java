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

package io.kyligence.kap.common.metric;

import com.google.common.collect.Maps;
import io.kyligence.kap.shaded.influxdb.okhttp3.Interceptor;
import io.kyligence.kap.shaded.influxdb.okhttp3.MediaType;
import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.okhttp3.Protocol;
import io.kyligence.kap.shaded.influxdb.okhttp3.Request;
import io.kyligence.kap.shaded.influxdb.okhttp3.Response;
import io.kyligence.kap.shaded.influxdb.okhttp3.ResponseBody;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class InfluxDBWriterTest {

    private final String SHOW_DATABASES = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"KE_METRIC\"]]}]}]}\n";

    private InfluxDBWriter influxDBWriter;

    @Before
    public void setup() {
        InfluxDBWriter.influxDB = mockInfluxDB();
        influxDBWriter = InfluxDBWriter.getInstance();
    }

    @Test
    public void testBasic() {
        try {
            final Map<String, String> tags = Maps.newHashMap();
            tags.put("project", "default");
            final Map<String, Object> fields = Maps.newHashMap();
            fields.put("sql", "selct * from test_table");
            influxDBWriter.write("KE_METRIC", "query_metric", tags, fields, 0);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private InfluxDB mockInfluxDB() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                final Request request = chain.request();
                final URL url = request.url().url();
                if ("/ping".equals(url.getPath())) {
                    return mockPingSuccess(request);
                }

                if (url.toString().contains("SHOW+DATABASES")) {
                    return mockShowDatabases(request);
                }

                if ("/write".equals(url.getPath())) {
                    return mockWriteSuccess(request);
                }

                return chain.proceed(request);
            }
        });

        return InfluxDBFactory.connect("http://localhost:8086", "root", "root", client);
    }

    private Response mockPingSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }

    private Response mockShowDatabases(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), SHOW_DATABASES)).build();
    }

    private Response mockWriteSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }
}
