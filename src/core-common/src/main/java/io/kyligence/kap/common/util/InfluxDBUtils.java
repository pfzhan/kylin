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
package io.kyligence.kap.common.util;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.net.util.TrustManagerUtils;

import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;

public class InfluxDBUtils {

    private InfluxDBUtils() {
    }

    public static final String HTTP = "http://";

    public static final String HTTPS = "https://";

    public static InfluxDB getInfluxDBInstance(String addr, String username, String password, boolean enableSsl, boolean enableUnsafeSsl)
            throws Exception {
        String protocol = enableSsl ? HTTPS : HTTP;
        String url = protocol + addr;
        if (enableSsl && enableUnsafeSsl) {
            return InfluxDBFactory.connect(url, username, password, getBuilderWithAllTrustManager());
        }
        return InfluxDBFactory.connect(url, username, password);
    }

    private static OkHttpClient.Builder getBuilderWithAllTrustManager() throws Exception {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        X509TrustManager x509TrustManager = TrustManagerUtils.getAcceptAllTrustManager();
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] { x509TrustManager }, null);
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        builder.sslSocketFactory(sslSocketFactory, x509TrustManager);
        builder.hostnameVerifier((hostname, session) -> true);
        return builder;
    }

}
