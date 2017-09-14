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
package io.kyligence.kap.rest.client;


import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClient {
    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public static String doGet(String url, String proxyServer, int proxyPort) {
        DefaultHttpClient client = getHttpClient(proxyServer, proxyPort);
        HttpGet request = new HttpGet(url);
        try {
            HttpResponse response = client.execute(request);
            if (response != null) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    try {
                        return IOUtils.toString(response.getEntity().getContent());
                    } catch (Exception e) {
                        logger.error("http client do get, url=" + url, e);
                    }
                }
            }
            return null;
        } catch (Exception e) {
            logger.error("http get exception." + url, e);
            return null;
        } finally {
            request.releaseConnection();
        }
    }

    public static DefaultHttpClient getHttpClient(String proxyServer, int proxyPort) {
        DefaultHttpClient client = new DefaultHttpClient();
        if (proxyServer != null && proxyPort > 0) {
            org.apache.http.HttpHost proxy = new org.apache.http.HttpHost(proxyServer, proxyPort);
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }
        client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 30000);
        client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 60000);
        return client;
    }


}
