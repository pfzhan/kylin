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

package io.kyligence.kap.clickhouse.job;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.HadoopUtil;

import java.net.URI;
import java.util.Locale;

@Data
public class BlobUrl {
    private String accountName;
    private String accountKey;
    private String container;
    private String hostSuffix;
    private int port;
    private String path;
    private String httpSchema;
    private String blobSchema;

    public static BlobUrl fromHttpUrl(String url) {
        URI uri = URI.create(url);
        BlobUrl blobUrl = new BlobUrl();
        blobUrl.setHttpSchema(uri.getScheme());
        blobUrl.setBlobSchema(http2blobSchema(uri.getScheme()));
        blobUrl.setAccountName(uri.getHost().split("\\.")[0]);
        blobUrl.setContainer(uri.getPath().split("/")[1]);
        blobUrl.setHostSuffix(uri.getHost().replace(blobUrl.getAccountName() + ".", ""));
        blobUrl.setPort(uri.getPort());
        blobUrl.setPath(uri.getPath().replace("/" + blobUrl.getContainer(), ""));
        blobUrl.setAccountKey(getAccountKeyByName(blobUrl.getAccountName(),
                blobUrl.getHostSuffix(), blobUrl.getPort()));
        return blobUrl;
    }

    public static BlobUrl fromBlobUrl(String url) {
        URI uri = URI.create(url);
        BlobUrl blobUrl = new BlobUrl();
        blobUrl.setHttpSchema(blob2httpSchema(uri.getScheme()));
        blobUrl.setBlobSchema(uri.getScheme());
        blobUrl.setAccountName(uri.getHost().split("\\.")[0]);
        blobUrl.setContainer(uri.getUserInfo());
        blobUrl.setHostSuffix(uri.getHost().replace(blobUrl.getAccountName() + ".", ""));
        blobUrl.setPort(uri.getPort());
        blobUrl.setPath(uri.getPath());
        blobUrl.setAccountKey(getAccountKeyByName(blobUrl.getAccountName(),
                blobUrl.getHostSuffix(), blobUrl.getPort()));
        return blobUrl;
    }

    private static String getAccountKeyByName(String accountName, String host, int port) {
        Configuration configuration = HadoopUtil.getCurrentConfiguration();
        String configKey;
        if (port == -1) {
            configKey = String.format(Locale.ROOT, "%s.%s", accountName, host);
        } else {
            configKey = String.format(Locale.ROOT, "%s.%s:%s", accountName, host, port);
        }
        return configuration.get(String.format(Locale.ROOT, "fs.azure.account.key.%s", configKey));
    }

    private static String http2blobSchema(String httpSchema) {
        String schema;
        switch (httpSchema) {
            case "http":
                schema = "wasb";
                break;
            case "https":
                schema = "wasbs";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(Locale.ROOT, "Unsupported schema %s", httpSchema));
        }
        return schema;
    }

    private static String blob2httpSchema(String blobSchema) {
        String schema;
        switch (blobSchema) {
            case "wasb":
                schema = "http";
                break;
            case "wasbs":
                schema = "https";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(Locale.ROOT, "Unsupported schema %s", blobSchema));
        }
        return schema;
    }

    public String getHttpEndpoint() {
        String defaultPort = this.getPort() == -1 ? "" : ":" + this.getPort();
        return String.format(Locale.ROOT, "%s://%s.%s%s", this.getHttpSchema(), this.getAccountName(),
                this.getHostSuffix(), defaultPort);
    }

    public String getConnectionString() {
        String formatHostSuffix = this.getHostSuffix().replace("blob.", "");
        return String.format(Locale.ROOT, "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;EndpointSuffix=%s",
                this.getHttpSchema(), this.getAccountName(), this.getAccountKey(), formatHostSuffix);
    }
}
