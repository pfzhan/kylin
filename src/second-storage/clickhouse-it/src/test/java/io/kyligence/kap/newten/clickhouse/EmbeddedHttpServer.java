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
package io.kyligence.kap.newten.clickhouse;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.testcontainers.containers.NginxContainer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.Locale;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.NetworkAliases;

@Slf4j
public class EmbeddedHttpServer {
    private final NginxContainer<?> nginx;

   public EmbeddedHttpServer(NginxContainer<?> nginx) {
        this.nginx = nginx;
    }

    @SneakyThrows
    public URL getBaseUrl() {
       return nginx.getBaseUrl("http", 80);
    }

    @SneakyThrows
    public String getDockerAccessURL() {
       return String.format(Locale.ROOT, "http://%s:%d", NetworkAliases, 80);
   }

    public void stopServer() {
        if (nginx != null) {
            nginx.stop();
        }
    }

    @SneakyThrows
    public static EmbeddedHttpServer startNginx(String workingDir) {
        File working = new File(workingDir);
        if (!working.exists()) {
            Assert.assertTrue(working.mkdirs());
        }
        File indexFile = new File(working, "index.html");
        @Cleanup PrintStream printStream = new PrintStream(new FileOutputStream(indexFile));
        printStream.println("<html><body>Hello World!</body></html>");

        NginxContainer<?> nginx = NginxContainerFactory.INSTANCE.startNginxContainer(workingDir);
        return new EmbeddedHttpServer(nginx);
    }
}
