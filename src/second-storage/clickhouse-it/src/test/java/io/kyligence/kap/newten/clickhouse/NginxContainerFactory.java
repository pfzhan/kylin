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


import com.github.dockerjava.api.model.Container;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.junit.Assert;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.dockerclient.DockerClientConfigUtils;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.NetworkAliases;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.TEST_NETWORK;

@Slf4j
public enum NginxContainerFactory {

    INSTANCE;

    private static final int FIRST_LINE_INDEX = 0;
    private static final int CONTAINER_FULL_ID_INDEX = 4;

    public static final DockerImageName NGINX_IMAGE = DockerImageName.parse("nginx:1.21.1");
    @SuppressWarnings({"OperatorWrap"})
    private static final String NGINX_DEFAULT_CONF_TEMPLATE =
            "server {\n" +
                    "    listen       80;\n" +
                    "    listen  [::]:80;\n" +
                    "    server_name  localhost;\n" +
                    "\n" +
                    "    #access_log  /var/log/nginx/host.access.log  main;\n" +
                    "\n" +
                    "    location / {\n" +
                    "        root   ${WORKDIR};\n" +
                    "        index  index.html index.htm;\n" +
                    "    }\n" +
                    "\n" +
                    "    #error_page  404              /404.html;\n" +
                    "\n" +
                    "    # redirect server error pages to the static page /50x.html\n" +
                    "    #\n" +
                    "    error_page   500 502 503 504  /50x.html;\n" +
                    "    location = /50x.html {\n" +
                    "        root   ${WORKDIR};\n" +
                    "    }\n" +
                    "\n" +
                    "    # proxy the PHP scripts to Apache listening on 127.0.0.1:80\n" +
                    "    #\n" +
                    "    #location ~ \\.php$ {\n" +
                    "    #    proxy_pass   http://127.0.0.1;\n" +
                    "    #}\n" +
                    "\n" +
                    "    # pass the PHP scripts to FastCGI server listening on 127.0.0.1:9000\n" +
                    "    #\n" +
                    "    #location ~ \\.php$ {\n" +
                    "    #    root           html;\n" +
                    "    #    fastcgi_pass   127.0.0.1:9000;\n" +
                    "    #    fastcgi_index  index.php;\n" +
                    "    #    fastcgi_param  SCRIPT_FILENAME  /scripts$fastcgi_script_name;\n" +
                    "    #    include        fastcgi_params;\n" +
                    "    #}\n" +
                    "\n" +
                    "    # deny access to .htaccess files, if Apache's document root\n" +
                    "    # concurs with nginx's one\n" +
                    "    #\n" +
                    "    #location ~ /\\.ht {\n" +
                    "    #    deny  all;\n" +
                    "    #}\n" +
                    "}";

    private NginxContainerFactory() {
    }

    @SneakyThrows
    public NginxContainer startNginxContainer(String workingDir) {
        final NginxContainer<?> nginx = new NginxContainer<>(NGINX_IMAGE)
                .withNetwork(TEST_NETWORK)
                .withNetworkAliases(NetworkAliases)
                .waitingFor(new HttpWaitStrategy());

        if (DockerClientConfigUtils.IN_A_CONTAINER) {
            final String currentContainerId = obtainContainerId();
            log.info("current container id: {}", currentContainerId);
            final Container currentContainer = DockerClientFactory.instance().client().listContainersCmd().exec()
                    .stream()
                    .filter(container -> container.getId().substring(0, 12).equals(currentContainerId))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Can not found container with container id[" + currentContainerId + "]"));

            nginx.withVolumesFrom(new VolumesContainer(currentContainer), BindMode.READ_ONLY);
            nginx.start();

            // rendering nginx default.conf
            final String nginxConf = new StringSubstitutor(Collections.singletonMap("WORKDIR", workingDir)).replace(NGINX_DEFAULT_CONF_TEMPLATE);
            final File nginxConfFile = new File(workingDir, "default.conf");
            @Cleanup final PrintStream printStream = new PrintStream(nginxConfFile);
            printStream.print(nginxConf);

            nginx.copyFileToContainer(MountableFile.forHostPath(nginxConfFile.toPath()), "/etc/nginx/conf.d/default.conf");
            final org.testcontainers.containers.Container.ExecResult reloadResult = nginx.execInContainer("nginx", "-s", "reload");
            Assert.assertEquals(0, reloadResult.getExitCode());

            new HttpWaitStrategy().forStatusCode(200).waitUntilReady(nginx);

        } else {
            nginx.withCustomContent(workingDir);
            nginx.start();
        }

        Assert.assertTrue(nginx.isRunning());
        return nginx;
    }

    @SneakyThrows
    private String obtainContainerId() {
        // in k8s, the container id is obtained from '/proc/self/cgroup' file
        final File cgroup = new File("/proc/self/cgroup");
        Assert.assertTrue(cgroup.exists());

        log.info("/proc/self/cgroup: \n{}", FileUtils.readFileToString(cgroup));

        return FileUtils.readLines(cgroup).get(FIRST_LINE_INDEX).split("/")[CONTAINER_FULL_ID_INDEX].substring(0, 12);
    }

}