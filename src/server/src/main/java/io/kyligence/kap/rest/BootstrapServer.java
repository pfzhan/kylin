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

package io.kyligence.kap.rest;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.kylin.common.KylinConfig;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClient;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;

import io.kyligence.kap.guava20.shaded.common.hash.Hashing;
import io.kyligence.kap.guava20.shaded.common.base.Charsets;
import io.kyligence.kap.common.util.EncryptUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.val;

@ImportResource(locations = { "applicationContext.xml", "kylinSecurity.xml" })
@SpringBootApplication
@EnableScheduling
@EnableAsync
@EnableCaching
@EnableDiscoveryClient
@LoadBalancerClient(name = "spring-boot-provider", configuration = io.kyligence.kap.rest.LoadBalanced.class)
@EnableSpringHttpSession
@MapperScan("io.kyligence.kap.job.mapper")
public class BootstrapServer implements ApplicationListener<ApplicationEvent> {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapServer.class);

    public static void main(String[] args) {
        SpringApplication.run(BootstrapServer.class, args);
    }

    @Bean
    public ServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addContextCustomizers(context -> context.setRequestCharacterEncoding("UTF-8"));
        if (KylinConfig.getInstanceFromEnv().isServerHttpsEnabled()) {
            tomcat.addAdditionalTomcatConnectors(createSslConnector());
        }
        return tomcat;
    }

    private Connector createSslConnector() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        Http11NioProtocol protocol = (Http11NioProtocol) connector.getProtocolHandler();

        connector.setScheme("https");
        connector.setSecure(true);
        connector.setPort(kylinConfig.getServerHttpsPort());
        protocol.setSSLEnabled(true);
        protocol.setKeystoreType(kylinConfig.getServerHttpsKeyType());
        protocol.setKeystoreFile(kylinConfig.getServerHttpsKeystore());
        protocol.setKeyAlias(kylinConfig.getServerHttpsKeyAlias());
        String serverHttpsKeyPassword = kylinConfig.getServerHttpsKeyPassword();
        if (EncryptUtil.isEncrypted(serverHttpsKeyPassword)) {
            serverHttpsKeyPassword = EncryptUtil.decryptPassInKylin(serverHttpsKeyPassword);
        }
        protocol.setKeystorePass(serverHttpsKeyPassword);
        return connector;
    }

    @Bean
    public CookieSerializer cookieSerializer() {
        val serializer = new DefaultCookieSerializer();
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        String cookieName = url.getIdentifier()
                + (url.getParameter("url") == null ? "" : "_" + url.getParameter("url"));
        cookieName = cookieName.replaceAll("\\W", "_");
        cookieName = Hashing.sha256().newHasher().putString(cookieName, Charsets.UTF_8).hash().toString();
        serializer.setCookieName(cookieName);
        return serializer;
    }

    @Bean
    public InstanceSerializer<ZookeeperInstance> zookeeperInstanceInstanceSerializer() {
        return new JsonInstanceSerializer<ZookeeperInstance>(ZookeeperInstance.class) {
            @Override
            public ServiceInstance<ZookeeperInstance> deserialize(byte[] bytes) throws Exception {
                try {
                    return super.deserialize(bytes);
                } catch (Exception e) {
                    logger.warn("Zookeeper instance deserialize failed", e);
                    return null;
                }
            }
        };
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
            logger.info("init backend end...");
        } else if (event instanceof ContextClosedEvent) {
            logger.info("Stop Kyligence node...");
            EpochManager.getInstance().releaseOwnedEpochs();
        }
    }
}
