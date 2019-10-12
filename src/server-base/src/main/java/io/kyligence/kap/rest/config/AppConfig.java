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
package io.kyligence.kap.rest.config;

import java.net.MalformedURLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.cache.ehcache.EhCacheManagerFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableWebMvc
public class AppConfig extends WebMvcConfigurerAdapter {

    @Bean
    public TaskScheduler taskScheduler() {
        val scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadNamePrefix("DefaultTaskScheduler-");
        return scheduler;
    }

    @Bean
    public EhCacheManagerFactoryBean cacheFactoryBean(Environment environment) {
        val factory = new EhCacheManagerFactoryBean();
        factory.setShared(true);
        try {
            log.debug("Trying to use {}", cacheConfigLocation);
            factory.setConfigLocation(new UrlResource(cacheConfigLocation));
        } catch (MalformedURLException e) {
            log.warn("Cannot use " + cacheConfigLocation + ", use default ehcache.xml", e);
            factory.setConfigLocation(new ClassPathResource("ehcache.xml"));
        }
        return factory;
    }

    @Bean
    public EhCacheCacheManager cacheManager(Environment environment) {
        val manager = new EhCacheCacheManager();
        manager.setCacheManager(cacheFactoryBean(environment).getObject());
        return manager;
    }

    @Value("${server.port:7070}")
    @Getter
    private int port;

    @Value("${kylin.cache.config}")
    private String cacheConfigLocation;

    @Bean
    @ConditionalOnMissingBean(ClusterManager.class)
    public ClusterManager clusterManager() {
        return new DefaultClusterManager(port);
    }

    @Bean(name = "normalRestTemplate")
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.favorPathExtension(false);
    }

}
