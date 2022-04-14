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

import static java.lang.Math.toIntExact;

import java.net.MalformedURLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.cache.ehcache.EhCacheManagerFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.session.MapSessionRepository;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import io.kyligence.kap.common.util.DefaultHostInfoFetcher;
import io.kyligence.kap.common.util.HostInfoFetcher;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.handler.KapNoOpResponseErrorHandler;
import io.kyligence.kap.rest.interceptor.ReloadAuthoritiesInterceptor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class AppConfig implements WebMvcConfigurer {

    @Value("${kylin.thread.pool.core-pool-size:5}")
    private int threadPoolCorePoolSize;
    @Value("${kylin.thread.pool.max-pool-size:20}")
    private int threadPoolMaxPoolSize;
    @Value("${kylin.thread.pool.queue-capacity:200}")
    private int threadPoolQueueCapacity;
    @Value("${kylin.thread.pool.keep-alive-time:300s}")
    private String threadPoolKeepAliveTime;

    @Bean
    public TaskScheduler taskScheduler() {
        val scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadNamePrefix("DefaultTaskScheduler-");
        return scheduler;
    }

    @Bean
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(threadPoolCorePoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(threadPoolMaxPoolSize);
        threadPoolTaskExecutor.setQueueCapacity(threadPoolQueueCapacity);
        int threadPoolKeepAliveSeconds = toIntExact(TimeUtil.timeStringAs(
                StringUtils.isBlank(threadPoolKeepAliveTime) ? "300s" : threadPoolKeepAliveTime, TimeUnit.SECONDS));
        threadPoolTaskExecutor.setKeepAliveSeconds(threadPoolKeepAliveSeconds);
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.setThreadNamePrefix("DefaultThreadPoolTaskExecutor-");
        return threadPoolTaskExecutor;
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

    @Autowired(required = false)
    WebProperties webProperties;

    @Bean
    @ConditionalOnMissingBean(ClusterManager.class)
    public ClusterManager clusterManager() {
        return new DefaultClusterManager(port);
    }

    @Bean(name = "normalRestTemplate")
    public RestTemplate restTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new KapNoOpResponseErrorHandler());
        return restTemplate;
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        if (webProperties == null) {
            return;
        }
        registry.addResourceHandler("/index.html")
                .addResourceLocations(webProperties.getResources().getStaticLocations())
                .setCacheControl(CacheControl.noStore());
    }

    @Bean
    public ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        FilterProvider filterProvider = new SimpleFilterProvider().addFilter("passwordFilter",
                SimpleBeanPropertyFilter.serializeAllExcept("password"));

        return objectMapper.setFilterProvider(filterProvider);
    }

    @Bean
    @ConditionalOnMissingBean(HostInfoFetcher.class)
    public HostInfoFetcher hostInfoFetcher() {
        return new DefaultHostInfoFetcher();
    }

    @Bean
    public ReloadAuthoritiesInterceptor getReloadAuthoritiesInterceptor() {
        return new ReloadAuthoritiesInterceptor();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(getReloadAuthoritiesInterceptor());
    }

    @Bean
    @ConditionalOnProperty(prefix = "spring.session", name = "store-type", havingValue = "NONE")
    public MapSessionRepository sessionRepository() {
        return new MapSessionRepository(new ConcurrentHashMap<>());
    }

    @Bean
    public MultipartResolver multipartResolver() {
        CommonsMultipartResolver commonsMultipartResolver = new CommonsMultipartResolver();
        commonsMultipartResolver.setSupportedMethods(HttpMethod.POST.name(), HttpMethod.PUT.name());
        return commonsMultipartResolver;
    }
}
