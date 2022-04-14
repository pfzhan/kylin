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

package org.apache.kylin.rest.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

public class KylinEhCache implements KylinCache {
    private static final Logger logger = LoggerFactory.getLogger(KylinEhCache.class);

    private CacheManager cacheManager;

    public static KylinCache getInstance() {
        return Singletons.getInstance(KylinEhCache.class);
    }

    private KylinEhCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String cacheConfigLocation = kylinConfig.getEhCacheConfigPath();
        try {
            logger.info("Trying to load ehcache properties from {}.", cacheConfigLocation);
            this.cacheManager = CacheManager.create(new URL(cacheConfigLocation));
        } catch (MalformedURLException e) {
            logger.warn("Cannot use " + cacheConfigLocation, e);
        } catch (CacheException e) {
            logger.warn("Create cache manager failed with config path: {}.", cacheConfigLocation, e);
        } finally {
            logger.info("Use default ehcache.xml.");
            this.cacheManager = CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache.xml"));
        }
    }

    public Ehcache getEhCache(String type, String project) {
        final String projectCacheName = String.format(Locale.ROOT, "%s-%s", type, project);
        if (cacheManager.getEhcache(projectCacheName) == null) {
            CacheConfiguration cacheConfiguration = cacheManager.getEhcache(type).getCacheConfiguration()
                    .clone();
            cacheConfiguration.setName(projectCacheName);
            cacheManager.addCacheIfAbsent(new Cache(cacheConfiguration));
        }

        return cacheManager.getEhcache(projectCacheName);
    }

    @Override
    public void put(String type, String project, Object key, Object value) {
        Ehcache ehcache = getEhCache(type, project);
        ehcache.put(new Element(key, value));
    }

    @Override
    public void update(String type, String project, Object key, Object value) {
        put(type, project, key, value);
    }

    @Override
    public Object get(String type, String project, Object key) {
        Ehcache ehcache = getEhCache(type, project);
        Element element = ehcache.get(key);
        return element == null ? null : element.getObjectValue();
    }

    @Override
    public boolean remove(String type, String project, Object key) {
        Ehcache ehcache = getEhCache(type, project);
        return ehcache.remove(key);
    }

    @Override
    public void clearAll() {
        cacheManager.clearAll();
    }

    @Override
    public void clearByType(String type, String project) {
        final String projectCacheName = String.format(Locale.ROOT, "%s-%s", type, project);
        String[] cacheNames = cacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Ehcache ehcache = cacheManager.getEhcache(cacheName);
            if (ehcache != null && cacheName.contains(projectCacheName)) {
                ehcache.removeAll();
            }
        }
    }
}
