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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.cachesync;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.util.BrokenEntityProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;

public abstract class CachedCrudAssist<T extends RootPersistentEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CachedCrudAssist.class);

    private final ResourceStore store;
    private final Class<T> entityType;
    private final String resRootPath;
    private final String resPathSuffix;
    private final Serializer<T> serializer;
    @Getter(AccessLevel.PROTECTED)
    private final Cache<String, T> cache;

    private boolean checkCopyOnWrite;

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, Class<T> entityType) {
        this(store, resourceRootPath, MetadataConstants.FILE_SURFIX, entityType);
    }

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, String resourcePathSuffix,
            Class<T> entityType) {
        this.store = store;
        this.entityType = entityType;
        this.resRootPath = resourceRootPath;
        this.resPathSuffix = resourcePathSuffix;
        this.serializer = new JsonSerializer<>(entityType);
        this.cache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

        this.checkCopyOnWrite = store.getConfig().isCheckCopyOnWrite();

        Preconditions.checkArgument(resourceRootPath.equals("") || resRootPath.startsWith("/"));
        Preconditions.checkArgument(!resRootPath.endsWith("/"));
    }

    public Serializer<T> getSerializer() {
        return serializer;
    }

    public void setCheckCopyOnWrite(boolean check) {
        this.checkCopyOnWrite = check;
    }

    // Make copy of an entity such that update can apply on the copy.
    // Note cached and shared object MUST NOT be updated directly.
    public T copyForWrite(T entity) {
        return JsonUtil.copyForWrite(entity, serializer, this::initEntityAfterReload);
    }

    public T copyBySerialization(T entity) {
        return JsonUtil.copyBySerialization(entity, serializer, this::initEntityAfterReload);
    }

    private String resourcePath(String resourceName) {
        return resRootPath + "/" + resourceName + resPathSuffix;
    }

    private String resourceName(String resourcePath) {
        Preconditions.checkArgument(resourcePath.startsWith(resRootPath));
        Preconditions.checkArgument(resourcePath.endsWith(resPathSuffix));
        return resourcePath.substring(resRootPath.length() + 1, resourcePath.length() - resPathSuffix.length());
    }

    public void reloadAll() {
        logger.trace("Reloading " + entityType.getSimpleName() + " from " + store.getReadableResourcePath(resRootPath));

        cache.invalidateAll();

        List<String> paths = store.collectResourceRecursively(resRootPath, resPathSuffix);
        for (String path : paths) {
            reloadQuietlyAt(path);
        }

        logger.debug("Loaded " + cache.size() + " " + entityType.getSimpleName() + "(s) out of " + paths.size()
                + " resource from " + store.getReadableResourcePath(resRootPath));
    }

    private T reload(String resourceName) {
        return reloadAt(resourcePath(resourceName));
    }

    private T reloadQuietlyAt(String path) {
        try {
            return reloadAt(path);
        } catch (Exception ex) {
            logger.error("Error loading " + entityType.getSimpleName() + " at " + path, ex);
            return null;
        }
    }

    public T reloadAt(String path) {
        T entity = null;
        try {
            entity = store.getResource(path, serializer);
            if (entity == null) {
                throw new IllegalStateException(
                        "No " + entityType.getSimpleName() + " found at " + path + ", returning null");
            }

            // mark cached object
            entity.setCachedAndShared(true);
            entity = initEntityAfterReload(entity, resourceName(path));

            if (!path.equals(resourcePath(entity.resourceName())))
                throw new IllegalStateException("The entity " + entity + " read from " + path
                        + " will save to a different path " + resourcePath(entity.resourceName()));

        } catch (Exception e) {
            entity = initBrokenEntity(entity, resourceName(path));
            entity.setCachedAndShared(false);
            logger.warn("Error loading " + entityType.getSimpleName() + " at " + path + " entity", e);
        }
        cache.put(entity.resourceName(), entity);
        return entity;
    }

    public boolean exists(String resourceName) {
        return store.getResource(resourcePath(resourceName)) != null;
    }

    public T get(String resourceName) {
        val raw = store.getResource(resourcePath(resourceName));
        val entity = cache.getIfPresent(resourceName);
        if (raw == null) {
            return null;
        } else if (entity == null) {
            reloadAt(resourcePath(resourceName));
        } else if (raw.getMvcc() == entity.getMvcc()) {
            return entity;
            //        } else if (raw.getMvcc() < entity.getMvcc()) {
            //            throw new IllegalStateException("resource " + raw.getResPath() + " version is less than cache");
        } else {
            reloadAt(resourcePath(resourceName));
        }
        return cache.getIfPresent(resourceName);
    }

    abstract protected T initEntityAfterReload(T entity, String resourceName);

    protected T initBrokenEntity(T entity, String resourceName) {
        BrokenEntityProxy brokenEntityProxy = new BrokenEntityProxy();
        T brokenEntity = brokenEntityProxy.getProxy(entityType);
        brokenEntity.setBroken(true);
        brokenEntity.setUuid(resourceName);
        brokenEntity.setMvcc(-1L);
        return brokenEntity;
    }

    public T save(T entity) {
        Preconditions.checkArgument(entity != null);
        Preconditions.checkArgument(entity.getUuid() != null);
        Preconditions.checkArgument(entityType.isInstance(entity));

        String resName = entity.resourceName();
        Preconditions.checkArgument(resName != null && resName.length() > 0);

        if (checkCopyOnWrite) {
            if (entity.isCachedAndShared() || cache.getIfPresent(resName) == entity) {
                throw new IllegalStateException("Copy-on-write violation! The updating entity " + entity
                        + " is a shared object in " + entityType.getSimpleName() + " cache, which should not be.");
            }
        }

        String path = resourcePath(resName);
        logger.debug("Saving {} at {}", entityType.getSimpleName(), path);

        store.checkAndPutResource(path, entity, serializer);

        // keep the pass-in entity out of cache, the caller may use it for further update
        // return a reloaded new object
        return reload(resName);
    }

    public void delete(T entity) {
        delete(entity.resourceName());
    }

    public void delete(String resName) {
        Preconditions.checkArgument(resName != null);

        String path = resourcePath(resName);
        logger.debug("Deleting {} at {}", entityType.getSimpleName(), path);

        store.deleteResource(path);
        cache.invalidate(resName);
    }

    public List<T> listAll() {
        val all = Lists.<T> newArrayList();
        for (String path : store.collectResourceRecursively(resRootPath, resPathSuffix)) {
            T value = get(resourceName(path));
            all.add(value);
        }
        return all;
    }

    /**
     * some cache entries may be outdated, because deletion might not touch CachedCrudAssist
     */
    public List<T> listAllValidCache() {
        val all = Lists.<T> newArrayList();
        for (val e : cache.asMap().entrySet()) {
            if (exists(e.getKey()))
                all.add(e.getValue());
        }
        return all;
    }

    public boolean contains(String name) {
        return store.getResource(resourcePath(name)) != null;
    }
}
