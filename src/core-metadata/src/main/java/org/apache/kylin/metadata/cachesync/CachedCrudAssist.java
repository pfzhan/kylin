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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;

public abstract class CachedCrudAssist<T extends RootPersistentEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CachedCrudAssist.class);

    final private ResourceStore store;
    final private Class<T> entityType;
    final private String resRootPath;
    final private String resPathSuffix;
    final private Serializer<T> serializer;
    @Getter(AccessLevel.PROTECTED)
    final private Map<String, T> cache;

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
        this.cache = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

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
        if (!entity.isCachedAndShared())
            return entity;
        else
            return copyBySerialization(entity);
    }

    public T copyBySerialization(T entity) {
        Preconditions.checkNotNull(entity);
        T copy;
        try {
            byte[] bytes;
            try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(buf)) {
                serializer.serialize(entity, dout);
                bytes = buf.toByteArray();
            }

            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                copy = serializer.deserialize(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        copy.setCachedAndShared(false);
        initEntityAfterReload(copy, entity.resourceName());
        return copy;
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
        logger.debug("Reloading " + entityType.getSimpleName() + " from " + store.getReadableResourcePath(resRootPath));

        cache.clear();

        List<String> paths = store.collectResourceRecursively(resRootPath, resPathSuffix);
        for (String path : paths) {
            reloadQuietlyAt(path);
        }

        logger.debug("Loaded " + cache.size() + " " + entityType.getSimpleName() + "(s) out of " + paths.size()
                + " resource");
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
        try {
            T entity = store.getResource(path, serializer);
            if (entity == null) {
                logger.warn("No " + entityType.getSimpleName() + " found at " + path + ", returning null");
                cache.remove(resourceName(path));
                return null;
            }

            // mark cached object
            entity.setCachedAndShared(true);
            entity = initEntityAfterReload(entity, resourceName(path));

            if (!path.equals(resourcePath(entity.resourceName())))
                throw new IllegalStateException("The entity " + entity + " read from " + path
                        + " will save to a different path " + resourcePath(entity.resourceName()));

            cache.put(entity.resourceName(), entity);
            return entity;
        } catch (Exception e) {
            throw new IllegalStateException("Error loading " + entityType.getSimpleName() + " at " + path, e);
        }
    }

    public T get(String resourceName) {
        val raw = store.getResource(resourcePath(resourceName));
        val entity = cache.get(resourceName);
        if (entity == null) {
            reloadAt(resourcePath(resourceName));
        } else if (raw.getMvcc() == entity.getMvcc()) {
            return entity;
        } else if (raw.getMvcc() < entity.getMvcc()) {
            throw new IllegalStateException("resource " + raw.getResPath() + " version is less than cache");
        } else {
            reloadAt(resourcePath(resourceName));
        }
        return cache.get(resourceName);
    }

    abstract protected T initEntityAfterReload(T entity, String resourceName);

    public T save(T entity) {
        Preconditions.checkArgument(entity != null);
        Preconditions.checkArgument(entity.getUuid() != null);
        Preconditions.checkArgument(entityType.isInstance(entity));

        String resName = entity.resourceName();
        Preconditions.checkArgument(resName != null && resName.length() > 0);

        if (checkCopyOnWrite) {
            if (entity.isCachedAndShared() || cache.get(resName) == entity) {
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
        cache.remove(resName);
    }

    public List<T> getAll() {
        val all = Lists.<T> newArrayList();
        for (String path : store.collectResourceRecursively(resRootPath, resPathSuffix)) {
            T value = get(resourceName(path));
            all.add(value);
        }
        return all;
    }

    public boolean contains(String name) {
        return store.getResource(resourcePath(name)) != null;
    }
}
