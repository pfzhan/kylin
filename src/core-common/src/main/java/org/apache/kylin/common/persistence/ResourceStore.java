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

package org.apache.kylin.common.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A general purpose resource store to persist small metadata, like JSON files.
 *
 * In additional to raw bytes save and load, the store takes special care for concurrent modifications
 * by using a timestamp based test-and-set mechanism to detect (and refuse) dirty writes.
 */
public abstract class ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);

    public static final String CUBE_RESOURCE_ROOT = "/cube";
    public static final String DATA_MODEL_DESC_RESOURCE_ROOT = "/model_desc";
    public static final String DICT_RESOURCE_ROOT = "/dict";
    public static final String PROJECT_RESOURCE_ROOT = "/project";
    public static final String SNAPSHOT_RESOURCE_ROOT = "/table_snapshot";
    public static final String TABLE_EXD_RESOURCE_ROOT = "/table_exd";
    public static final String TEMP_STATMENT_RESOURCE_ROOT = "/temp_statement";
    public static final String TABLE_RESOURCE_ROOT = "/table";
    public static final String EXTERNAL_FILTER_RESOURCE_ROOT = "/ext_filter";
    public static final String EXECUTE_RESOURCE_ROOT = "/execute";
    public static final String EXECUTE_OUTPUT_RESOURCE_ROOT = "/execute_output";
    public static final String STREAMING_RESOURCE_ROOT = "/streaming";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";
    public static final String USER_ROOT = "/user";
    public static final String USER_GROUP_ROOT = "/user_group";
    public static final String EVENT_RESOURCE_ROOT = "/event";
    public static final String DATA_LOADING_RANGE_RESOURCE_ROOT = "/loading_range";
    public static final String QUERY_FILTER_RULE_RESOURCE_ROOT = "/rule";

    public static final String PROJECT_DICT_RESOURCE_ROOT = DICT_RESOURCE_ROOT + "/project_dict";
    public static final String SPARDER_DICT_RESOURCE_ROOT = "/sparder/sdict";

    public static final String GLOBAL_DICT_RESOURCE_ROOT = DICT_RESOURCE_ROOT + "/global_dict";

    public static final String METASTORE_UUID_TAG = "/UUID";
    public static final String QUERY_HISTORY_TIME_OFFSET = "/query_history_time_offset.json";

    private static final Map<KylinConfig, ResourceStore> META_CACHE = new ConcurrentHashMap<>();

    public interface IKylinMetaStoreFactory {
        ResourceStore createMetaStore(KylinConfig config);
    }

    /**
     * Get a resource store for Kylin's metadata.
     */
    public static ResourceStore getKylinMetaStore(KylinConfig config) {
        ResourceStore store = META_CACHE.get(config);
        if (store != null)
            return store;

        synchronized (ResourceStore.class) {
            store = META_CACHE.get(config);
            if (store == null) {
                store = createKylinMetaStore(config);
                META_CACHE.put(config, store);

                if (isPotentialMemoryLeak()) {
                    logger.warn("Cached {} kylin meta stores, memory leak?", META_CACHE.size(),
                            new RuntimeException());
                }
            }
        }
        return store;
    }

    public static boolean isPotentialMemoryLeak() {
        return META_CACHE.size() > 100;
    }

    public static void clearCache() {
        META_CACHE.clear();
    }

    public static void clearCache(KylinConfig config) {
        META_CACHE.remove(config);
    }

    private static ResourceStore createKylinMetaStore(KylinConfig config) {
        ResourceStore store;

        String factoryClz = config.getMetaStoreFactory();
        if (factoryClz == null || factoryClz.isEmpty()) {
            store = createResourceStore(config, config.getMetadataUrl());
        } else {
            IKylinMetaStoreFactory factory = (IKylinMetaStoreFactory) ClassUtil.newInstance(factoryClz);
            store = factory.createMetaStore(config);
        }

        try {
            if (!store.exists(METASTORE_UUID_TAG)) {
                store.putResource(METASTORE_UUID_TAG, new StringEntity(store.createMetaStoreUUID()), 0, StringEntity.serializer);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to init metadata store", e);
        }

        return store;
    }

    /**
     * Create a resource store for general purpose, according specified by given StorageURL.
     */
    public static ResourceStore createResourceStore(KylinConfig config, StorageURL url) {
        logger.info("Creating resource store by {}", url);
        String clsName = config.getResourceStoreImpls().get(url.getScheme());
        try {
            Class<? extends ResourceStore> cls = ClassUtil.forName(clsName, ResourceStore.class);
            return cls.getConstructor(KylinConfig.class, StorageURL.class).newInstance(config, url);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed to create metadata store by url: " + url, e);
        }
    }

    // ============================================================================

    protected final KylinConfig kylinConfig;
    protected final StorageURL storageUrl;

    protected ResourceStore(KylinConfig kylinConfig, StorageURL storageUrl) {
        this.kylinConfig = kylinConfig;
        this.storageUrl = storageUrl;
    }

    public final KylinConfig getConfig() {
        return kylinConfig;
    }

    public final StorageURL getStorageUrl() {
        return storageUrl;
    }

    /**
     * List resources and sub-folders under a given folder, return null if given path is not a folder
     */
    public final NavigableSet<String> listResources(String folderPath) throws IOException {
        String path = norm(folderPath);
        return listResourcesImpl(path, false);
    }

    public final NavigableSet<String> listResourcesRecursively(String folderPath) throws IOException {
        String path = norm(folderPath);
        return listResourcesImpl(path, true);
    }

    /**
     * return null if given path is not a folder or not exists
     */
    protected abstract NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException;

    protected String createMetaStoreUUID() {
        return String.valueOf(UUID.randomUUID());
    }

    public String getMetaStoreUUID() throws IOException {
        if (!exists(ResourceStore.METASTORE_UUID_TAG)) {
            putResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(createMetaStoreUUID()), 0, StringEntity.serializer);
        }
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.class, StringEntity.serializer);
        return String.valueOf(entity);
    }

    /**
     * Return true if a resource exists, return false in case of folder or non-exist
     */
    public final boolean exists(String resPath) throws IOException {
        return existsImpl(norm(resPath));
    }

    protected abstract boolean existsImpl(String resPath) throws IOException;

    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    public final <T extends RootPersistentEntity> T getResource(String resPath, Class<T> clz, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        RawResource res = getResourceImpl(resPath);
        if (res == null)
            return null;

        DataInputStream din = new DataInputStream(res.inputStream);
        try {
            T r = serializer.deserialize(din);
            r.setLastModified(res.timestamp);
            return r;
        } finally {
            IOUtils.closeQuietly(din);
            IOUtils.closeQuietly(res.inputStream);
        }
    }

    public final RawResource getResource(String resPath) throws IOException {
        return getResourceImpl(norm(resPath));
    }

    public final long getResourceTimestamp(String resPath) throws IOException {
        return getResourceTimestampImpl(norm(resPath));
    }

    /**
     * Read all resources under a folder. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, Class<T> clazz, Serializer<T> serializer) throws IOException {
        return getAllResources(folderPath, Long.MIN_VALUE, Long.MAX_VALUE, clazz, serializer);
    }

    /**
     * Read all resources under a folder having last modified time between given range. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, long timeStart, long timeEndExclusive, Class<T> clazz, Serializer<T> serializer) throws IOException {
        final List<RawResource> allResources = getAllResourcesImpl(folderPath, timeStart, timeEndExclusive);
        if (allResources == null || allResources.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = Lists.newArrayListWithCapacity(allResources.size());
        try {
            for (RawResource rawResource : allResources) {
                final T element = serializer.deserialize(new DataInputStream(rawResource.inputStream));
                element.setLastModified(rawResource.timestamp);
                result.add(element);
            }
            return result;
        } finally {
            for (RawResource rawResource : allResources) {
                if (rawResource != null)
                    IOUtils.closeQuietly(rawResource.inputStream);
            }
        }
    }

    /**
     * return empty list if given path is not a folder or not exists
     */
    protected abstract List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException;

    /**
     * returns null if not exists
     */
    protected abstract RawResource getResourceImpl(String resPath) throws IOException;

    /**
     * returns 0 if not exists
     */
    protected abstract long getResourceTimestampImpl(String resPath) throws IOException;

    /**
     * overwrite a resource without write conflict check
     */
    public final <T extends RootPersistentEntity> void putResourceWithoutCheck(String resPath, T obj, long ts,
                                                                               Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        logger.trace("Directly saving resource {} (Store {})", resPath, kylinConfig.getMetadataUrl());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        serializer.serialize(obj, dout);
        dout.close();
        buf.close();
        ByteArrayInputStream is = new ByteArrayInputStream(buf.toByteArray());
        putResourceCheckpoint(resPath, is, ts);
        is.close();
    }

    /**
     * overwrite a resource without write conflict check
     */
    public final void putResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        logger.trace("Directly saving resource {} (Store {})", resPath, kylinConfig.getMetadataUrl());
        putResourceCheckpoint(resPath, content, ts);
    }

    private void putResourceCheckpoint(String resPath, InputStream content, long ts) throws IOException {
        beforeChange(resPath);
        putResourceImpl(resPath, content, ts);
    }

    protected abstract void putResourceImpl(String resPath, InputStream content, long ts) throws IOException;

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> long putResource(String resPath, T obj, Serializer<T> serializer) throws IOException {
        return putResource(resPath, obj, System.currentTimeMillis(), serializer);
    }

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> long putResource(String resPath, T obj, long newTS, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);

        long oldTS = obj.getLastModified();
        obj.setLastModified(newTS);

        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();

            newTS = checkAndPutResourceCheckpoint(resPath, buf.toByteArray(), oldTS, newTS);
            obj.setLastModified(newTS); // update again the confirmed TS
            return newTS;
        } catch (Exception e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        }
    }

    private long checkAndPutResourceCheckpoint(String resPath, byte[] content, long oldTS, long newTS) throws IOException {
        beforeChange(resPath);
        return checkAndPutResourceImpl(resPath, content, oldTS, newTS);
    }

    /**
     * checks old timestamp when overwriting existing
     */
    protected abstract long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException;

    /**
     * delete a resource, does nothing on a folder
     */
    public final void deleteResource(String resPath) throws IOException {
        logger.trace("Deleting resource {} (Store {})", resPath, kylinConfig.getMetadataUrl());
        deleteResourceCheckpoint(norm(resPath));
    }

    private void deleteResourceCheckpoint(String resPath) throws IOException {
        beforeChange(resPath);
        deleteResourceImpl(resPath);
    }

    protected abstract void deleteResourceImpl(String resPath) throws IOException;

    /**
     * get a readable string of a resource path
     */
    public final String getReadableResourcePath(String resPath) {
        return getReadableResourcePathImpl(norm(resPath));
    }

    protected abstract String getReadableResourcePathImpl(String resPath);

    private String norm(String resPath) {
        resPath = resPath.trim();
        while (resPath.startsWith("//"))
            resPath = resPath.substring(1);
        while (resPath.endsWith("/"))
            resPath = resPath.substring(0, resPath.length() - 1);
        if (!resPath.startsWith("/"))
            resPath = "/" + resPath;
        return resPath;
    }

    // ============================================================================

    ThreadLocal<Checkpoint> checkpointing = new ThreadLocal<>();

    public Checkpoint checkpoint() {
        Checkpoint cp = checkpointing.get();
        if (cp != null)
            throw new IllegalStateException("A checkpoint has been open for this thread: " + cp);

        cp = new Checkpoint();
        checkpointing.set(cp);
        return cp;
    }

    private void beforeChange(String resPath) throws IOException {
        Checkpoint cp = checkpointing.get();
        if (cp != null)
            cp.beforeChange(resPath);
    }

    public class Checkpoint implements Closeable {

        LinkedHashMap<String, byte[]> origResData = new LinkedHashMap<>();
        LinkedHashMap<String, Long> origResTimestamp = new LinkedHashMap<>();

        private void beforeChange(String resPath) throws IOException {
            if (origResData.containsKey(resPath))
                return;

            RawResource raw = getResourceImpl(resPath);
            if (raw == null) {
                origResData.put(resPath, null);
                origResTimestamp.put(resPath, null);
            } else {
                origResData.put(resPath, readAll(raw.inputStream));
                origResTimestamp.put(resPath, raw.timestamp);
            }
        }

        private byte[] readAll(InputStream inputStream) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, out);
            inputStream.close();
            out.close();
            return out.toByteArray();
        }

        public void rollback() {
            checkThread();

            for (Map.Entry<String, byte[]> entry : origResData.entrySet()) {
                String resPath = entry.getKey();
                logger.debug("Rollbacking {}", resPath);
                try {
                    byte[] data = entry.getValue();
                    Long ts = origResTimestamp.get(resPath);
                    if (data == null || ts == null) {
                        deleteResourceImpl(resPath);
                    } else {
                        putResourceImpl(resPath, new ByteArrayInputStream(data), ts);
                    }
                } catch (IOException ex) {
                    logger.error("Failed to rollback " + resPath, ex);
                }
            }
        }

        @Override
        public void close() throws IOException {
            checkThread();

            origResData = null;
            origResTimestamp = null;
            checkpointing.set(null);
        }

        private void checkThread() {
            Checkpoint cp = checkpointing.get();
            if (this != cp)
                throw new IllegalStateException();
        }
    }

    // ============================================================================

    public static interface Visitor {
        void visit(String path) throws IOException;
    }

    public void scanRecursively(String path, Visitor visitor) throws IOException {
        NavigableSet<String> children = listResources(path);
        if (children != null) {
            for (String child : children)
                scanRecursively(child, visitor);
            return;
        }

        if (exists(path))
            visitor.visit(path);
    }

    public List<String> collectResourceRecursively(String root, final String suffix) throws IOException {
        final ArrayList<String> collector = Lists.newArrayList();
        scanRecursively(root, new Visitor() {
            @Override
            public void visit(String path) {
                if (path.endsWith(suffix))
                    collector.add(path);
            }
        });
        return collector;
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList) throws IOException {
        long startTime = System.currentTimeMillis();

        ResourceStore from = ResourceStore.getKylinMetaStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        ResourceStore to = ResourceStore.getKylinMetaStore(localConfig);
        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }
}
