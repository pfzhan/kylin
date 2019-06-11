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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.AuditLogStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import lombok.Getter;
import lombok.val;

/**
 * A general purpose resource store to persist small metadata, like JSON files.
 *
 * In additional to raw bytes save and load, the store takes special care for concurrent modifications
 * by using a timestamp based test-and-set mechanism to detect (and refuse) dirty writes.
 */
public abstract class ResourceStore implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);

    public static final String GLOBAL_PROJECT = "/_global";
    public static final String USER_ROOT = GLOBAL_PROJECT + "/user";
    public static final String USER_GROUP_ROOT = GLOBAL_PROJECT + "/user_group";
    public static final String ACL_ROOT = GLOBAL_PROJECT + "/acl";
    public static final String PROJECT_ROOT = GLOBAL_PROJECT + "/project";

    public static final String DATA_MODEL_DESC_RESOURCE_ROOT = "/model_desc";
    public static final String TABLE_EXD_RESOURCE_ROOT = "/table_exd";
    public static final String TEMP_STATMENT_RESOURCE_ROOT = "/temp_statement";
    public static final String TABLE_RESOURCE_ROOT = "/table";
    public static final String EXTERNAL_FILTER_RESOURCE_ROOT = "/ext_filter";
    public static final String EXECUTE_RESOURCE_ROOT = "/execute";
    public static final String STREAMING_RESOURCE_ROOT = "/streaming";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";
    public static final String EVENT_RESOURCE_ROOT = "/event";
    public static final String DATA_LOADING_RANGE_RESOURCE_ROOT = "/loading_range";
    public static final String QUERY_FILTER_RULE_RESOURCE_ROOT = "/rule";
    public static final String FAVORITE_QUERY_RESOURCE_ROOT = "/favorite";
    public static final String JOB_STATISTICS = "/job_stats";

    public static final String METASTORE_IMAGE = "/_image";
    public static final String METASTORE_UUID_TAG = "/UUID";
    public static final String QUERY_HISTORY_TIME_OFFSET = "/query_history_time_offset";
    public static final String ACCELERATE_RATIO_RESOURCE_ROOT = "/accelerate_ratio";

    private static final Map<KylinConfig, ResourceStore> META_CACHE = new ConcurrentHashMap<>();
    @Getter
    protected MetadataStore metadataStore;

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
                    logger.warn("Cached {} kylin meta stores, memory leak?", META_CACHE.size(), new RuntimeException());
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

    public static void setRS(KylinConfig config, ResourceStore rs) {
        META_CACHE.put(config, rs);
    }

    private static ResourceStore createKylinMetaStore(KylinConfig config) {
        ResourceStore store = createResourceStore(config);
        if (!store.exists(METASTORE_UUID_TAG)) {
            val output = ByteStreams.newDataOutput();
            output.writeUTF(store.createMetaStoreUUID());
            store.putResourceWithoutCheck(METASTORE_UUID_TAG, ByteStreams.asByteSource(output.toByteArray()),
                    System.currentTimeMillis(), 0);
        }

        return store;
    }

    /**
     * Create a resource store for general purpose, according specified by given StorageURL.
     */
    private static ResourceStore createResourceStore(KylinConfig config) {
        try (val resourceStore = new InMemResourceStore(config)) {
            val snapshotStore = MetadataStore.createMetadataStore(config);
            resourceStore.init(snapshotStore);
            return resourceStore;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    // ============================================================================

    protected final KylinConfig kylinConfig;

    protected ResourceStore(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public final KylinConfig getConfig() {
        return kylinConfig;
    }

    /**
     * List resources and sub-folders under a given folder, return null if given path is not a folder
     */
    public final NavigableSet<String> listResources(String folderPath) {
        String path = norm(folderPath);
        return listResourcesImpl(path, false);
    }

    public final NavigableSet<String> listResourcesRecursively(String folderPath) {
        String path = norm(folderPath);
        return listResourcesImpl(path, true);
    }

    /**
     * return null if given path is not a folder or not exists
     */
    protected abstract NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive);

    protected void init(MetadataStore metadataStore) throws Exception {
        metadataStore.restore(this);
        this.metadataStore = metadataStore;
    }

    protected String createMetaStoreUUID() {
        return String.valueOf(UUID.randomUUID());
    }

    public String getMetaStoreUUID() {
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.serializer);
        return String.valueOf(entity);
    }

    /**
     * Return true if a resource exists, return false in case of folder or non-exist
     */
    public final boolean exists(String resPath) {
        return existsImpl(norm(resPath));
    }

    protected abstract boolean existsImpl(String resPath);

    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    public final <T extends RootPersistentEntity> T getResource(String resPath, Serializer<T> serializer) {
        resPath = norm(resPath);
        RawResource res = getResourceImpl(resPath);
        if (res == null)
            return null;

        return getResourceFromRawResource(res, serializer);
    }

    private <T extends RootPersistentEntity> T getResourceFromRawResource(RawResource res, Serializer<T> serializer) {
        try (InputStream is = res.getByteSource().openStream(); DataInputStream din = new DataInputStream(is)) {
            T r = serializer.deserialize(din);
            r.setLastModified(res.getTimestamp());
            r.setMvcc(res.getMvcc());
            return r;
        } catch (IOException e) {
            logger.warn("error when deserializing resource: " + res.getResPath(), e);
            return null;
        }
    }

    public final RawResource getResource(String resPath) {
        return getResourceImpl(norm(resPath));
    }

    /**
     * Read all resources under a folder. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, Serializer<T> serializer) {
        return getAllResources(folderPath, Long.MIN_VALUE, Long.MAX_VALUE, serializer);
    }

    /**
     * Read all resources under a folder having last modified time between given range. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, long timeStart,
            long timeEndExclusive, Serializer<T> serializer) {
        final List<RawResource> allResources = getAllResourcesImpl(folderPath, timeStart, timeEndExclusive);
        if (allResources == null || allResources.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = Lists.newArrayListWithCapacity(allResources.size());

        for (RawResource rawResource : allResources) {
            T element = getResourceFromRawResource(rawResource, serializer);
            result.add(element);
        }
        return result;
    }

    /**
     * return empty list if given path is not a folder or not exists
     */
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) {
        NavigableSet<String> resources = listResources(folderPath);
        if (resources == null)
            return Collections.emptyList();

        List<RawResource> result = Lists.newArrayListWithCapacity(resources.size());

        for (String res : resources) {
            RawResource resource = getResourceImpl(res);
            if (resource != null) {// can be null if is a sub-folder
                long ts = resource.getTimestamp();
                if (timeStart <= ts && ts < timeEndExclusive) {
                    result.add(resource);
                }
            }
        }
        return result;
    }

    /**
     * returns null if not exists
     */
    protected abstract RawResource getResourceImpl(String resPath);

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> void checkAndPutResource(String resPath, T obj,
            Serializer<T> serializer) {
        resPath = norm(resPath);

        long oldMvcc = obj.getMvcc();
        obj.setMvcc(oldMvcc + 1);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        try {
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        ByteSource byteSource = ByteStreams.asByteSource(buf.toByteArray());

        val x = checkAndPutResource(resPath, byteSource, oldMvcc);
        obj.setLastModified(x.getTimestamp());
    }

    /**
     * checks old timestamp when overwriting existing
     */
    public abstract RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc);

    /**
     * delete a resource, does nothing on a folder
     */
    public final void deleteResource(String resPath) {
        logger.trace("Deleting resource {}", resPath);
        deleteResourceImpl(resPath);
    }

    protected abstract void deleteResourceImpl(String resPath);

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

        Preconditions.checkArgument(!resPath.contains("//"),
                String.format("input resPath contains consequent slash: %s", resPath));

        return resPath;
    }

    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc) {
        throw new NotImplementedException("Only implemented in InMemoryResourceStore");
    }

    public void catchup() {
        val auditLogStore = getAuditLogStore();
        val raw = getResource(METASTORE_IMAGE);
        try {
            long offset = 0;
            if (raw != null) {
                val imageDesc = JsonUtil.readValue(raw.getByteSource().read(), ImageDesc.class);
                offset = imageDesc.getOffset();
            }
            auditLogStore.restore(this, offset);
        } catch (IOException ignore) {
        }
    }

    public interface Visitor {
        void visit(String path);
    }

    private void scanRecursively(String path, Visitor visitor) {
        NavigableSet<String> children = listResources(path);
        if (children != null) {
            for (String child : children)
                scanRecursively(child, visitor);
            return;
        }

        if (exists(path))
            visitor.visit(path);
    }

    public List<String> collectResourceRecursively(String root, final String suffix) {
        final ArrayList<String> collector = Lists.newArrayList();
        scanRecursively(root, path -> {
            if (path.endsWith(suffix))
                collector.add(path);
        });
        return collector;
    }

    public void close() {
        clearCache(this.getConfig());
    }

    public static void dumpResourceMaps(KylinConfig kylinConfig, File metaDir, Map<String, RawResource> dumpMap, Properties properties) {
        long startTime = System.currentTimeMillis();
        metaDir.mkdirs();
        for (Map.Entry<String, RawResource> entry: dumpMap.entrySet()) {
            RawResource res = entry.getValue();
            if (res == null) {
                throw new IllegalStateException("No resource found at -- " + entry.getKey());
            }
            try {
                File f = Paths.get(metaDir.getAbsolutePath(), res.getResPath()).toFile();
                f.getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(f)) {
                    IOUtils.copy(res.getByteSource().openStream(), out);
                    if (!f.setLastModified(res.getTimestamp())) {
                        logger.info("{} modified time change failed", f);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("dump " + res.getResPath() + " failed", e);
            }
        }
        if (properties != null) {
            File kylinPropsFile = new File(metaDir, "kylin.properties");
            try (FileOutputStream os = new FileOutputStream(kylinPropsFile)) {
                properties.store(os, kylinPropsFile.getAbsolutePath());
            } catch (Exception e) {
                throw new IllegalStateException("save kylin.properties failed", e);
            }

        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList,
            Properties properties) {
        long startTime = System.currentTimeMillis();

        metaDir.mkdirs();
        ResourceStore from = ResourceStore.getKylinMetaStore(kylinConfig);

        if (dumpList == null) {
            dumpList = from.listResourcesRecursively("/");
        }

        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            try {
                File f = Paths.get(metaDir.getAbsolutePath(), res.getResPath()).toFile();
                f.getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(f)) {
                    IOUtils.copy(res.getByteSource().openStream(), out);
                    if (!f.setLastModified(res.getTimestamp())) {
                        logger.info("{} modified time change failed", f);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("dump " + res.getResPath() + " failed", e);
            }
        }

        if (properties != null) {
            File kylinPropsFile = new File(metaDir, "kylin.properties");
            try (FileOutputStream os = new FileOutputStream(kylinPropsFile)) {
                properties.store(os, kylinPropsFile.getAbsolutePath());
            } catch (Exception e) {
                throw new IllegalStateException("save kylin.properties failed", e);
            }

        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList) {
        dumpResources(kylinConfig, metaDir, dumpList, null);
    }

    public static void dumpResources(KylinConfig kylinConfig, String dumpDir) {
        dumpResources(kylinConfig, new File(dumpDir), null, null);
    }

    public void copy(String resPath, ResourceStore destRS) {
        val resource = getResource(resPath);
        if (resource != null) {
            //res is a file
            destRS.putResourceWithoutCheck(resPath, resource.getByteSource(), resource.getTimestamp(),
                    resource.getMvcc());
        } else {
            NavigableSet<String> resources = listResourcesRecursively(resPath);
            if (resources == null || resources.isEmpty()) {
                return;
            }
            for (val res : resources) {
                val rawResource = getResource(res);
                if (rawResource == null) {
                    logger.warn("The resource {} doesn't exists,there may be transaction problems here", res);
                    continue;
                }
                destRS.putResourceWithoutCheck(res, rawResource.getByteSource(), rawResource.getTimestamp(),
                        rawResource.getMvcc());
            }
        }
    }

    public AuditLogStore getAuditLogStore() {
        return getMetadataStore().getAuditLogStore();
    }
}
