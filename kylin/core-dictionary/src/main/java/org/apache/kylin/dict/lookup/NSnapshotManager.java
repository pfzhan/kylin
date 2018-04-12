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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.dict.NDictionaryManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author yangli9
 */
public class NSnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(NSnapshotManager.class);

    public static NSnapshotManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NSnapshotManager.class);
    }

    // called by reflection
    static NSnapshotManager newInstance(KylinConfig config, String project) throws IOException {
        return new NSnapshotManager(config, project);
    }

    // ============================================================================

    private String project;

    private KylinConfig config;

    // path ==> SnapshotTable
    private LoadingCache<String, NSnapshotTable> snapshotCache; // resource

    private NSnapshotManager(KylinConfig config, String project) {
        this.project = project;
        this.config = config;
        this.snapshotCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, NSnapshotTable>() {
            @Override
            public void onRemoval(RemovalNotification<String, NSnapshotTable> notification) {
                NSnapshotManager.logger.info("Snapshot with resource path " + notification.getKey()
                        + " is removed due to " + notification.getCause());
            }
        }).maximumSize(config.getCachedSnapshotMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, NSnapshotTable>() {
                    @Override
                    public NSnapshotTable load(String key) throws Exception {
                        NSnapshotTable snapshotTable = NSnapshotManager.this.load(key, true);
                        return snapshotTable;
                    }
                });
    }

    public void wipeoutCache() {
        snapshotCache.invalidateAll();
    }

    public NSnapshotTable getSnapshotTable(String resourcePath) throws IOException {
        try {
            NSnapshotTable r = snapshotCache.get(resourcePath);
            if (r == null) {
                r = load(resourcePath, true);
                snapshotCache.put(resourcePath, r);
            }
            return r;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void removeSnapshot(String resourcePath) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(resourcePath);
        snapshotCache.invalidate(resourcePath);
    }

    public NSnapshotTable buildSnapshot(IReadableTable table, TableDesc tableDesc) throws IOException {
        NSnapshotTable snapshot = new NSnapshotTable(table, tableDesc.getIdentity(), project);
        snapshot.updateRandomUuid();

        String dup = checkDupByInfo(snapshot);
        if (dup != null) {
            logger.info("Identical input " + table.getSignature() + ", reuse existing snapshot at " + dup);
            return getSnapshotTable(dup);
        }

        if (snapshot.getSignature().getSize() / 1024 / 1024 > config.getTableSnapshotMaxMB()) {
            throw new IllegalStateException("Table snapshot should be no greater than " + config.getTableSnapshotMaxMB() //
                    + " MB, but " + tableDesc + " size is " + snapshot.getSignature().getSize());
        }

        snapshot.takeSnapshot(table, tableDesc);

        return trySaveNewSnapshot(snapshot);
    }

    public NSnapshotTable rebuildSnapshot(IReadableTable table, TableDesc tableDesc, String overwriteUUID)
            throws IOException {
        NSnapshotTable snapshot = new NSnapshotTable(table, tableDesc.getIdentity(), project);
        snapshot.setUuid(overwriteUUID);

        snapshot.takeSnapshot(table, tableDesc);

        NSnapshotTable existing = getSnapshotTable(snapshot.getResourcePath());
        snapshot.setLastModified(existing.getLastModified());

        save(snapshot);
        snapshotCache.put(snapshot.getResourcePath(), snapshot);

        return snapshot;
    }

    public NSnapshotTable trySaveNewSnapshot(NSnapshotTable snapshotTable) throws IOException {

        String dupTable = checkDupByContent(snapshotTable);
        if (dupTable != null) {
            logger.info("Identical snapshot content " + snapshotTable + ", reuse existing snapshot at " + dupTable);
            return getSnapshotTable(dupTable);
        }

        save(snapshotTable);
        snapshotCache.put(snapshotTable.getResourcePath(), snapshotTable);

        return snapshotTable;
    }

    private String checkDupByInfo(NSnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        TableSignature sig = snapshot.getSignature();
        for (String existing : existings) {
            NSnapshotTable existingTable = load(existing, false); // skip cache,
            // direct load from store
            if (existingTable != null && sig.equals(existingTable.getSignature()))
                return existing;
        }

        return null;
    }

    private String checkDupByContent(NSnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        for (String existing : existings) {
            NSnapshotTable existingTable = load(existing, true); // skip cache, direct load from store
            if (existingTable != null && existingTable.equals(snapshot))
                return existing;
        }

        return null;
    }

    private void save(NSnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String path = snapshot.getResourcePath();
        store.putResource(path, snapshot, NSnapshotTableSerializer.FULL_SERIALIZER);
    }

    private NSnapshotTable load(String resourcePath, boolean loadData) throws IOException {
        logger.info("Loading nsnapshotTable from " + resourcePath + ", with loadData: " + loadData);
        ResourceStore store = getStore();

        NSnapshotTable table = store.getResource(resourcePath, NSnapshotTable.class,
                loadData ? NSnapshotTableSerializer.FULL_SERIALIZER : NSnapshotTableSerializer.INFO_SERIALIZER);

        Preconditions.checkNotNull(table);
        table.setProject(project);

        if (loadData)
            logger.debug("Loaded snapshot at " + resourcePath);

        return table;
    }

    // snapshot shares the same store with dictionary
    private ResourceStore getStore() {
        return NDictionaryManager.getInstance(config, project).getStore();
    }
}
