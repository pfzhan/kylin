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
package io.kyligence.kap.common.persistence.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;

import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MetadataStore {

    static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("/UUID");

    public static MetadataStore createMetadataStore(KylinConfig config) {
        StorageURL url = config.getMetadataUrl();
        log.info("Creating metadata store by KylinConfig {}", config);
        String clsName = config.getMetadataStoreImpls().get(url.getScheme());
        try {
            Class<? extends MetadataStore> cls = ClassUtil.forName(clsName, MetadataStore.class);
            return cls.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    @Getter
    @Setter
    AuditLogStore auditLogStore;

    public MetadataStore(KylinConfig kylinConfig) {
        // for reflection
        auditLogStore = new NoopAuditLogStore();
    }

    protected abstract void save(String path, ByteSource bs, long ts, long mvcc) throws Exception;

    public abstract NavigableSet<String> list(String rootPath);

    public abstract RawResource load(String path) throws IOException;

    public void batchUpdate(UnitMessages unitMessages) throws Exception {
        batchUpdate(unitMessages, false);
    }

    public void batchUpdate(UnitMessages unitMessages, boolean skipAuditLog) throws Exception {
        for (Event event : unitMessages.getMessages()) {
            if (event instanceof ResourceCreateOrUpdateEvent) {
                val rawResource = ((ResourceCreateOrUpdateEvent) event).getCreatedOrUpdated();
                putResource(rawResource);
            } else if (event instanceof ResourceDeleteEvent) {
                deleteResource(((ResourceDeleteEvent) event).getResPath());
            }
        }
        if(!skipAuditLog) {
            auditLogStore.save(unitMessages);
        }
    }

    public void restore(ResourceStore store) throws IOException {
        val all = list("/");
        for (String resPath : all) {
            val raw = load(resPath);
            store.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTimestamp(), raw.getMvcc());
        }
    }

    public void putResource(RawResource res) throws Exception {
        save(res.getResPath(), res.getByteSource(), res.getTimestamp(), res.getMvcc());
    }

    public void deleteResource(String resPath) throws Exception {
        save(resPath, null, 0, 0);
    }

    public void dump(ResourceStore store) throws Exception {
        dump(store, "/");
    }

    public void dump(ResourceStore store, String rootPath) throws Exception {
        val resources = store.listResourcesRecursively(rootPath);
        if (resources == null || resources.isEmpty()) {
            log.info("there is no resources in rootPath ({}),please check the rootPath.", rootPath);
            return;
        }
        for (String resPath : resources) {
            val raw = store.getResource(resPath);
            putResource(raw);
        }
    }

    /**
     * upload local files to snapshot, will not change resource store synchronized, perhaps should call ResourceStore.clearCache() manually
     * @param folder local directory contains snapshot, ${folder}/metadata contains resource store, ${folder}/events contains event store, ${folder}/kylin.properties etc.
     */
    public void uploadFromFile(File folder) {
        foreachFile(folder, res -> {
            try {
                if (IMMUTABLE_PREFIX.contains(res.getResPath())) {
                    return;
                }
                save(res.getResPath(), res.getByteSource(), res.getTimestamp(), res.getMvcc());
            } catch (Exception e) {
                throw new IllegalArgumentException("put resource " + res.getResPath() + " failed", e);
            }
        });
    }

    static void foreachFile(File root, Consumer<RawResource> resourceConsumer) {
        if (!root.exists()) {
            return;
        }
        val files = FileUtils.listFiles(root, null, true);
        files.forEach(f -> {
            try (val fis = new FileInputStream(f)) {
                val resPath = f.getPath().replace(root.getPath(), "");
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(fis));
                val raw = new RawResource(resPath, bs, f.lastModified(), 0);
                resourceConsumer.accept(raw);
            } catch (IOException e) {
                throw new IllegalArgumentException("cannot not read file " + f, e);
            }
        });
    }
}