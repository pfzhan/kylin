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
package org.apache.kylin.common.persistence.image;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;

import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import lombok.Data;
import lombok.val;

public abstract class ImageStore {

    static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("/UUID");

    public static final String METADATA_DIR = "/metadata";
    public static final String EVENT_PROPERTIES_FILE = "/events.json";

    public ImageStore(KylinConfig kylinConfig) {
    }

    protected abstract void saveFile(String path, ByteSource bs, long ts) throws Exception;

    protected abstract NavigableSet<String> listFiles(String rootPath);

    protected abstract RawResource loadFile(String path) throws IOException;

    public void restore(EventStore store) throws IOException {
        val raw = loadFile(EVENT_PROPERTIES_FILE);
        Map props = JsonUtil.readValue(raw.getByteSource().openStream(), Map.class);
        props.forEach((key, value) -> {
            store.getEventStoreProperties().put(key.toString(), value.toString());
        });
    }

    public void restore(ResourceStore store) throws IOException {
        val all = listFiles(METADATA_DIR);
        for (String resPath : all) {
            val raw = loadFile(METADATA_DIR + resPath);
            store.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getMvcc());
        }
    }

    public void putResource(RawResource res) throws Exception {
        saveFile(METADATA_DIR + res.getResPath(), res.getByteSource(), res.getTimestamp());
    }

    public void dump(ResourceStore store) throws Exception {
        for (String resPath : store.listResourcesRecursively("/")) {
            val raw = store.getResource(resPath);
            putResource(raw);
        }
    }

    public void dump(EventStore eventStore) throws Exception {
        val properties = eventStore.getEventStoreProperties();
        saveFile(EVENT_PROPERTIES_FILE, ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(properties)),
                System.currentTimeMillis());
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
                saveFile(res.getResPath(), res.getByteSource(), res.getTimestamp());
            } catch (Exception e) {
                throw new IllegalArgumentException("put resource " + res.getResPath() + " failed", e);
            }
        });
    }

    public static long getMvcc(ByteSource bs) {
        try {
            val wrapper = JsonUtil.readValue(bs.openStream(), MvccWrapper.class);
            return wrapper.getMvcc() + 1;
        } catch (IOException e) {
            return 0;
        }
    }

    static void foreachFile(File root, Consumer<RawResource> resourceConsumer) {
        if (!root.exists()) {
            return;
        }
        val files = FileUtils.listFiles(root, null, true);
        files.forEach(f -> {
            try {
                val resPath = f.getPath().replace(root.getPath(), "");
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(new FileInputStream(f)));
                val raw = new RawResource(resPath, bs, f.lastModified(), getMvcc(bs));
                resourceConsumer.accept(raw);
            } catch (IOException e) {
                throw new IllegalArgumentException("cannot not read file " + f, e);
            }
        });
    }

    @Data
    public static class MvccWrapper {
        private long mvcc = -1;
    }
}
