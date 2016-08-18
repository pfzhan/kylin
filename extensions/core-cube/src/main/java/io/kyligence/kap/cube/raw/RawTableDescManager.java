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

package io.kyligence.kap.cube.raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager class for RawTableDesc
 */
public class RawTableDescManager {

    private static final Logger logger = LoggerFactory.getLogger(RawTableDescManager.class);

    public static final Serializer<RawTableDesc> DESC_SERIALIZER = new JsonSerializer<RawTableDesc>(RawTableDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, RawTableDescManager> CACHE = new ConcurrentHashMap<KylinConfig, RawTableDescManager>();

    public static RawTableDescManager getInstance(KylinConfig config) {
        RawTableDescManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RawTableDescManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RawTableDescManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init RawTableDescManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // name ==> RawTableDesc
    private CaseInsensitiveStringCache<RawTableDesc> rawTableDescMap;

    private RawTableDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing RawTableDescManager with config " + config);
        this.config = config;
        this.rawTableDescMap = new CaseInsensitiveStringCache<RawTableDesc>(config, Broadcaster.TYPE.INVERTED_INDEX_DESC);
        reloadAllRawTableDesc();
    }

    public RawTableDesc getRawTableDesc(String name) {
        return rawTableDescMap.get(name);
    }

    public List<RawTableDesc> listAllDesc() {
        return new ArrayList<RawTableDesc>(rawTableDescMap.values());
    }

    /**
     * Reload RawTableDesc from resource store. Triggered by an desc update event.
     */
    public RawTableDesc reloadRawTableDescLocal(String name) throws IOException {

        // Save Source
        String path = RawTableDesc.concatResourcePath(name);

        // Reload the RawTableDesc
        RawTableDesc desc = loadRawTableDesc(path);

        // Here replace the old one
        rawTableDescMap.putLocal(desc.getName(), desc);
        Cuboid.reloadCache(name);
        return desc;
    }

    private RawTableDesc loadRawTableDesc(String path) throws IOException {
        ResourceStore store = getStore();
        RawTableDesc desc = store.getResource(path, RawTableDesc.class, DESC_SERIALIZER);

        if (StringUtils.isBlank(desc.getName())) {
            throw new IllegalStateException("RawTableDesc name must not be blank");
        }

        desc.init(config);
        
        return desc;
    }

    /**
     * Create a new RawTableDesc
     */
    public RawTableDesc createRawTableDesc(RawTableDesc rawTableDesc) throws IOException {
        if (rawTableDesc.getUuid() == null || rawTableDesc.getName() == null)
            throw new IllegalArgumentException();
        if (rawTableDescMap.containsKey(rawTableDesc.getName()))
            throw new IllegalArgumentException("RawTableDesc '" + rawTableDesc.getName() + "' already exists");

        rawTableDesc.init(config);
            
        String path = rawTableDesc.getResourcePath();
        getStore().putResource(path, rawTableDesc, DESC_SERIALIZER);
        rawTableDescMap.put(rawTableDesc.getName(), rawTableDesc);

        return rawTableDesc;
    }

    public void removeRawTableDesc(RawTableDesc RawTableDesc) throws IOException {
        String path = RawTableDesc.getResourcePath();
        getStore().deleteResource(path);
        rawTableDescMap.remove(RawTableDesc.getName());
    }

    public void removeRawTableDescLocal(String name) throws IOException {
        rawTableDescMap.removeLocal(name);
    }

    void reloadAllRawTableDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading RawTableDesc from folder " + store.getReadableResourcePath(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT));

        rawTableDescMap.clear();

        List<String> paths = store.collectResourceRecursively(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            RawTableDesc desc;
            try {
                desc = loadRawTableDesc(path);
            } catch (Exception e) {
                logger.error("Error loading RawTableDesc " + path, e);
                continue;
            }
            if (path.equals(desc.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (rawTableDescMap.containsKey(desc.getName())) {
                logger.error("Dup RawTableDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            rawTableDescMap.putLocal(desc.getName(), desc);
        }

        logger.debug("Loaded " + rawTableDescMap.size() + " RawTableDesc(s)");
    }

    /**
     * Update RawTableDesc with the input. Broadcast the event into cluster
     */
    public RawTableDesc updateRawTableDesc(RawTableDesc desc) throws IOException {
        // Validate RawTableDesc
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = desc.getName();
        if (!rawTableDescMap.containsKey(name)) {
            throw new IllegalArgumentException("RawTableDesc '" + name + "' does not exist.");
        }

        desc.init(config);

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, DESC_SERIALIZER);

        // Reload the RawTableDesc
        RawTableDesc ndesc = loadRawTableDesc(path);
        // Here replace the old one
        rawTableDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
