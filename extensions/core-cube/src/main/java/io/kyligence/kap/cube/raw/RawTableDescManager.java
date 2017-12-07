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

package io.kyligence.kap.cube.raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager class for RawTableDesc
 */
public class RawTableDescManager {

    private static final Logger logger = LoggerFactory.getLogger(RawTableDescManager.class);

    public static final Serializer<RawTableDesc> DESC_SERIALIZER = new JsonSerializer<RawTableDesc>(RawTableDesc.class);

    public static RawTableDescManager getInstance(KylinConfig config) {
        return config.getManager(RawTableDescManager.class);
    }

    // called by reflection
    static RawTableDescManager newInstance(KylinConfig config) throws IOException {
        return new RawTableDescManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // name ==> RawTableDesc
    private CaseInsensitiveStringCache<RawTableDesc> rawTableDescMap;

    private RawTableDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing RawTableDescManager with config " + config);
        this.config = config;
        this.rawTableDescMap = new CaseInsensitiveStringCache<RawTableDesc>(config, "raw_table_desc");

        // touch lower level metadata before registering my listener
        reloadAllRawTableDesc();
        Broadcaster.getInstance(config).registerListener(new RawTableDescSyncListener(), "raw_table_desc");
    }

    private class RawTableDescSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof RawTableInstance) {
                    String descName = ((RawTableInstance) real).getDescName();
                    reloadRawTableDescLocal(descName);
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String descName = cacheKey;
            RawTableDesc desc = getRawTableDesc(descName);
            String modelName = desc == null ? null : desc.getModelName();

            if (event == Event.DROP)
                removeRawTableDescLocal(descName);
            else
                reloadRawTableDescLocal(descName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
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
        if (rawTableDesc.isDraft())
            throw new IllegalArgumentException("RawTableDesc '" + rawTableDesc.getName() + "' must not be a draft");

        rawTableDesc.init(config);

        String path = rawTableDesc.getResourcePath();
        getStore().putResource(path, rawTableDesc, DESC_SERIALIZER);
        rawTableDescMap.put(rawTableDesc.getName(), rawTableDesc);

        return rawTableDesc;
    }

    public void removeRawTableDesc(RawTableDesc rawTableDesc) throws IOException {
        String path = rawTableDesc.getResourcePath();
        getStore().deleteResource(path);
        rawTableDescMap.remove(rawTableDesc.getName());
    }

    public void removeRawTableDescLocal(String name) throws IOException {
        rawTableDescMap.removeLocal(name);
    }

    void reloadAllRawTableDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading RawTableDesc from folder "
                + store.getReadableResourcePath(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT));

        rawTableDescMap.clear();

        List<String> paths = store.collectResourceRecursively(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            RawTableDesc desc;
            try {
                desc = loadRawTableDesc(path);
            } catch (Exception e) {
                logger.error("Error loading RawTableDesc " + path, e);
                continue;
            }
            if (path.equals(desc.getResourcePath()) == false) {
                logger.error(
                        "Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
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
        if (desc.getUuid() == null || desc.getName() == null)
            throw new IllegalArgumentException();
        String name = desc.getName();
        if (!rawTableDescMap.containsKey(name))
            throw new IllegalArgumentException("RawTableDesc '" + name + "' does not exist.");
        if (desc.isDraft())
            throw new IllegalArgumentException("RawTableDesc '" + desc.getName() + "' must not be a draft");

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
