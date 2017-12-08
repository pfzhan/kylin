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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
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
    private CachedCrudAssist<RawTableDesc> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock descMapLock = new AutoReadWriteLock();

    private RawTableDescManager(KylinConfig cfg) throws IOException {
        logger.info("Initializing RawTableDescManager with config " + cfg);
        this.config = cfg;
        this.rawTableDescMap = new CaseInsensitiveStringCache<RawTableDesc>(config, "raw_table_desc");
        this.crud = new CachedCrudAssist<RawTableDesc>(getStore(), RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT,
                RawTableDesc.class, rawTableDescMap) {
            @Override
            protected RawTableDesc initEntityAfterReload(RawTableDesc desc, String resourceName) {
                desc.init(config);
                return desc;
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new RawTableDescSyncListener(), "raw_table_desc");
    }

    private class RawTableDescSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            try (AutoLock lock = descMapLock.lockForWrite()) {
                for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                    if (real instanceof RawTableInstance) {
                        String descName = ((RawTableInstance) real).getDescName();
                        crud.reloadQuietly(descName);
                    }
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String descName = cacheKey;
            RawTableDesc desc = getRawTableDesc(descName);
            String modelName = desc == null ? null : desc.getModelName();

            try (AutoLock lock = descMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    rawTableDescMap.removeLocal(descName);
                else
                    crud.reloadQuietly(descName);
            }

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    public RawTableDesc getRawTableDesc(String name) {
        try (AutoLock lock = descMapLock.lockForRead()) {
            return rawTableDescMap.get(name);
        }
    }

    public List<RawTableDesc> listAllDesc() {
        try (AutoLock lock = descMapLock.lockForRead()) {
            return new ArrayList<RawTableDesc>(rawTableDescMap.values());
        }
    }

    // for test
    void reloadAll() throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            crud.reloadAll();
        }
    }

    /**
     * Reload RawTableDesc from resource store.
     */
    public RawTableDesc reloadRawTableDescLocal(String name) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            return crud.reload(name);
        }
    }

    /**
     * Create a new RawTableDesc
     */
    public RawTableDesc createRawTableDesc(RawTableDesc rawTableDesc) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            if (rawTableDesc.getUuid() == null || rawTableDesc.getName() == null)
                throw new IllegalArgumentException();
            if (rawTableDescMap.containsKey(rawTableDesc.getName()))
                throw new IllegalArgumentException("RawTableDesc '" + rawTableDesc.getName() + "' already exists");
            if (rawTableDesc.isDraft())
                throw new IllegalArgumentException("RawTableDesc '" + rawTableDesc.getName() + "' must not be a draft");

            rawTableDesc.init(config);

            crud.save(rawTableDesc);

            return rawTableDesc;
        }
    }

    public void removeRawTableDesc(RawTableDesc rawTableDesc) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            crud.delete(rawTableDesc);
        }
    }

    /**
     * Update RawTableDesc with the input. Broadcast the event into cluster
     */
    public RawTableDesc updateRawTableDesc(RawTableDesc desc) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            // Validate RawTableDesc
            if (desc.getUuid() == null || desc.getName() == null)
                throw new IllegalArgumentException();
            String name = desc.getName();
            if (!rawTableDescMap.containsKey(name))
                throw new IllegalArgumentException("RawTableDesc '" + name + "' does not exist.");
            if (desc.isDraft())
                throw new IllegalArgumentException("RawTableDesc '" + desc.getName() + "' must not be a draft");

            desc.init(config);

            crud.save(desc);

            return desc;
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
