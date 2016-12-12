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

package io.kyligence.kap.engine.mr.modelstats;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelStatsManager {

    private static final Logger logger = LoggerFactory.getLogger(ModelStatsManager.class);
    public static final Serializer<ModelStats> MODEL_STATISTICS_SERIALIZER = new JsonSerializer<>(ModelStats.class);
    private static final ConcurrentHashMap<KylinConfig, ModelStatsManager> CACHE = new ConcurrentHashMap<>();
    private KylinConfig kylinConfig;

    public static final String MODEL_STATISTICS_ROOT = "/model_stats";

    private ModelStatsManager(KylinConfig config) throws IOException {
        logger.info("Initializing ModelStatsManager with config " + config);
        this.kylinConfig = config;
    }

    public static ModelStatsManager getInstance(KylinConfig config) {
        ModelStatsManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (ModelStatsManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new ModelStatsManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init ModelStatsManager from " + config, e);
            }
        }
    }

    public ModelStats getModelStats(String modelName) throws IOException {

        ModelStats result = getStore().getResource(getResourcePath(modelName), ModelStats.class, MODEL_STATISTICS_SERIALIZER);
        // create new
        if (null == result) {
            result = new ModelStats();
            result.setModelName(modelName);
        }

        return result;
    }

    public void saveModelStats(ModelStats modelStats) throws IOException {
        if (modelStats.getModelName() == null) {
            throw new IllegalArgumentException();
        }

        String path = modelStats.getResourcePath();
        getStore().putResource(path, modelStats, MODEL_STATISTICS_SERIALIZER);
    }

    public void removeModelStats(String modelName) throws IOException {
        String path = getResourcePath(modelName);
        getStore().deleteResource(path);
    }

    public String getResourcePath(String modelName) {
        return MODEL_STATISTICS_ROOT + "/" + modelName + MetadataConstants.FILE_SURFIX;
    }

    public static void clearCache() {
        CACHE.clear();
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }
}
