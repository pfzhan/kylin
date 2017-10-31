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

package io.kyligence.kap.smart.cube;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelOptimizeLogManager {
    private static final Logger logger = LoggerFactory.getLogger(ModelOptimizeLogManager.class);
    public static final Serializer<ModelOptimizeLog> MODEL_OPTIMIZE_LOG_STATISTICS_SERIALIZER = new JsonSerializer<>(
            ModelOptimizeLog.class);
    
    public static final String MODEL_OPTIMIZE_LOG_STATISTICS_ROOT = "/model_opt_log";

    public static ModelOptimizeLogManager getInstance(KylinConfig config) {
        return config.getManager(ModelOptimizeLogManager.class);
    }

    // called by reflection
    static ModelOptimizeLogManager newInstance(KylinConfig config) throws IOException {
        return new ModelOptimizeLogManager(config);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    
    private ModelOptimizeLogManager(KylinConfig config) throws IOException {
        logger.info("Initializing ModelOptimizeLogManager with config " + config);
        this.kylinConfig = config;
        Broadcaster.getInstance(config).registerListener(new ModelOptimizeLogManager.ModelSyncListener(), "model");
    }

    private class ModelSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String modelName = cacheKey;

            if (event == Broadcaster.Event.DROP) {
                removeModelOptimizeLog(modelName);
                logger.info("Remove ModelOptimizeLog: {}", modelName);
            } else
                // TODO: should update ModelOptimizeLog
                logger.info("Model {} changed, should mark related ModelOptimizeLog obsoleted?", modelName); // should mark current model stats obsoleted?
        }
    }

    public ModelOptimizeLog getModelOptimizeLog(String modelName) throws IOException {

        ModelOptimizeLog result = getStore().getResource(getResourcePath(modelName), ModelOptimizeLog.class,
                MODEL_OPTIMIZE_LOG_STATISTICS_SERIALIZER);
        if (result == null) {
            result = new ModelOptimizeLog();
            result.setModelName(modelName);
        }
        return result;
    }

    public void saveModelOptimizeLog(ModelOptimizeLog modelOptimizeLog) throws IOException {
        if (modelOptimizeLog.getModelName() == null) {
            throw new IllegalArgumentException();
        }

        String path = modelOptimizeLog.getResourcePath();
        getStore().putResource(path, modelOptimizeLog, MODEL_OPTIMIZE_LOG_STATISTICS_SERIALIZER);
    }

    public void removeModelOptimizeLog(String modelName) throws IOException {
        String path = getResourcePath(modelName);
        getStore().deleteResource(path);
    }

    public String getResourcePath(String modelName) {
        return MODEL_OPTIMIZE_LOG_STATISTICS_ROOT + "/" + modelName + MetadataConstants.FILE_SURFIX;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }
}
