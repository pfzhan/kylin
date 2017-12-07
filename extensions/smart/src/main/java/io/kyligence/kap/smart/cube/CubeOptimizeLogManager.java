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

public class CubeOptimizeLogManager {

    private static final Logger logger = LoggerFactory.getLogger(CubeOptimizeLogManager.class);
    public static final Serializer<CubeOptimizeLog> CUBE_OPTIMIZE_LOG_STATISTICS_SERIALIZER = new JsonSerializer<>(
            CubeOptimizeLog.class);

    public static final String CUBE_OPTIMIZE_LOG_STATISTICS_ROOT = "/cube_opt_log";

    public static CubeOptimizeLogManager getInstance(KylinConfig config) {
        return config.getManager(CubeOptimizeLogManager.class);
    }

    // called by reflection
    static CubeOptimizeLogManager newInstance(KylinConfig config) throws IOException {
        return new CubeOptimizeLogManager(config);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    
    private CubeOptimizeLogManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeOptimizeLogManager with config " + config);
        this.kylinConfig = config;
        Broadcaster.getInstance(config).registerListener(new CubeOptimizeLogManager.CubeSyncListener(), "cube");
    }

    private class CubeSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;

            if (event == Broadcaster.Event.DROP)
                removeCubeOptimizeLog(cubeName);
            else
                // TODO: should update CubeOptimizeLog
                logger.info("Cube {} changed, should mark related CubeOptimizeLog obsoleted?", cubeName); // should mark current model stats obsoleted?
        }
    }

    public CubeOptimizeLog getCubeOptimizeLog(String cubeName) throws IOException {

        CubeOptimizeLog result = getStore().getResource(getResourcePath(cubeName), CubeOptimizeLog.class,
                CUBE_OPTIMIZE_LOG_STATISTICS_SERIALIZER);
        // create new
        if (null == result) {
            result = new CubeOptimizeLog();
            result.setCubeName(cubeName);
        }

        return result;
    }

    public void saveCubeOptimizeLog(CubeOptimizeLog cubeOptimizeLog) throws IOException {
        if (cubeOptimizeLog.getCubeName() == null) {
            throw new IllegalArgumentException();
        }

        String path = cubeOptimizeLog.getResourcePath();
        getStore().putResource(path, cubeOptimizeLog, CUBE_OPTIMIZE_LOG_STATISTICS_SERIALIZER);
    }

    public void removeCubeOptimizeLog(String cubeName) throws IOException {
        String path = getResourcePath(cubeName);
        getStore().deleteResource(path);
    }

    public String getResourcePath(String cubeName) {
        return CUBE_OPTIMIZE_LOG_STATISTICS_ROOT + "/" + cubeName + MetadataConstants.FILE_SURFIX;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }
}
