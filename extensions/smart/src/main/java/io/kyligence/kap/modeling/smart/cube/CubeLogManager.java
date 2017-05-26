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

package io.kyligence.kap.modeling.smart.cube;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CubeLogManager {

    private static final Logger logger = LoggerFactory.getLogger(CubeLogManager.class);
    public static final Serializer<CubeLog> CUBE_LOG_STATISTICS_SERIALIZER = new JsonSerializer<>(CubeLog.class);
    private static final ConcurrentMap<KylinConfig, CubeLogManager> CACHE = new ConcurrentHashMap<>();
    private KylinConfig kylinConfig;

    public static final String CUBE_LOG_STATISTICS_ROOT = "/cube_logs";

    private CubeLogManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeLogManager with config " + config);
        this.kylinConfig = config;
    }

    public static CubeLogManager getInstance(KylinConfig config) {
        CubeLogManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (CubeLogManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new CubeLogManager(config);
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

    public CubeLog getCubeLog(String cubeName) throws IOException {

        CubeLog result = getStore().getResource(getResourcePath(cubeName), CubeLog.class,
                CUBE_LOG_STATISTICS_SERIALIZER);
        // create new
        if (null == result) {
            result = new CubeLog();
            result.setCubeName(cubeName);
        }

        return result;
    }

    public void saveCubeLog(CubeLog cubeLog) throws IOException {
        if (cubeLog.getCubeName() == null) {
            throw new IllegalArgumentException();
        }

        String path = cubeLog.getResourcePath();
        getStore().putResource(path, cubeLog, CUBE_LOG_STATISTICS_SERIALIZER);
    }

    public void removeCubeLog(String cubeName) throws IOException {
        String path = getResourcePath(cubeName);
        getStore().deleteResource(path);
    }

    public String getResourcePath(String cubeName) {
        return CUBE_LOG_STATISTICS_ROOT + "/" + cubeName + MetadataConstants.FILE_SURFIX;
    }

    public static void clearCache() {
        CACHE.clear();
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }
}
