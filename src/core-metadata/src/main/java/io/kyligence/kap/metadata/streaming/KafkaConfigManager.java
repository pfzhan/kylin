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

package io.kyligence.kap.metadata.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 */
public class KafkaConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigManager.class);

    public static KafkaConfigManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, KafkaConfigManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static KafkaConfigManager newInstance(KylinConfig config, String project) {
        return new KafkaConfigManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;

    // name ==> StreamingConfig
    private CachedCrudAssist<KafkaConfig> crud;

    private KafkaConfigManager(KylinConfig config, String project) {
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.KAFKA_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<KafkaConfig>(getStore(), resourceRootPath, KafkaConfig.class) {
            @Override
            protected KafkaConfig initEntityAfterReload(KafkaConfig t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public KafkaConfig getKafkaConfig(String id) {
        if (org.apache.commons.lang.StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public KafkaConfig createKafkaConfig(KafkaConfig kafkaConfig) {
        if (kafkaConfig == null || StringUtils.isEmpty(kafkaConfig.getName())) {
            throw new IllegalArgumentException();
        }
        if (crud.contains(kafkaConfig.resourceName()))
            throw new IllegalArgumentException("Kafka Config '" + kafkaConfig.getName() + "' already exists");

        kafkaConfig.updateRandomUuid();
        return crud.save(kafkaConfig);
    }

    public KafkaConfig updateKafkaConfig(KafkaConfig kafkaConfig) {
        if (!crud.contains(kafkaConfig.resourceName())) {
            throw new IllegalArgumentException("Kafka Config '" + kafkaConfig.getName() + "' does not exist.");
        }
        return crud.save(kafkaConfig);
    }

    public KafkaConfig removeKafkaConfig(String tableIdentity) {
        KafkaConfig kafkaConfig = getKafkaConfig(tableIdentity);
        if (kafkaConfig == null) {
            logger.warn("Dropping Kafka Config '{}' does not exist", tableIdentity);
            return null;
        }
        crud.delete(kafkaConfig);
        logger.info("Dropping Kafka Config '{}'", tableIdentity);
        return kafkaConfig;
    }

    public List<KafkaConfig> listAllKafkaConfigs() {
        return crud.listAll().stream().collect(Collectors.toList());
    }

    public List<KafkaConfig> getKafkaTablesUsingTable(String table) {
        List<KafkaConfig> kafkaConfigs = new ArrayList<>();
        for (KafkaConfig kafkaConfig : listAllKafkaConfigs()) {
            if (kafkaConfig.hasBatchTable() && kafkaConfig.getBatchTable().equals(table))
                kafkaConfigs.add(kafkaConfig);
        }
        return kafkaConfigs;
    }

    public void invalidCache(String resourceName) {
        crud.invalidateCache(resourceName);
    }

}
