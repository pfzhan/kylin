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

import static org.apache.kylin.common.persistence.ResourceStore.CLUSTER_RESOURCE_ROOT;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.val;

public class KafkaClusterManager {

    public static KafkaClusterManager getInstance(KylinConfig config) {
        return config.getManager(KafkaClusterManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static KafkaClusterManager newInstance(KylinConfig config) {
        return new KafkaClusterManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    private CachedCrudAssist<KafkaCluster> crud;

    private KafkaClusterManager(KylinConfig config) {
        this.config = config;
        this.crud = new CachedCrudAssist<KafkaCluster>(getStore(), CLUSTER_RESOURCE_ROOT, KafkaCluster.class) {
            @Override
            protected KafkaCluster initEntityAfterReload(KafkaCluster t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public KafkaCluster getKafkaCluster() {
        return crud.get(KafkaCluster.RESOURCE_NAME);
    }

    public KafkaCluster updateKafkaBroker(KafkaCluster kafkaCluster) {
        if (kafkaCluster == null) {
            throw new IllegalArgumentException();
        }
        val copy = crud.copyBySerialization(kafkaCluster);
        return crud.save(copy);
    }

}
