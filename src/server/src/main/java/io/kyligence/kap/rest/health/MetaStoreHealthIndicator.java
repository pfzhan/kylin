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
package io.kyligence.kap.rest.health;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.rest.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.UUID;

@Component
public class MetaStoreHealthIndicator extends AbstractKylinHealthIndicator {
    public static final Logger logger = LoggerFactory.getLogger(MetaStoreHealthIndicator.class);

    private static final String UNIT_NAME = "_health";
    private static final String HEALTH_ROOT_PATH = "/" + UNIT_NAME;
    private static final String UUID_PATH = "/UUID";
    private static final int MAX_RETRY = 3;

    @Getter
    @Setter
    private String serverMode;

    public MetaStoreHealthIndicator() {
        this.config = KylinConfig.getInstanceFromEnv();
        this.serverMode = this.config.getServerMode();

        this.warningResponseMs = KapConfig.wrap(config).getMetaStoreHealthWarningResponseMs();
        this.errorResponseMs = KapConfig.wrap(config).getMetaStoreHealthErrorResponseMs();
    }

    private Health allNodeCheck() {
        return UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.<Health> builder().skipAuditLog(true)
                .unitName(UNIT_NAME).maxRetry(MAX_RETRY).processor(() -> {
                    ResourceStore store;
                    try {
                        store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get meta store", e);
                    }

                    String uuid = UUID.randomUUID().toString();
                    String resourcePath = HEALTH_ROOT_PATH + "/" + uuid;
                    long start;
                    String op;

                    // test write
                    op = "Writing metadata (40 bytes)";
                    logger.info(op);
                    start = System.currentTimeMillis();
                    try {
                        store.checkAndPutResource(resourcePath, new StringEntity(uuid), StringEntity.serializer);
                        checkTime(start, op);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to write metadata", e);
                    }

                    // test read
                    op = "Reading metadata (40 bytes)";
                    logger.info(op);
                    start = System.currentTimeMillis();
                    try {
                        StringEntity value = store.getResource(resourcePath, StringEntity.serializer);
                        checkTime(start, op);
                        if (!new StringEntity(uuid).equals(value)) {
                            throw new RuntimeException("Metadata store failed to read a newly created resource.");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to read metadata", e);
                    }

                    // test delete
                    op = "Deleting metadata (40 bytes)";
                    logger.info(op);
                    start = System.currentTimeMillis();
                    try {
                        store.deleteResource(resourcePath);
                        checkTime(start, op);
                    } catch (Exception e) {
                        logger.error("Failed to delete metadata", e);
                        throw new RuntimeException("Failed to delete metadata", e);
                    }

                    return Health.up().build();
                }).build());
    }

    private Health queryNodeCheck() {
        return UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.<Health> builder().skipAuditLog(true).readonly(true)
                .unitName(UNIT_NAME).maxRetry(MAX_RETRY).processor(() -> {
                    ResourceStore store;
                    try {
                        store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get meta store", e);
                    }

                    long start;
                    String op;

                    // test read
                    op = "Reading metadata /UUID";
                    logger.info(op);
                    start = System.currentTimeMillis();
                    try {
                        StringEntity value = store.getResource(UUID_PATH, StringEntity.serializer);
                        checkTime(start, op);
                        if (Objects.isNull(value)) {
                            throw new RuntimeException("Metadata store failed to read a resource.");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to read metadata", e);
                    }

                    return Health.up().build();
                }).build());
    }

    @Override
    public Health health() {
        Health ret;
        try {
            if (Constant.SERVER_MODE_ALL.equals(this.serverMode)) {
                ret = allNodeCheck();
            } else {
                ret = queryNodeCheck();
            }
        } catch (Exception e) {
            logger.error("Failed to check the metastore health", e);
            return Health.down().build();
        }

        if (Objects.isNull(ret)) {
            return Health.down().build();
        }

        return ret;
    }
}
