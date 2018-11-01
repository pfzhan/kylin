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
package io.kyligence.kap.metadata.favorite;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryHistoryTimeOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryTimeOffsetManager.class);

    public static final Serializer<QueryHistoryTimeOffset> QUERY_HISTORY_TIME_OFFSET_SERIALIZER = new JsonSerializer<>(
            QueryHistoryTimeOffset.class);

    private final KylinConfig kylinConfig;
    private AutoReadWriteLock autoLock = new AutoReadWriteLock();
    private ResourceStore resourceStore;

    public static QueryHistoryTimeOffsetManager getInstance(KylinConfig kylinConfig) {
        return kylinConfig.getManager(QueryHistoryTimeOffsetManager.class);
    }

    // called by reflection
    static QueryHistoryTimeOffsetManager newInstance(KylinConfig config) throws IOException {
        return new QueryHistoryTimeOffsetManager(config);
    }

    private QueryHistoryTimeOffsetManager(KylinConfig kylinConfig) {
        logger.info("Initializing QueryHistoryTimeOffsetManager with config " + kylinConfig);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public void save(QueryHistoryTimeOffset time) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            resourceStore.putResource(ResourceStore.QUERY_HISTORY_TIME_OFFSET, time, QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
        }
    }

    public QueryHistoryTimeOffset get() throws IOException {
        return resourceStore.getResource(ResourceStore.QUERY_HISTORY_TIME_OFFSET, QueryHistoryTimeOffset.class, QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
    }
}
