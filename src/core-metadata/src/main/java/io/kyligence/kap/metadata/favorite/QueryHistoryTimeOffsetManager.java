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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;

public class QueryHistoryTimeOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryTimeOffsetManager.class);

    public static final Serializer<QueryHistoryTimeOffset> QUERY_HISTORY_TIME_OFFSET_SERIALIZER = new JsonSerializer<>(
            QueryHistoryTimeOffset.class);

    private final KylinConfig kylinConfig;
    private ResourceStore resourceStore;
    private String resourceRootPath;

    public static QueryHistoryTimeOffsetManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, QueryHistoryTimeOffsetManager.class);
    }

    // called by reflection
    static QueryHistoryTimeOffsetManager newInstance(KylinConfig config, String project) {
        return new QueryHistoryTimeOffsetManager(config, project);
    }

    private QueryHistoryTimeOffsetManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing QueryHistoryTimeOffsetManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRootPath = "/" + project + ResourceStore.QUERY_HISTORY_TIME_OFFSET;
    }

    private String path(String uuid) {
        return this.resourceRootPath + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(QueryHistoryTimeOffset time) {
        resourceStore.checkAndPutResource(path(time.getUuid()), time, QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
    }

    public QueryHistoryTimeOffset get() {
        List<QueryHistoryTimeOffset> queryHistoryTimeOffsetList = resourceStore.getAllResources(resourceRootPath,
                QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
        if (queryHistoryTimeOffsetList.isEmpty()) {
            QueryHistoryTimeOffset queryHistoryTimeOffset = new QueryHistoryTimeOffset(System.currentTimeMillis(),
                    System.currentTimeMillis());
            return queryHistoryTimeOffset;
        }

        return queryHistoryTimeOffsetList.get(0);
    }
}
