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

public class QueryHistoryIdOffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryIdOffsetManager.class);

    public static final Serializer<QueryHistoryIdOffset> QUERY_HISTORY_ID_OFFSET_SERIALIZER = new JsonSerializer<>(
            QueryHistoryIdOffset.class);

    private final KylinConfig kylinConfig;
    private ResourceStore resourceStore;
    private String resourceRoot;

    private QueryHistoryIdOffsetManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing QueryHistoryIdOffsetManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRoot = "/" + project + ResourceStore.QUERY_HISTORY_ID_OFFSET;
    }

    // called by reflection
    static QueryHistoryIdOffsetManager newInstance(KylinConfig config, String project) {
        return new QueryHistoryIdOffsetManager(config, project);
    }

    public static QueryHistoryIdOffsetManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, QueryHistoryIdOffsetManager.class);
    }

    private String path(String uuid) {
        return this.resourceRoot + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(QueryHistoryIdOffset idOffset) {
        resourceStore.checkAndPutResource(path(idOffset.getUuid()), idOffset, QUERY_HISTORY_ID_OFFSET_SERIALIZER);
    }

    public QueryHistoryIdOffset get() {
        List<QueryHistoryIdOffset> queryHistoryIdOffsetList = resourceStore.getAllResources(resourceRoot,
                QUERY_HISTORY_ID_OFFSET_SERIALIZER);
        if (queryHistoryIdOffsetList.isEmpty()) {
            return new QueryHistoryIdOffset(0);
        }

        return queryHistoryIdOffsetList.get(0);
    }
}
