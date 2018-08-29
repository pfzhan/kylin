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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class QueryHistoryManager {
    public static final Serializer<QueryHistory> QUERY_HISTORY_INSTANCE_SERIALIZER = new JsonSerializer<>(
            QueryHistory.class);
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryManager.class);

    private static String QUERY_HISTORY_ROOT_PATH;

    public static QueryHistoryManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, QueryHistoryManager.class);
    }

    // called by reflection
    static QueryHistoryManager newInstance(KylinConfig config, String project) throws IOException {
        return new QueryHistoryManager(config, project);
    }

    private KylinConfig kylinConfig;
    private String project;

    public QueryHistoryManager(KylinConfig config, String project) {
        logger.info("Initializing QueryHistoryManager with config " + config);
        this.kylinConfig = config;
        this.project = project;
        this.QUERY_HISTORY_ROOT_PATH = "/" + project + ResourceStore.QUERY_HISTORY_RESOURCE_ROOT;
    }

    public QueryHistory findQueryHistory(String queryHistoryId) throws IOException {
        if (queryHistoryId == null || StringUtils.isEmpty(queryHistoryId))
            throw new IllegalArgumentException();

        for (QueryHistory queryHistory : getAllQueryHistories()) {
            if (queryHistoryId.equals(queryHistory.getUuid()))
                return queryHistory;
        }

        return null;
    }

    public QueryHistory findQueryHistory(Predicate<QueryHistory> predicate) throws IOException {
        return Iterators.tryFind(getAllQueryHistories().iterator(), predicate).orNull();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public List<QueryHistory> getAllQueryHistories() throws IOException {
        List<QueryHistory> queryHistories = getStore().getAllResources(QUERY_HISTORY_ROOT_PATH,
                QueryHistory.class, QUERY_HISTORY_INSTANCE_SERIALIZER);
        Collections.sort(queryHistories);

        logger.debug("Loaded " + queryHistories.size() + " Query(s)");
        return queryHistories;
    }

    public String getResourcePathForQueryHistory(String resouceName) {
        return QUERY_HISTORY_ROOT_PATH + "/" + resouceName + MetadataConstants.FILE_SURFIX;
    }

    public void upsertEntry(QueryHistory queryHistory) throws IOException {
        upsertEntries(Lists.newArrayList(queryHistory));
    }

    public void upsertEntries(Collection<QueryHistory> queryHistoryEntries) throws IOException {
        if (StringUtils.isEmpty(project) || queryHistoryEntries == null)
            throw new IllegalArgumentException();

        for (QueryHistory queryHistory : queryHistoryEntries) {
            Preconditions.checkArgument(queryHistory != null && queryHistory.resourceName() != null);
            getStore().putResource(getResourcePathForQueryHistory(queryHistory.resourceName()), queryHistory, QUERY_HISTORY_INSTANCE_SERIALIZER);
        }
    }
}
