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

package org.apache.kylin.badquery;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BadQueryHistoryManager {
    public static final Serializer<BadQueryHistory> BAD_QUERY_INSTANCE_SERIALIZER = new JsonSerializer<>(
            BadQueryHistory.class);
    private static final Logger logger = LoggerFactory.getLogger(BadQueryHistoryManager.class);

    public static BadQueryHistoryManager getInstance(KylinConfig config) {
        return config.getManager(BadQueryHistoryManager.class);
    }

    // called by reflection
    static BadQueryHistoryManager newInstance(KylinConfig config) throws IOException {
        return new BadQueryHistoryManager(config);
    }

    // ============================================================================

    private KylinConfig kylinConfig;

    private BadQueryHistoryManager(KylinConfig config) throws IOException {
        logger.info("Initializing BadQueryHistoryManager with config " + config);
        this.kylinConfig = config;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public BadQueryHistory getBadQueriesForProject(String project) throws IOException {
        BadQueryHistory badQueryHistory = getStore().getResource(getResourcePathForProject(project),
                BadQueryHistory.class, BAD_QUERY_INSTANCE_SERIALIZER);
        if (badQueryHistory == null) {
            badQueryHistory = new BadQueryHistory(project);
        }

        logger.debug("Loaded " + badQueryHistory.getEntries().size() + " Bad Query(s)");
        return badQueryHistory;
    }

    public BadQueryHistory upsertEntryToProject(BadQueryEntry badQueryEntry, String project) throws IOException {
        if (StringUtils.isEmpty(project) || badQueryEntry.getAdj() == null || badQueryEntry.getSql() == null)
            throw new IllegalArgumentException();

        BadQueryHistory badQueryHistory = getBadQueriesForProject(project);
        NavigableSet<BadQueryEntry> entries = badQueryHistory.getEntries();

        entries.remove(badQueryEntry); // in case the entry already exists and this call means to update

        entries.add(badQueryEntry);

        int maxSize = kylinConfig.getBadQueryHistoryNum();
        if (entries.size() > maxSize) {
            entries.pollFirst();
        }

        getStore().putResource(badQueryHistory.getResourcePath(), badQueryHistory, BAD_QUERY_INSTANCE_SERIALIZER);
        return badQueryHistory;
    }

    public void removeBadQueryHistory(String project) throws IOException {
        getStore().deleteResource(getResourcePathForProject(project));
    }

    public String getResourcePathForProject(String project) {
        return ResourceStore.BAD_QUERY_RESOURCE_ROOT + "/" + project + MetadataConstants.FILE_SURFIX;
    }
}