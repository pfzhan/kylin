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

package io.kyligence.kap.metadata.badquery;

import java.io.IOException;
import java.util.Collection;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class NBadQueryHistoryManager {
    public static final Serializer<NBadQueryHistory> BAD_QUERY_INSTANCE_SERIALIZER = new JsonSerializer<>(
            NBadQueryHistory.class);
    private static final Logger logger = LoggerFactory.getLogger(NBadQueryHistoryManager.class);

    public static NBadQueryHistoryManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NBadQueryHistoryManager.class);
    }

    // called by reflection
    static NBadQueryHistoryManager newInstance(KylinConfig config, String project) throws IOException {
        return new NBadQueryHistoryManager(config, project);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    private String project;

    private NBadQueryHistoryManager(KylinConfig config, String project) throws IOException {
        logger.info("Initializing NBadQueryHistoryManager with config " + config);
        this.kylinConfig = config;
        this.project = project;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public NBadQueryHistory getBadQueriesForProject() throws IOException {
        NBadQueryHistory NBadQueryHistory = getStore().getResource(getResourcePathForProject(), NBadQueryHistory.class,
                BAD_QUERY_INSTANCE_SERIALIZER);
        if (NBadQueryHistory == null) {
            NBadQueryHistory = new NBadQueryHistory(project);
        }

        logger.debug("Loaded " + NBadQueryHistory.getEntries().size() + " Bad Query(s)");
        return NBadQueryHistory;
    }

    public NBadQueryHistory upsertEntryToProject(BadQueryEntry badQueryEntry) throws IOException {
        return upsertEntryToProject(Lists.newArrayList(badQueryEntry));
    }

    public NBadQueryHistory upsertEntryToProject(Collection<BadQueryEntry> badQueryEntries) throws IOException {
        if (StringUtils.isEmpty(project) || badQueryEntries == null)
            throw new IllegalArgumentException();

        NBadQueryHistory NBadQueryHistory = getBadQueriesForProject();
        NavigableSet<BadQueryEntry> entries = NBadQueryHistory.getEntries();

        for (BadQueryEntry entry : badQueryEntries) {
            Preconditions.checkArgument(entry != null && entry.getAdj() != null && entry.getSql() != null);
            entries.remove(entry); // in case the entry already exists and this call means to update
            entries.add(entry);
        }

        int maxSize = kylinConfig.getBadQueryHistoryNum();
        if (entries.size() > maxSize) {
            entries.pollFirst();
        }

        getStore().putResource(NBadQueryHistory.getResourcePath(), NBadQueryHistory, BAD_QUERY_INSTANCE_SERIALIZER);
        return NBadQueryHistory;
    }

    public NBadQueryHistory upsertToProject(NBadQueryHistory NBadQueryHistory) throws IOException {
        if (StringUtils.isEmpty(project) || NBadQueryHistory == null)
            throw new IllegalArgumentException();

        getStore().putResource(NBadQueryHistory.getResourcePath(), NBadQueryHistory, BAD_QUERY_INSTANCE_SERIALIZER);
        return NBadQueryHistory;
    }

    public void removeBadQueryHistory() throws IOException {
        getStore().deleteResource(getResourcePathForProject());
    }

    public String getResourcePathForProject() {
        return "/" + project + ResourceStore.BAD_QUERY_RESOURCE_ROOT + "/" + project + MetadataConstants.FILE_SURFIX;
    }
}