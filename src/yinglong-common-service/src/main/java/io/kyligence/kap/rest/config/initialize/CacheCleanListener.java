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

package io.kyligence.kap.rest.config.initialize;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CacheCleanListener implements EventListenerRegistry.ResourceEventListener {

    private static final List<Pattern> PROJECT_RESOURCE_PATTERN = Lists.newArrayList(
            Pattern.compile(ResourceStore.PROJECT_ROOT + "/([^/]+)$"));

    private static final List<Pattern> TABLE_RESOURCE_PATTERN = Lists.newArrayList(
            Pattern.compile("/([^/]+)" + ResourceStore.TABLE_RESOURCE_ROOT + "/([^/]+)"),
            Pattern.compile("/([^/]+)" + ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/([^/]+)"),
            Pattern.compile("/([^/]+)" + ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT + "/([^/]+)"));

    private static final List<Pattern> KAFKA_RESOURCE_PATTERN = Lists
            .newArrayList(Pattern.compile("/([^/]+)" + ResourceStore.KAFKA_RESOURCE_ROOT + "/([^/]+)"));

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        // Do nothing. Cache will update internally because mvcc is changed.
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        try {
            PROJECT_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                if (StringUtils.isNotBlank(project)) {
                    NProjectManager.getInstance(config).invalidCache(project);
                }
            });
            TABLE_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                String table = extractTable(resPath, pattern);
                if (StringUtils.isNotBlank(project) && StringUtils.isNotBlank(table)) {
                    NTableMetadataManager.getInstance(config, project).invalidCache(table);
                }
            });
            KAFKA_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                String kafkaTableName = extractTable(resPath, pattern);
                if (StringUtils.isNotBlank(project) && StringUtils.isNotBlank(kafkaTableName)) {
                    KafkaConfigManager.getInstance(config, project).invalidCache(kafkaTableName);
                }
            });
        } catch (Exception e) {
            log.error("Unexpected error happened! Clean resource {} cache failed.", resPath, e);
        }

    }

    private String extractProject(String resPath, Pattern pattern) {
        Matcher matcher = pattern.matcher(resPath);
        if (matcher.find()) {
            return matcher.group(1).replace(".json", "");
        }
        return null;
    }

    private String extractTable(String resPath, Pattern pattern) {
        Matcher matcher = pattern.matcher(resPath);
        if (matcher.find() && matcher.groupCount() == 2) {
            return matcher.group(2).replace(".json", "");
        }
        return null;
    }
}
