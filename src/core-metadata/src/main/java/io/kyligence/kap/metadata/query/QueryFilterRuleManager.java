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

package io.kyligence.kap.metadata.query;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class QueryFilterRuleManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryFilterRuleManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CaseInsensitiveStringCache<QueryFilterRule> cache;
    private CachedCrudAssist<QueryFilterRule> crud;
    private AutoReadWriteLock autoLock = new AutoReadWriteLock();

    public static QueryFilterRuleManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, QueryFilterRuleManager.class);
    }

    // called by reflection
    static QueryFilterRuleManager newInstance(KylinConfig config, String project) throws IOException {
        return new QueryFilterRuleManager(config, project);
    }

    private QueryFilterRuleManager(KylinConfig kylinConfig, String project) throws IOException {
        logger.info("Initializing QueryFilterRuleManager with config " + kylinConfig);

        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() throws IOException {
        this.cache = new CaseInsensitiveStringCache<>(this.kylinConfig, this.project, ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT);

        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRootPath = "/" + this.project + "/" + ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<QueryFilterRule>(store, resourceRootPath, QueryFilterRule.class, cache) {
            @Override
            protected QueryFilterRule initEntityAfterReload(QueryFilterRule entity, String resourceName) {
                return entity;
            }
        };

        crud.reloadAll();

        Broadcaster.getInstance(kylinConfig).registerListener(new Broadcaster.Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey) throws IOException {
                try (AutoReadWriteLock.AutoLock lock = QueryFilterRuleManager.this.autoLock.lockForWrite()) {
                    if (Broadcaster.Event.DROP == event) {
                        cache.removeLocal(cacheKey);
                    } else if (Broadcaster.Event.UPDATE == event) {
                        crud.reloadQuietly(cacheKey);
                    }
                }
            }
        }, this.project, ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT);
    }

    public QueryFilterRule save(final QueryFilterRule rule) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            return crud.save(rule);
        }
    }

    public void delete(final QueryFilterRule rule) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            crud.delete(rule);
        }
    }

    public List<QueryFilterRule> getAll() {
        List<QueryFilterRule> queryFilterRules = Lists.newArrayList();

        try (AutoReadWriteLock.AutoLock lock = this.autoLock.lockForRead()) {
            for (Map.Entry<String, QueryFilterRule> entry : cache.getMap().entrySet()) {
                queryFilterRules.add(entry.getValue());
            }
        }

        logger.debug("Loaded " + queryFilterRules.size() + " rules");
        return queryFilterRules;
    }

    public List<QueryFilterRule> getAllEnabled() {
        List<QueryFilterRule> enabledRules = Lists.newArrayList();

        for (QueryFilterRule rule : getAll()) {
            if (rule.isEnabled()) {
                enabledRules.add(rule);
            }
        }

        return enabledRules;
    }

    public QueryFilterRule get(String id) {
        try (AutoReadWriteLock.AutoLock lock = autoLock.lockForRead()) {
            return cache.get(id);
        }
    }
}
