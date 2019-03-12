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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FavoriteRuleManager {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteRule> crud;

    public static FavoriteRuleManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteRuleManager.class);
    }

    // called by reflection
    static FavoriteRuleManager newInstance(KylinConfig config, String project) throws IOException {
        return new FavoriteRuleManager(config, project);
    }

    private FavoriteRuleManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing FavoriteRuleManager with config {} for project {}", kylinConfig, project);

        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() {

        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRootPath = "/" + this.project + ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FavoriteRule>(store, resourceRootPath, FavoriteRule.class) {
            @Override
            protected FavoriteRule initEntityAfterReload(FavoriteRule entity, String resourceName) {
                return entity;
            }
        };

        crud.setCheckCopyOnWrite(true);
        crud.reloadAll();
    }

    public void createRule(final FavoriteRule rule) {
        if (getByName(rule.getName()) != null)
            return;

        crud.save(rule);
    }

    public void appendSqlPatternToBlacklist(FavoriteRule.SQLCondition newCondition) {
        FavoriteRule blacklist = crud.copyForWrite(getByName(FavoriteRule.BLACKLIST_NAME));
        List<FavoriteRule.AbstractCondition> conditions = blacklist.getConds();

        if (conditions.contains(newCondition))
            return;

        conditions.add(newCondition);
        blacklist.setConds(conditions);
        crud.save(blacklist);
    }

    public void removeSqlPatternFromBlacklist(String id) {
        FavoriteRule rule = crud.copyForWrite(getByName(FavoriteRule.BLACKLIST_NAME));
        List<FavoriteRule.AbstractCondition> conditions = rule.getConds();

        for (int i = 0; i < conditions.size(); i++) {
            FavoriteRule.SQLCondition sqlCondition = (FavoriteRule.SQLCondition) conditions.get(i);
            if (id.equals(sqlCondition.getId())) {
                conditions.remove(sqlCondition);
            }
        }

        rule.setConds(conditions);
        crud.save(rule);
    }

    public void updateRule(List<FavoriteRule.Condition> conditions, boolean isEnabled, String ruleName) {
        FavoriteRule copy = crud.copyForWrite(getByName(ruleName));
        copy.setEnabled(isEnabled);

        List<FavoriteRule.AbstractCondition> newConditions = Lists.newArrayList();
        if (!conditions.isEmpty()) {
            for (FavoriteRule.AbstractCondition condition : conditions) {
                newConditions.add(condition);
            }
        }

        copy.setConds(newConditions);
        crud.save(copy);
    }

    public List<FavoriteRule> getAll() {
        List<FavoriteRule> favoriteRules = Lists.newArrayList();

        favoriteRules.addAll(crud.listAll());
        return favoriteRules;
    }

    public List<FavoriteRule> getAllEnabled() {
        List<FavoriteRule> enabledRules = Lists.newArrayList();

        for (FavoriteRule rule : getAll()) {
            if (rule.isEnabled()) {
                enabledRules.add(rule);
            }
        }

        return enabledRules;
    }

    public FavoriteRule getByName(String name) {
        for (FavoriteRule rule : getAll()) {
            if (rule.getName().equals(name))
                return rule;
        }

        return null;
    }

    public Set<String> getBlacklistSqls() {
        val blacklist = getByName(FavoriteRule.BLACKLIST_NAME);
        return blacklist.getConds().stream().map(cond -> ((FavoriteRule.SQLCondition) cond).getSqlPattern())
                .collect(Collectors.toSet());
    }
}
