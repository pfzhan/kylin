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
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class FavoriteRuleManager {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteRule> crud;

    protected static final Map<String, String> REVERSE_RULE_NAME_MAP = Maps.newHashMap();

    static {
        REVERSE_RULE_NAME_MAP.put(FavoriteRule.WHITELIST_NAME, FavoriteRule.BLACKLIST_NAME);
        REVERSE_RULE_NAME_MAP.put(FavoriteRule.BLACKLIST_NAME, FavoriteRule.WHITELIST_NAME);
    }

    public static FavoriteRuleManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteRuleManager.class);
    }

    // called by reflection
    static FavoriteRuleManager newInstance(KylinConfig config, String project) throws IOException {
        return new FavoriteRuleManager(config, project);
    }

    private FavoriteRuleManager(KylinConfig kylinConfig, String project) throws IOException {
        logger.info("Initializing FavoriteRuleManager with config " + kylinConfig);

        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() throws IOException {

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

    public FavoriteRule createRule(final FavoriteRule rule) {
        return crud.save(rule);
    }

    public void appendSqlConditions(List<FavoriteRule.SQLCondition> newConditions, String ruleName)
            throws RuleConditionExistException {
        FavoriteRule rule = crud.copyBySerialization(getByName(ruleName));
        List<FavoriteRule.AbstractCondition> conditions = rule.getConds();

        for (FavoriteRule.SQLCondition newCondition : newConditions) {
            checkIfInOppositeList(newCondition, REVERSE_RULE_NAME_MAP.get(ruleName));

            if (conditions.contains(newCondition))
                continue;

            conditions.add(newCondition);
        }

        rule.setConds(conditions);
        crud.save(rule);
    }

    private void checkIfInOppositeList(FavoriteRule.SQLCondition sqlCondition, String ruleName)
            throws RuleConditionExistException {
        FavoriteRule rule = getByName(ruleName);
        for (FavoriteRule.AbstractCondition condition : rule.getConds()) {
            if (sqlCondition.getSqlPatternHash() == ((FavoriteRule.SQLCondition) condition).getSqlPatternHash())
                throw new RuleConditionExistException();
        }
    }

    public void removeSqlCondition(String id, String ruleName) throws IOException {
        FavoriteRule rule = crud.copyBySerialization(getByName(ruleName));
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

    public FavoriteRule.SQLCondition updateWhitelistSql(FavoriteRule.SQLCondition updatedCondition)
            throws RuleConditionExistException {
        checkIfInOppositeList(updatedCondition, REVERSE_RULE_NAME_MAP.get(FavoriteRule.WHITELIST_NAME));

        FavoriteRule whitelist = crud.copyBySerialization(getByName(FavoriteRule.WHITELIST_NAME));

        List<FavoriteRule.AbstractCondition> conditions = whitelist.getConds();
        int index = 0;
        boolean idExist = false;
        // need to loop over all conditions to check if updated sql exists in whitelist
        for (int i = 0; i < conditions.size(); i++) {
            FavoriteRule.SQLCondition sqlCondition = (FavoriteRule.SQLCondition) conditions.get(i);
            // when updated sql already exists in white list
            if (updatedCondition.getSql().equals(sqlCondition.getSql()))
                return null;

            if (updatedCondition.getId().equals(sqlCondition.getId())) {
                idExist = true;
                index = i;
            }
        }

        if (!idExist)
            return null;

        conditions.set(index, updatedCondition);
        whitelist.setConds(conditions);
        crud.save(whitelist);

        return updatedCondition;
    }

    public void updateRule(List<FavoriteRule.AbstractCondition> conditions, boolean isEnabled, String ruleName) {
        FavoriteRule copy = crud.copyBySerialization(getByName(ruleName));
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

        favoriteRules.addAll(crud.getAll());

        logger.debug("Loaded " + favoriteRules.size() + " rules");
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

    public class RuleConditionExistException extends Exception {
        public RuleConditionExistException() {
            super();
        }
    }
}
