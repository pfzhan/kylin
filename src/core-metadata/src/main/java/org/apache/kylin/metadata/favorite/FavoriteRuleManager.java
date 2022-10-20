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

package org.apache.kylin.metadata.favorite;

import static org.apache.kylin.metadata.favorite.FavoriteRule.FAVORITE_RULE_NAMES;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import lombok.val;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class FavoriteRuleManager {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleManager.class);

    private final FavoriteRuleStore favoriteRuleStore;
    private final String project;

    public static FavoriteRuleManager getInstance(String project) {
        return Singletons.getInstance(project, FavoriteRuleManager.class);
    }

    private FavoriteRuleManager(String project) throws Exception {
        this.project = project;
        this.favoriteRuleStore = new FavoriteRuleStore(KylinConfig.getInstanceFromEnv());
    }

    public DataSourceTransactionManager getTransactionManager() {
        return favoriteRuleStore.getTransactionManager();
    }

    public List<FavoriteRule> getAll() {
        return favoriteRuleStore.queryByProject(project);
    }

    public List<FavoriteRule> listAll() {
        return FAVORITE_RULE_NAMES.stream().map(this::getOrDefaultByName).collect(Collectors.toList());
    }

    public FavoriteRule getByName(String name) {
        return favoriteRuleStore.queryByName(project, name);
    }

    public String getValue(String ruleName) {
        val rule = getOrDefaultByName(ruleName);
        FavoriteRule.Condition condition = (FavoriteRule.Condition) rule.getConds().get(0);
        return condition.getRightThreshold();
    }

    public FavoriteRule getOrDefaultByName(String ruleName) {
        return FavoriteRule.getDefaultRuleIfNull(getByName(ruleName), ruleName);
    }

    public void resetRule() {
        FavoriteRule.getAllDefaultRule().forEach(this::updateRule);
    }

    public void updateRule(FavoriteRule rule) {
        updateRule(rule.getConds(), rule.isEnabled(), rule.getName());
    }

    public void updateRule(List<FavoriteRule.AbstractCondition> conditions, boolean isEnabled, String ruleName) {
        JdbcUtil.withTxAndRetry(getTransactionManager(), () -> {
            FavoriteRule rule = getOrDefaultByName(ruleName);
            rule.setEnabled(isEnabled);
            List<FavoriteRule.AbstractCondition> newConditions = Lists.newArrayList();
            if (!conditions.isEmpty()) {
                newConditions.addAll(conditions);
            }
            rule.setConds(newConditions);
            saveOrUpdate(rule);
            return null;
        });
    }

    private void saveOrUpdate(FavoriteRule rule) {
        if (rule.getId() == 0) {
            rule.setProject(project);
            rule.setCreateTime(System.currentTimeMillis());
            rule.setUpdateTime(rule.getCreateTime());
            favoriteRuleStore.save(rule);
        } else {
            rule.setUpdateTime(System.currentTimeMillis());
            favoriteRuleStore.update(rule);
        }
    }

    public void delete(FavoriteRule favoriteRule) {
        favoriteRuleStore.deleteByName(project, favoriteRule.getName());
    }

    @VisibleForTesting
    public void createRule(final FavoriteRule rule) {
        if (getByName(rule.getName()) != null)
            return;
        saveOrUpdate(rule);
    }

    @VisibleForTesting
    public List<FavoriteRule> getAllEnabled() {
        List<FavoriteRule> enabledRules = Lists.newArrayList();

        for (FavoriteRule rule : getAll()) {
            if (rule.isEnabled()) {
                enabledRules.add(rule);
            }
        }

        return enabledRules;
    }

    public Set<String> getExcludedTables() {
        FavoriteRule favoriteRule = getOrDefaultByName(FavoriteRule.EXCLUDED_TABLES_RULE);
        if (!favoriteRule.isEnabled()) {
            return Sets.newHashSet();
        }
        FavoriteRule.Condition condition = (FavoriteRule.Condition) favoriteRule.getConds().get(0);
        return Arrays.stream(condition.getRightThreshold().split(",")) //
                .map(table -> table.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
    }
}
