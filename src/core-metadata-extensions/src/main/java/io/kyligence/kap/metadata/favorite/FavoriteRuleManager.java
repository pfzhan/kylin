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

import static io.kyligence.kap.metadata.favorite.FavoriteRule.FAVORITE_RULE_NAMES;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import lombok.val;

public class FavoriteRuleManager {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteRule> crud;

    public static FavoriteRuleManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteRuleManager.class);
    }

    // called by reflection
    static FavoriteRuleManager newInstance(KylinConfig config, String project) {
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
        final String resourceRoot = "/" + this.project + ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FavoriteRule>(store, resourceRoot, FavoriteRule.class) {
            @Override
            protected FavoriteRule initEntityAfterReload(FavoriteRule entity, String resourceName) {
                return entity;
            }
        };

        crud.setCheckCopyOnWrite(true);
        crud.reloadAll();
    }

    public List<FavoriteRule> getAll() {
        List<FavoriteRule> favoriteRules = Lists.newArrayList();

        favoriteRules.addAll(crud.listAll());
        return favoriteRules;
    }

    public List<FavoriteRule> listAll() {
        return FAVORITE_RULE_NAMES.stream().map(this::getOrDefaultByName).collect(Collectors.toList());
    }

    public FavoriteRule getByName(String name) {
        for (FavoriteRule rule : getAll()) {
            if (rule.getName().equals(name))
                return rule;
        }
        return null;
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
        FavoriteRule copy = crud.copyForWrite(getOrDefaultByName(ruleName));

        copy.setEnabled(isEnabled);

        List<FavoriteRule.AbstractCondition> newConditions = Lists.newArrayList();
        if (!conditions.isEmpty()) {
            newConditions.addAll(conditions);
        }

        copy.setConds(newConditions);
        crud.save(copy);
    }

    public void delete(FavoriteRule favoriteRule) {
        crud.delete(favoriteRule);
    }

    @VisibleForTesting
    public void createRule(final FavoriteRule rule) {
        if (getByName(rule.getName()) != null)
            return;

        crud.save(rule);
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
