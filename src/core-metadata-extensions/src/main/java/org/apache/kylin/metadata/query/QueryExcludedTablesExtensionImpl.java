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

package org.apache.kylin.metadata.query;

import com.google.common.collect.Lists;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.query.QueryExcludedTablesExtension;

import java.util.List;
import java.util.Set;

public class QueryExcludedTablesExtensionImpl implements QueryExcludedTablesExtension {
    @Override
    public Set<String> getExcludedTables(KylinConfig kylinConfig, String projectName) {
        return FavoriteRuleManager.getInstance(kylinConfig, projectName).getExcludedTables();
    }

    @Override
    public void addExcludedTables(KylinConfig config, String projectName, String tableName, boolean isEnabled) {
        FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(config, projectName);
        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
        conds.add(new FavoriteRule.Condition(null, tableName));
        ruleManager.updateRule(conds, isEnabled, FavoriteRule.EXCLUDED_TABLES_RULE);
    }
}
