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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.obf.IKeepNames;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FavoriteQueryManager implements IKeepNames {

    private final String project;
    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteQuery> crud;

    private Map<String, FavoriteQuery> favoriteQueryMap;

    public static FavoriteQueryManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteQueryManager.class);
    }

    // called by reflection
    static FavoriteQueryManager newInstance(KylinConfig config, String project) {
        return new FavoriteQueryManager(config, project);
    }

    private FavoriteQueryManager(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() {
        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRootPath = "/" + this.project + ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FavoriteQuery>(store, resourceRootPath, FavoriteQuery.class) {
            @Override
            protected FavoriteQuery initEntityAfterReload(FavoriteQuery entity, String resourceName) {
                return entity;
            }
        };

        crud.reloadAll();
        reloadSqlPatternMap();
    }

    private void reloadSqlPatternMap() {
        favoriteQueryMap = Maps.newConcurrentMap();
        List<FavoriteQuery> favoriteQueries = crud.listAll();
        for (FavoriteQuery favoriteQuery : favoriteQueries) {
            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), favoriteQuery);
        }
    }

    public boolean contains(String sqlPattern) {
        if (favoriteQueryMap == null)
            reloadSqlPatternMap();
        return favoriteQueryMap.containsKey(sqlPattern);
    }

    public void create(final Set<FavoriteQuery> favoriteQueries) {
        favoriteQueries.forEach(favoriteQuery -> {
            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), crud.save(favoriteQuery));
        });
    }

    public void delete(String uuid) {
        FavoriteQuery fq = crud.get(uuid);
        if (fq != null) {
            String channel = fq.getChannel();
            String sqlPattern = fq.getSqlPattern();
            // put to blacklist
            if (channel.equals(FavoriteQuery.CHANNEL_FROM_RULE)) {
                FavoriteRule.SQLCondition sqlCondition = new FavoriteRule.SQLCondition(sqlPattern);
                FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(kylinConfig, project);
                ruleManager.appendSqlPatternToBlacklist(sqlCondition);
            }
            crud.delete(fq);
            favoriteQueryMap.remove(fq.getSqlPattern());
        }
    }

    public void updateStatistics(final List<FavoriteQuery> favoriteQueries) {
        favoriteQueries.forEach(favoriteQuery -> {
            FavoriteQuery cached = get(favoriteQuery.getSqlPattern());
            if (cached == null)
                return;
            FavoriteQuery copyForWrite = crud.copyForWrite(cached);
            copyForWrite.update(favoriteQuery);
            favoriteQueryMap.put(copyForWrite.getSqlPattern(), crud.save(copyForWrite));
        });
    }

    public FavoriteQuery resetRealizations(String sqlPattern, final List<FavoriteQueryRealization> realizations) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return null;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);
        copyForWrite.setRealizations(realizations);

        favoriteQueryMap.put(sqlPattern, crud.save(copyForWrite));
        return copyForWrite;
    }

    public void removeRealizations(String sqlPattern) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);

        copyForWrite.setRealizations(Lists.newArrayList());
        favoriteQueryMap.put(sqlPattern, crud.save(copyForWrite));
    }

    public void updateStatus(String sqlPattern, FavoriteQueryStatusEnum status, String comment) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);
        copyForWrite.updateStatus(status, comment);
        favoriteQueryMap.put(sqlPattern, crud.save(copyForWrite));
    }

    // for ut
    public Map<String, FavoriteQuery> getFavoriteQueryMap() {
        return favoriteQueryMap;
    }

    public void updateFavoriteQueryMap(FavoriteQuery favoriteQuery) {
        if (favoriteQueryMap == null)
            reloadSqlPatternMap();
        favoriteQueryMap.put(favoriteQuery.getSqlPattern(), favoriteQuery);
    }

    public List<FavoriteQuery> getAll() {
        return crud.listAll();
    }

    public List<String> getUnAcceleratedSqlPattern() {
        List<FavoriteQuery> favoriteQueries = crud.listAll().stream()
                .filter(input -> input.getStatus().equals(FavoriteQueryStatusEnum.WAITING))
                .collect(Collectors.toList());
        return favoriteQueries.stream().map(FavoriteQuery::getSqlPattern).collect(Collectors.toList());
    }

    public FavoriteQuery get(String sqlPattern) {
        if (favoriteQueryMap == null)
            reloadSqlPatternMap();
        return favoriteQueryMap.get(sqlPattern);
    }

    // when delete favorite query
    public void clearFavoriteQueryMap() {
        favoriteQueryMap = null;
    }

    public List<FavoriteQueryRealization> getRealizationsByConditions(String modelId,
                                                                      Long cuboidLayoutId) {
        List<FavoriteQueryRealization> realizations = Lists.newArrayList();
        List<FavoriteQuery> favoriteQueries = crud.listAll();
        favoriteQueries.forEach(fq -> {
            List<FavoriteQueryRealization> fqRealizations = fq.getRealizations();
            for (FavoriteQueryRealization fqr : fqRealizations) {
                if (StringUtils.isNotBlank(modelId) && !modelId.equals(fqr.getModelId()))
                    continue;

                if (cuboidLayoutId != null && cuboidLayoutId != fqr.getLayoutId())
                    continue;

                realizations.add(fqr);
            }
        });

        return realizations;
    }
}
