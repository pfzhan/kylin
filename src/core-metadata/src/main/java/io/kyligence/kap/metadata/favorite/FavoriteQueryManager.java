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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.project.NProjectManager;

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
                entity.initAfterReload(kylinConfig, project);
                return entity;
            }
        };

        crud.reloadAll();
        reloadSqlPatternMap();
    }

    public void reloadSqlPatternMap() {
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
        Set<String> blacklistSqls = FavoriteRuleManager.getInstance(kylinConfig, project).getBlacklistSqls();

        favoriteQueries.forEach(favoriteQuery -> {
            if (blacklistSqls.contains(favoriteQuery.getSqlPattern()))
                return;

            if (contains(favoriteQuery.getSqlPattern()))
                return;

            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), crud.save(favoriteQuery));
        });
    }

    public void createWithoutCheck(final Set<FavoriteQuery> favoriteQueries) {
        favoriteQueries.forEach(
                favoriteQuery -> favoriteQueryMap.put(favoriteQuery.getSqlPattern(), crud.save(favoriteQuery)));
    }

    public FavoriteQuery getByUuid(String uuid) {
        return crud.get(uuid);
    }

    public void delete(FavoriteQuery favoriteQuery) {
        crud.delete(favoriteQuery);
        favoriteQueryMap.remove(favoriteQuery.getSqlPattern());
    }

    public void delete(String uuid) {
        delete(getByUuid(uuid));
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

    public void rollBackToInitialStatus(String sqlPattern, String comment) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return;

        FavoriteQuery copyForWrite = crud.copyForWrite(cached);
        copyForWrite.updateStatus(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, comment);
        copyForWrite.setRealizations(Lists.newArrayList());
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

    public List<String> getAcceleratedSqlPattern() {
        List<FavoriteQuery> favoriteQueries = crud.listAll().stream()
                .filter(input -> input.getStatus().equals(FavoriteQueryStatusEnum.ACCELERATED))
                .collect(Collectors.toList());
        return favoriteQueries.stream().map(FavoriteQuery::getSqlPattern).collect(Collectors.toList());
    }

    public List<String> getAccelerableSqlPattern() {
        List<FavoriteQuery> favoriteQueries = crud.listAll().stream()
                .filter(input -> input.getStatus() == FavoriteQueryStatusEnum.TO_BE_ACCELERATED
                        || input.getStatus() == FavoriteQueryStatusEnum.PENDING)
                .collect(Collectors.toList());
        return favoriteQueries.stream().map(FavoriteQuery::getSqlPattern).collect(Collectors.toList());
    }

    public List<String> getToBeAcceleratedSqlPattern() {
        List<FavoriteQuery> favoriteQueries = crud.listAll().stream()
                .filter(input -> input.getStatus() == FavoriteQueryStatusEnum.TO_BE_ACCELERATED)
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

    public List<FavoriteQueryRealization> getFQRByConditions(String modelId, Long cuboidLayoutId) {
        List<FavoriteQueryRealization> realizations = Lists.newArrayList();
        List<FavoriteQuery> favoriteQueries = crud.listAll();
        favoriteQueries.stream().map(FavoriteQuery::getRealizations).flatMap(List::stream)
                .filter(fqr -> fqr.getModelId().equals(modelId))
                .filter(fqr -> cuboidLayoutId == null || fqr.getLayoutId() == cuboidLayoutId)
                .forEach(realizations::add);
        return realizations;
    }

    public List<FavoriteQuery> getLowFrequencyFQs() {
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        long favoriteQueryFrequencyTimeWindow = projectInstance.getConfig().getFavoriteQueryFrequencyTimeWindow();
        return getAll().stream()
                .filter(fq -> System.currentTimeMillis() - fq.getCreateTime() >= favoriteQueryFrequencyTimeWindow)
                .filter(fq -> fq.getFrequencyMap().isLowFrequency(project)).collect(Collectors.toList());
    }
}
