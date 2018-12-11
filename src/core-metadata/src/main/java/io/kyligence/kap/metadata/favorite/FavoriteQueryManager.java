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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FavoriteQueryManager implements IKeepNames {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryManager.class);

    private final String project;
    private final KylinConfig kylinConfig;

    private CachedCrudAssist<FavoriteQuery> crud;

    private Map<String, FavoriteQuery> favoriteQueryMap = Maps.newConcurrentMap();

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
        initializeSqlPatternMap();
    }

    private void initializeSqlPatternMap() {
        List<FavoriteQuery> favoriteQueries = crud.listAll();
        for (FavoriteQuery favoriteQuery : favoriteQueries) {
            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), favoriteQuery);
        }

        logger.info("Initialized {} favorite queries", favoriteQueryMap.size());
    }

    public boolean contains(String sqlPattern) {
        return favoriteQueryMap.containsKey(sqlPattern);
    }

    public void create(final List<FavoriteQuery> favoriteQueries) {
        favoriteQueries.forEach(favoriteQuery -> {
            crud.save(favoriteQuery);
            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), favoriteQuery);
        });
    }

    public void delete(String sqlPattern) {
        FavoriteQuery fq = get(sqlPattern);
        if (fq != null) {
            crud.delete(fq);
            favoriteQueryMap.remove(sqlPattern);
        }
    }

    public void updateStatistics(final List<FavoriteQuery> favoriteQueries) {
        favoriteQueries.forEach(favoriteQuery -> {
            FavoriteQuery cached = get(favoriteQuery.getSqlPattern());
            if (cached == null)
                return;
            FavoriteQuery copyForWrite = crud.copyForWrite(cached);
            copyForWrite.update(favoriteQuery);
            crud.save(copyForWrite);
            favoriteQueryMap.put(copyForWrite.getSqlPattern(), copyForWrite);
        });
    }

    public FavoriteQuery resetRealizations(String sqlPattern, final List<FavoriteQueryRealization> realizations) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return null;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);
        copyForWrite.setRealizations(realizations);

        crud.save(copyForWrite);
        favoriteQueryMap.put(sqlPattern, copyForWrite);
        return copyForWrite;
    }

    public void removeRealizations(String sqlPattern) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);

        copyForWrite.setRealizations(Lists.newArrayList());
        crud.save(copyForWrite);
        favoriteQueryMap.put(sqlPattern, copyForWrite);
    }

    public void updateStatus(String sqlPattern, FavoriteQueryStatusEnum status, String comment) {
        FavoriteQuery cached = get(sqlPattern);
        if (cached == null)
            return;
        FavoriteQuery copyForWrite = crud.copyForWrite(cached);
        copyForWrite.updateStatus(status, comment);
        crud.save(copyForWrite);
        favoriteQueryMap.put(sqlPattern, copyForWrite);
    }

    public List<FavoriteQuery> getAll() {
        List<FavoriteQuery> allFqs = crud.listAll();
        // sort by last query time
        allFqs.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        return allFqs;
    }

    public List<String> getUnAcceleratedSqlPattern() {
        List<FavoriteQuery> favoriteQueries = crud.listAll().stream()
                .filter(input -> input.getStatus().equals(FavoriteQueryStatusEnum.WAITING))
                .collect(Collectors.toList());
        return favoriteQueries.stream().map(FavoriteQuery::getSqlPattern).collect(Collectors.toList());
    }

    public FavoriteQuery get(String sqlPattern) {
        return favoriteQueryMap.get(sqlPattern);
    }

    public List<FavoriteQueryRealization> getRealizationsByConditions(String modelId, String cubePlanName,
            Long cuboidLayoutId) {
        List<FavoriteQueryRealization> realizations = Lists.newArrayList();
        List<FavoriteQuery> favoriteQueries = crud.listAll();
        favoriteQueries.forEach(fq -> {
            List<FavoriteQueryRealization> fqRealizations = fq.getRealizations();
            for (FavoriteQueryRealization fqr : fqRealizations) {
                if (StringUtils.isNotBlank(modelId) && !modelId.equals(fqr.getModelId()))
                    continue;

                if (StringUtils.isNotBlank(cubePlanName) && !cubePlanName.equals(fqr.getCubePlanId()))
                    continue;

                if (cuboidLayoutId != null && cuboidLayoutId != fqr.getCuboidLayoutId())
                    continue;

                realizations.add(fqr);
            }
        });

        return realizations;
    }
}
