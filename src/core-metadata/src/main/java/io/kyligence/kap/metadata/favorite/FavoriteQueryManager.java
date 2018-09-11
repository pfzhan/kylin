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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import io.kyligence.kap.common.obf.IKeepNames;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FavoriteQueryManager implements IKeepNames {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryManager.class);

    private final String project;

    private final KylinConfig kylinConfig;

    private CaseInsensitiveStringCache<FavoriteQuery> cache;
    private CachedCrudAssist<FavoriteQuery> crud;
    private AutoReadWriteLock autoLock = new AutoReadWriteLock();

    public static FavoriteQueryManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteQueryManager.class);
    }

    // called by reflection
    static FavoriteQueryManager newInstance(KylinConfig config, String project) throws IOException {
        return new FavoriteQueryManager(config, project);
    }

    private FavoriteQueryManager(KylinConfig kylinConfig, String project) throws IOException {
        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() throws IOException {
        this.cache = new CaseInsensitiveStringCache<>(this.kylinConfig, this.project, ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT);

        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRootPath = "/" + this.project + "/" + ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FavoriteQuery>(store, resourceRootPath, FavoriteQuery.class, cache) {
            @Override
            protected FavoriteQuery initEntityAfterReload(FavoriteQuery entity, String resourceName) {
                return entity;
            }
        };

        crud.reloadAll();

        Broadcaster.getInstance(this.kylinConfig).registerListener(new Broadcaster.Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event,
                    String favoriteQuery) throws IOException {
                try (AutoReadWriteLock.AutoLock lock = FavoriteQueryManager.this.autoLock.lockForWrite()) {
                    if (Broadcaster.Event.DROP == event) {
                        cache.removeLocal(favoriteQuery);
                    } else if (Broadcaster.Event.UPDATE == event) {
                        crud.reloadQuietly(favoriteQuery);
                    }
                }
            }
        }, this.project, ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT);
    }

    public FavoriteQuery favor(final FavoriteQuery favoriteQuery) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            return crud.save(favoriteQuery);
        }
    }

    public void unFavor(final FavoriteQuery favoriteQuery) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            crud.delete(favoriteQuery);
        }
    }

    public FavoriteQuery get(final String name) {
        try (AutoReadWriteLock.AutoLock lock = autoLock.lockForRead()) {
            return cache.get(name);
        }
    }

    public void update(final FavoriteQuery favoriteQuery) throws IOException {
        try (final AutoReadWriteLock.AutoLock lock = this.autoLock.lockForWrite()) {
            crud.save(favoriteQuery);
        }
    }

    public List<FavoriteQuery> getAll() {
        List<FavoriteQuery> favoriteQueries = new ArrayList<>();

        try (AutoReadWriteLock.AutoLock lock = this.autoLock.lockForRead()) {
            for (Map.Entry<String, FavoriteQuery> entry : cache.getMap().entrySet()) {
                favoriteQueries.add(entry.getValue());
            }
        }

        return favoriteQueries;
    }

    public FavoriteQuery findFavoriteQueryBySql(final String sql) {
        Predicate<FavoriteQuery> predicate = new Predicate<FavoriteQuery>() {
            @Override
            public boolean apply(@Nullable FavoriteQuery favoriteQuery) {
                return favoriteQuery.getSql().equals(sql);
            }
        };
        return Iterators.tryFind(getAll().iterator(), predicate).orNull();
    }

}
