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

package io.kyligence.kap.rest.service;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.KylinEhCache;
import org.apache.kylin.rest.cache.RedisCache;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.TableMetaCacheResult;
import org.apache.kylin.rest.response.TableMetaCacheResultV2;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

import static org.apache.kylin.common.util.CheckUtil.checkCondition;
import static org.apache.kylin.rest.cache.RedisCache.checkRedisClient;


/**
 * query cache manager that
 * 1. holding query cache - <SQlRequest, SQLResponse> pairs
 * 2. holding schema cache - <UserName, Schema> pairs
 */
@Component("queryCacheManager")
public class QueryCacheManager {

    public enum Type {
        SUCCESS_QUERY_CACHE("StorageCache"), EXCEPTION_QUERY_CACHE("ExceptionQueryCache"), SCHEMA_CACHE("SchemaCache");

        public String rootCacheName;

        Type(String rootCacheName) {
            this.rootCacheName = rootCacheName;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger("query");

    private KylinCache kylinCache;

    @PostConstruct
    public void init() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isRedisEnabled()) {
            kylinCache = RedisCache.getInstance();
        } else {
            kylinCache = KylinEhCache.getInstance();
        }
        if (kylinCache instanceof RedisCache && checkRedisClient()) {
            logger.info("Redis cache connect successfully!");
        }
    }

    /**
     * check if the sqlResponse is qualified for caching
     * @param sqlResponse
     * @return
     */
    private boolean cacheable(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
        long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
        long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
        long responseSize = sqlResponse.getResultRowCount() > 0
                ? sqlResponse.getResultRowCount() * sqlResponse.getColumnMetas().size() : 0;
        return checkCondition(QueryUtil.isSelectStatement(sqlRequest.getSql()), "query is non-select")
                && checkCondition(!sqlResponse.isException(), "query has exception") //
                && checkCondition(!sqlResponse.isQueryPushDown() || kylinConfig.isPushdownQueryCacheEnabled(),
                        "query is executed with pushdown, or the cache for pushdown is disabled") //
                && checkCondition(
                        sqlResponse.getDuration() > durationThreshold
                                || sqlResponse.getTotalScanRows() > scanCountThreshold
                                || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                        "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                        sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanRows(),
                        scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                && checkCondition(responseSize < kylinConfig.getLargeQueryThreshold(),
                        "query response is too large: {} ({})", responseSize, kylinConfig.getLargeQueryThreshold());
    }

    public void doCacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        try {
            sqlResponse.readAllRows();
            kylinCache.put(Type.SUCCESS_QUERY_CACHE.rootCacheName, sqlRequest.getProject(),
                    sqlRequest.getCacheKey(), sqlResponse);
        } catch (Exception e) {
            logger.error("[query cache log] Error caching result of success query {}", sqlRequest.getSql(), e);
        }
    }

    public void cacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        if (cacheable(sqlRequest, sqlResponse)) {
            doCacheSuccessQuery(sqlRequest, sqlResponse);
        }
    }

    public void cacheFailedQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        try {
            kylinCache.put(Type.EXCEPTION_QUERY_CACHE.rootCacheName, sqlRequest.getProject(),
                    sqlRequest.getCacheKey(), sqlResponse);
        } catch (Exception e) {
            logger.error("[query cache log] Error caching result of failed query {}", sqlRequest.getSql(), e);
        }
    }

    public SQLResponse doSearchQuery(QueryCacheManager.Type type, SQLRequest sqlRequest) {
        Object response = kylinCache.get(type.rootCacheName, sqlRequest.getProject(),
                sqlRequest.getCacheKey());
        logger.info("[query cache log] The cache key is: {}", sqlRequest.getCacheKey());
        if (response == null) {
            return null;
        }
        return (SQLResponse) response;
    }

    private SQLResponse searchSuccessCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.SUCCESS_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            logger.info("[query cache log] No success cache searched");
            return null;
        }

        // check signature for success query resp in case the datasource is changed
        if (QueryCacheSignatureUtil.checkCacheExpired(cached, sqlRequest.getProject())) {
            logger.info("[query cache log] cache has expired, cache key is {}", sqlRequest.getCacheKey());
            clearQueryCache(sqlRequest);
            return null;
        }

        cached.setStorageCacheUsed(true);
        QueryContext.current().getQueryTagInfo().setStorageCacheUsed(true);
        String cacheType = KylinConfig.getInstanceFromEnv().isRedisEnabled() ? "Redis" : "Ehcache";
        cached.setStorageCacheType(cacheType);
        QueryContext.current().getQueryTagInfo().setStorageCacheType(cacheType);
        return cached;
    }

    private SQLResponse searchFailedCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.EXCEPTION_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            logger.info("[query cache log] No failed cache searched");
            return null;
        }
        cached.setHitExceptionCache(true);
        QueryContext.current().getQueryTagInfo().setHitExceptionCache(true);
        return cached;
    }

    /**
     * search query in both success and failed query cache
     * for success cache, the cached result will be returned only if it passes the expiration check
     * @param sqlRequest
     * @return
     */
    public SQLResponse searchQuery(SQLRequest sqlRequest) {
        SQLResponse cached = searchSuccessCache(sqlRequest);
        if (cached != null) {
            return cached;
        }
        return searchFailedCache(sqlRequest);
    }

    @SuppressWarnings("unchecked")
    public List<TableMeta> getSchemaCache(String project, String userName) {
        TableMetaCacheResult cacheResult = doGetSchemaCache(project, userName);
        if (cacheResult == null) {
            return null;
        }
        if (QueryCacheSignatureUtil.checkCacheExpired(cacheResult.getTables(), cacheResult.getSignature(), project,
                null)) {
            logger.info("[schema cache log] cache has expired, cache key is {}", userName);
            clearSchemaCache(project, userName);
            return null;
        }
        return cacheResult.getTableMetaList();
    }

    public TableMetaCacheResult doGetSchemaCache(String project, String userName) {
        Object metaList = kylinCache.get(Type.SCHEMA_CACHE.rootCacheName, project, userName);
        if (metaList == null) {
            return null;
        }
        return (TableMetaCacheResult) metaList;
    }

    public void putSchemaCache(String project, String userName, TableMetaCacheResult schemas) {
        kylinCache.put(Type.SCHEMA_CACHE.rootCacheName, project, userName, schemas);
    }

    @SuppressWarnings("unchecked")
    public List<TableMetaWithType> getSchemaV2Cache(String project, String modelName, String userName) {
        TableMetaCacheResultV2 cacheResult = doGetSchemaCacheV2(project, modelName, userName);
        if (cacheResult == null) {
            return null;
        }
        if (QueryCacheSignatureUtil.checkCacheExpired(cacheResult.getTables(), cacheResult.getSignature(), project,
                modelName)) {
            logger.info("[schema cache log] cache has expired, cache key is {}", userName);
            clearSchemaCacheV2(project, userName);
            return null;
        }

        return cacheResult.getTableMetaList();
    }

    public TableMetaCacheResultV2 doGetSchemaCacheV2(String project, String modelName, String userName) {
        String cacheKey = userName + "v2";
        if (modelName != null) {
            cacheKey = cacheKey + modelName;
        }
        Object metaList = kylinCache.get(Type.SCHEMA_CACHE.rootCacheName, project, cacheKey);
        if (metaList == null) {
            return null;
        }
        return (TableMetaCacheResultV2) metaList;
    }

    public void putSchemaV2Cache(String project, String modelName, String userName, TableMetaCacheResultV2 schemas) {
        String cacheKey = userName + "v2";
        if (modelName != null) {
            cacheKey = cacheKey + modelName;
        }
        kylinCache.put(Type.SCHEMA_CACHE.rootCacheName, project, cacheKey, schemas);
    }

    public void clearSchemaCacheV2(String project, String userName) {
        kylinCache.remove(Type.SCHEMA_CACHE.rootCacheName, project, userName + "v2");
    }

    public void clearSchemaCache(String project, String userName) {
        kylinCache.remove(Type.SCHEMA_CACHE.rootCacheName, project, userName);
    }

    public void clearSchemaCache(String project) {
        kylinCache.clearByType(Type.SCHEMA_CACHE.rootCacheName, project);
    }

    public void clearQueryCache(SQLRequest request) {
        kylinCache.remove(Type.SUCCESS_QUERY_CACHE.rootCacheName,
                request.getProject(), request.getCacheKey());
        kylinCache.remove(Type.EXCEPTION_QUERY_CACHE.rootCacheName,
                request.getProject(), request.getCacheKey());
    }

    public void clearProjectCache(String project) {
        if (project == null) {
            logger.debug("[query cache log] clear query cache for all projects.");
            kylinCache.clearAll();
        } else {
            logger.debug("[query cache log] clear query cache for {}", project);
            kylinCache.clearByType(Type.SUCCESS_QUERY_CACHE.rootCacheName, project);
            kylinCache.clearByType(Type.EXCEPTION_QUERY_CACHE.rootCacheName, project);
            kylinCache.clearByType(Type.SCHEMA_CACHE.rootCacheName, project);
        }
    }

    public void recoverCache() {
        boolean isRedisEnabled = KylinConfig.getInstanceFromEnv().isRedisEnabled();
        if (isRedisEnabled) {
            RedisCache.recoverInstance();
            logger.info("[query cache log] Redis client recover successfully.");
        }
    }
}
