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

package io.kyligence.kap.rest.cache;

import static org.apache.kylin.common.util.CheckUtil.checkCondition;

import java.util.List;
import java.util.Locale;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

/**
 * query cache manager that
 * 1. holding query cache - <SQlRequest, SQLResponse> pairs
 * 2. holding scheam cache - <UserName, Schema> pairs
 */
@Component("queryCacheManager")
public class QueryCacheManager {

    enum Type {
        SUCCESS_QUERY_CACHE("StorageCache"), EXCEPTION_QUERY_CACHE("ExceptionQueryCache"), SCHEMA_CACHE("SchemaCache");

        private String rootCacheName;

        Type(String rootCacheName) {
            this.rootCacheName = rootCacheName;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(QueryCacheManager.class);

    @Autowired
    private CacheManager cacheManager;

    @PostConstruct
    public void init() {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    private Ehcache getProjectCache(QueryCacheManager.Type type, String project) {
        final String projectCacheName = String.format(Locale.ROOT, "%s-%s", type.rootCacheName, project);
        // make sure project cache exists
        // cacheManager maintains a concurrentHashMap for caches, so that we can simply call its addCacheIfAbsent method
        if (cacheManager.getEhcache(projectCacheName) == null) {
            CacheConfiguration cacheConfiguration = cacheManager.getEhcache(type.rootCacheName).getCacheConfiguration()
                    .clone();
            cacheConfiguration.setName(projectCacheName);
            cacheManager.addCacheIfAbsent(new Cache(cacheConfiguration));
        }
        return cacheManager.getEhcache(projectCacheName);
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
        long responseSize = sqlResponse.getResults().isEmpty() ? 0
                : sqlResponse.getResults().get(0).size() * sqlResponse.getResults().size();
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

    void doCacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        try {
            getProjectCache(Type.SUCCESS_QUERY_CACHE, sqlRequest.getProject())
                    .put(new Element(sqlRequest.getCacheKey(), sqlResponse));
        } catch (Exception e) {
            logger.error("Error caching result of success query {}", sqlRequest.getSql(), e);
        }
    }

    public void cacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (cacheable(sqlRequest, sqlResponse)) {
            doCacheSuccessQuery(sqlRequest, sqlResponse);
        }
    }

    public void cacheFailedQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        try {
            getProjectCache(Type.EXCEPTION_QUERY_CACHE, sqlRequest.getProject())
                    .put(new Element(sqlRequest.getCacheKey(), sqlResponse));
        } catch (Exception e) {
            logger.error("Error caching result of failed query {}", sqlRequest.getSql(), e);
        }
    }

    SQLResponse doSearchQuery(QueryCacheManager.Type type, SQLRequest sqlRequest) {
        Ehcache successCache = getProjectCache(type, sqlRequest.getProject());
        Element element = successCache.get(sqlRequest.getCacheKey());
        if (element == null) {
            return null;
        }
        return (SQLResponse) element.getObjectValue();
    }

    private SQLResponse searchSuccessCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.SUCCESS_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            return null;
        }

        // check signature for success query resp in case the datasource is changed
        if (QueryCacheSignatureUtil.checkCacheExpired(cached, sqlRequest.getProject())) {
            clearQueryCache(sqlRequest);
            return null;
        }

        cached.setStorageCacheUsed(true);
        return cached;
    }

    private SQLResponse searchFailedCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.EXCEPTION_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            return null;
        }
        cached.setHitExceptionCache(true);
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
        Element element = getProjectCache(Type.SCHEMA_CACHE, project).get(userName);
        if (element == null) {
            return null;
        }
        return (List<TableMeta>) element.getObjectValue();
    }

    public void putSchemaCache(String project, String userName, List<TableMeta> schemas) {
        getProjectCache(Type.SCHEMA_CACHE, project).put(new Element(userName, schemas));
    }

    @SuppressWarnings("unchecked")
    public List<TableMetaWithType> getSchemaV2Cache(String project, String userName) {
        Element element = getProjectCache(Type.SCHEMA_CACHE, project).get(userName + "v2");
        if (element == null) {
            return null;
        }
        return (List<TableMetaWithType>) element.getObjectValue();
    }

    public void putSchemaV2Cache(String project, String userName, List<TableMetaWithType> schemas) {
        getProjectCache(Type.SCHEMA_CACHE, project).put(new Element(userName + "v2", schemas));
    }

    public void clearSchemaCache(String project) {
        getProjectCache(Type.SCHEMA_CACHE, project).removeAll();
    }

    public void clearQueryCache(SQLRequest request) {
        getProjectCache(Type.SUCCESS_QUERY_CACHE, request.getProject()).remove(request.getCacheKey());
        getProjectCache(Type.EXCEPTION_QUERY_CACHE, request.getProject()).remove(request.getCacheKey());
    }

    public void clearProjectCache(String project) {
        logger.debug("clear query cache for {}", project);
        getProjectCache(Type.SUCCESS_QUERY_CACHE, project).removeAll();
        getProjectCache(Type.EXCEPTION_QUERY_CACHE, project).removeAll();
        getProjectCache(Type.SCHEMA_CACHE, project).removeAll();
    }
}
