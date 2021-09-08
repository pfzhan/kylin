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

import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.RedisCache;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import redis.embedded.RedisServer;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class QueryRedisCacheTest extends LocalFileMetadataTestCase {

    private static RedisServer redisServer = null;

    static {
        try {
            redisServer = new RedisServer(6379);
            redisServer.start();
        } catch (Exception e) {

        }
    }

    @Spy
    private KylinCache redisCache = Mockito.spy(RedisCache.getInstance());

    @InjectMocks
    private QueryCacheManager queryCacheManager;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        overwriteSystemProp("kylin.cache.redis.enabled", "true");
        createTestMetadata();
        try {
            redisServer = new RedisServer(6379);
            redisServer.start();
        } catch (Exception e) {

        }
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testProjectRedisCacheQuery() {
        if (redisServer != null) {
            overwriteSystemProp("kylin.cache.redis.enabled", "true");
            final String project = "default";
            final SQLRequest req1 = new SQLRequest();
            req1.setProject(project);
            req1.setSql("select a from b");
            final SQLResponse resp1 = new SQLResponse();
            List<List<String>> results = new ArrayList<>();
            resp1.setResults(results);
            resp1.setResultRowCount(1);
            // Single Node mode
            testHelper(req1, resp1, project);
            // TODO Cluster mode
        }
    }

    private void testHelper(SQLRequest req1, SQLResponse resp1, String project) {
        queryCacheManager.cacheSuccessQuery(req1, resp1);

        queryCacheManager.doCacheSuccessQuery(req1, resp1);
        Assert.assertEquals(resp1.getResultRowCount(), queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1).getResultRowCount());
        Assert.assertNull(queryCacheManager.searchQuery(req1));
        queryCacheManager.clearQueryCache(req1);
        Assert.assertNull(queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));

        queryCacheManager.cacheFailedQuery(req1, resp1);
        Assert.assertEquals(resp1.getResultRowCount(), queryCacheManager.searchQuery(req1).getResultRowCount());
        queryCacheManager.clearProjectCache(project);
        Assert.assertNull(queryCacheManager.searchQuery(req1));

        queryCacheManager.recoverCache();
    }
}
