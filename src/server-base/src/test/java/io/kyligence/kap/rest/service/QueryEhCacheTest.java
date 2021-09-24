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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.KylinEhCache;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.TableMetaCacheResult;
import org.apache.kylin.rest.response.TableMetaCacheResultV2;
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

import net.sf.ehcache.CacheManager;

@RunWith(MockitoJUnitRunner.class)
public class QueryEhCacheTest extends LocalFileMetadataTestCase {

    @Spy
    private CacheManager cacheManager = Mockito
            .spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache.xml")));

    @Spy
    private KylinCache kylinEhCache = Mockito.spy(KylinEhCache.getInstance());

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
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testProjectCacheQuery() {
        final String project = "default";
        final SQLRequest req1 = new SQLRequest();
        req1.setProject(project);
        req1.setSql("select a from b");
        final SQLResponse resp1 = new SQLResponse();
        List<List<String>> results = new ArrayList<>();
        resp1.setResults(results);

        queryCacheManager.cacheSuccessQuery(req1, resp1);

        queryCacheManager.doCacheSuccessQuery(req1, resp1);
        Assert.assertEquals(resp1, queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));
        Assert.assertNull(queryCacheManager.searchQuery(req1));
        queryCacheManager.clearQueryCache(req1);
        Assert.assertNull(queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));

        queryCacheManager.cacheFailedQuery(req1, resp1);
        Assert.assertEquals(resp1, queryCacheManager.searchQuery(req1));
        queryCacheManager.clearProjectCache(project);
        Assert.assertNull(queryCacheManager.searchQuery(req1));

        String username = "username";
        TableMetaCacheResult metaList = new TableMetaCacheResult();
        queryCacheManager.putSchemaCache(project, username, metaList);
        Assert.assertEquals(metaList, queryCacheManager.doGetSchemaCache(project, username));

        queryCacheManager.clearSchemaCache(project);
        Assert.assertNull(queryCacheManager.doGetSchemaCache(project, username));

        TableMetaCacheResultV2 metaWithTypeList = new TableMetaCacheResultV2();
        queryCacheManager.putSchemaV2Cache(project, null, username, metaWithTypeList);
        Assert.assertEquals(metaWithTypeList, queryCacheManager.doGetSchemaCacheV2(project, null, username));
    }

}
