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

package io.kyligence.kap.metadata.query;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class FavoriteQueryManagerTest extends NLocalFileMetadataTestCase {
    private FavoriteQueryManager manager;

    private static final String PROJECT = "default";
    private static final String FAVORITE_QUERY = "bd3285c9-55e3-4f2d-a12c-742a8d631195";

    @Before
    public void setUp() {
        createTestMetadata();
        manager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetAll() {
        List<FavoriteQuery> favoriteQueries = manager.getAll();

        Assert.assertEquals(1, favoriteQueries.size());
        Assert.assertEquals("select * from test_kylin_fact limit 3", favoriteQueries.get(0).getSql());
    }

    @Test
    public void testFavor() throws IOException {
        String sql = "select * from test_country";
        FavoriteQuery favoriteQuery = new FavoriteQuery(sql);
        favoriteQuery.setLastQueryTime(0);
        favoriteQuery.setModelName("test_model_1");
        favoriteQuery.setSuccessRate(0.99f);
        favoriteQuery.setFrequency(100);
        favoriteQuery.setAverageDuration(5.0f);

        manager.favor(favoriteQuery);

        List<FavoriteQuery> favoriteQueries = manager.getAll();
        Assert.assertEquals(2, favoriteQueries.size());

        FavoriteQuery newAdded = manager.get(favoriteQuery.getUuid());
        Assert.assertEquals(sql, newAdded.getSql());
        Assert.assertEquals("test_model_1", newAdded.getModelName());
    }

    @Test
    public void testUnFavor() throws IOException {
        FavoriteQuery favoriteQuery = manager.get(FAVORITE_QUERY);
        manager.unFavor(favoriteQuery);

        Assert.assertEquals(0, manager.getAll().size());
    }

    @Test
    public void testUpdate() throws IOException {
        FavoriteQuery favoriteQuery = manager.get(FAVORITE_QUERY);
        favoriteQuery.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);
        manager.update(favoriteQuery);

        Assert.assertEquals(1, manager.getAll().size());
        Assert.assertEquals(FavoriteQueryStatusEnum.FULLY_ACCELERATED, manager.get(favoriteQuery.getUuid()).getStatus());
    }

    @Test
    public void testFindFavoriteBySql() {
        FavoriteQuery favoriteQuery = manager.findFavoriteQueryBySql("select * from test_kylin_fact limit 3");
        Assert.assertEquals(FAVORITE_QUERY, favoriteQuery.getUuid());
    }
}
