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
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class FavoriteQueryJDBCDaoTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    FavoriteQueryJDBCDao favoriteQueryDao;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        favoriteQueryDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());
    }

    @After
    public void cleanUp() {
        favoriteQueryDao.dropTable();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    private List<FavoriteQuery> testDataWithoutInitialValue() {

        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1", "sql1".hashCode(), PROJECT);
        favoriteQuery1.setLastQueryTime(System.currentTimeMillis() + 1);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2", "sql2".hashCode(), PROJECT);
        favoriteQuery2.setLastQueryTime(System.currentTimeMillis() + 2);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3", "sql3".hashCode(), PROJECT);
        favoriteQuery3.setLastQueryTime(System.currentTimeMillis() + 3);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        return Lists.newArrayList(favoriteQuery1, favoriteQuery2, favoriteQuery3);
    }

    private List<FavoriteQuery> testDataWithInitialValue() {
        FavoriteQuery favoriteQuery4 = new FavoriteQuery("sql4", "sql4".hashCode(), PROJECT, System.currentTimeMillis() + 3, 5, 100);
        favoriteQuery4.setSuccessCount(4);
        favoriteQuery4.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery5 = new FavoriteQuery("sql5", "sql5".hashCode(), PROJECT, System.currentTimeMillis() + 4, 5, 100);
        favoriteQuery5.setSuccessCount(4);
        favoriteQuery5.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery6 = new FavoriteQuery("sql6", "sql6".hashCode(), PROJECT, System.currentTimeMillis() + 5, 5, 100);
        favoriteQuery6.setSuccessCount(4);
        favoriteQuery6.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        return Lists.newArrayList(favoriteQuery4, favoriteQuery5, favoriteQuery6);
    }

    @Test
    public void testBasics() {
        List<FavoriteQueryResponse> favoriteQueries = favoriteQueryDao.getByPage(PROJECT, 10, 0);
        Assert.assertEquals(0, favoriteQueries.size());

        // first insert data without initial values
        favoriteQueryDao.batchInsert(testDataWithoutInitialValue());

        favoriteQueries = favoriteQueryDao.getByPage(PROJECT, 10, 0);
        FavoriteQueryResponse latestFavoriteQuery = favoriteQueries.get(0);
        Assert.assertEquals(3, favoriteQueries.size());
        Assert.assertEquals("sql3", latestFavoriteQuery.getSqlPattern());
        Assert.assertEquals(0, latestFavoriteQuery.getTotalCount());
        Assert.assertEquals(0, latestFavoriteQuery.getAverageDuration(), 0.1);
        Assert.assertEquals(0, latestFavoriteQuery.getSuccessRate(), 0.1);
        Assert.assertEquals(FavoriteQuery.CHANNEL_FROM_RULE, latestFavoriteQuery.getChannel());
        Assert.assertEquals(3, favoriteQueryDao.getSqlPatternHashSet().get(PROJECT).size());

        // then insert another 3 rows with initial values
        favoriteQueryDao.batchInsert(testDataWithInitialValue());

        favoriteQueries = favoriteQueryDao.getByPage(PROJECT, 10, 0);
        Assert.assertEquals(6, favoriteQueries.size());
        latestFavoriteQuery = favoriteQueries.get(0);
        Assert.assertEquals("sql6", latestFavoriteQuery.getSqlPattern());
        Assert.assertEquals(5, latestFavoriteQuery.getTotalCount());
        Assert.assertEquals(20, latestFavoriteQuery.getAverageDuration(), 0.1);
        Assert.assertEquals(0.8, latestFavoriteQuery.getSuccessRate(), 0.1);
        Assert.assertEquals(FavoriteQuery.CHANNEL_FROM_RULE, latestFavoriteQuery.getChannel());
        Assert.assertEquals(6, favoriteQueryDao.getSqlPatternHashSet().get(PROJECT).size());

        // assert page cutting
        favoriteQueries = favoriteQueryDao.getByPage(PROJECT, 3, 1);
        Assert.assertEquals(3, favoriteQueries.size());
        Assert.assertEquals("sql3", favoriteQueries.get(0).getSqlPattern());

        // assert batch update
        List<FavoriteQuery> favoriteQueriesToUpdate = Lists.newArrayList();
        for (FavoriteQuery favoriteQuery : testDataWithInitialValue()) {
            favoriteQuery.setTotalCount(1);
            favoriteQuery.setTotalDuration(80);
            favoriteQuery.setSuccessCount(0);
            favoriteQueriesToUpdate.add(favoriteQuery);
        }

        favoriteQueryDao.batchUpdate(favoriteQueriesToUpdate);

        favoriteQueries = favoriteQueryDao.getByPage(PROJECT, 3, 0);
        latestFavoriteQuery = favoriteQueries.get(0);
        Assert.assertEquals(3, favoriteQueries.size());
        Assert.assertEquals("sql6", latestFavoriteQuery.getSqlPattern());
        Assert.assertEquals(6, latestFavoriteQuery.getTotalCount());
        Assert.assertEquals(180, latestFavoriteQuery.getTotalDuration());

        // get unaccelerated favorite queries
        List<String> sqlPatterns = favoriteQueryDao.getUnAcceleratedSqlPattern(PROJECT);
        Assert.assertEquals(6, sqlPatterns.size());

        // update accelerate status
        String testCommnet = "for test";
        for (FavoriteQuery favoriteQuery : favoriteQueriesToUpdate) {
            favoriteQuery.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
            favoriteQuery.setComment(testCommnet);
        }
        favoriteQueryDao.batchUpdateStatus(favoriteQueriesToUpdate);

        FavoriteQuery favoriteQuery = favoriteQueryDao.getFavoriteQuery(favoriteQueriesToUpdate.get(0).getSqlPatternHash(), PROJECT);
        Assert.assertEquals(favoriteQuery.getComment(), testCommnet);

        sqlPatterns = favoriteQueryDao.getUnAcceleratedSqlPattern(PROJECT);
        Assert.assertEquals(3, sqlPatterns.size());

        // get exist favorite query
        FavoriteQuery existFavorite = favoriteQueryDao.getFavoriteQuery("sql1".hashCode(), PROJECT);
        Assert.assertNotNull(existFavorite);
        Assert.assertEquals("sql1", existFavorite.getSqlPattern());

        // get not existing favorite query
        FavoriteQuery notExistFavorite = favoriteQueryDao.getFavoriteQuery("notExistSqlPattern".hashCode(), PROJECT);
        Assert.assertNull(notExistFavorite);

        // when there is no unaccelerated favorite queries
        sqlPatterns = favoriteQueryDao.getUnAcceleratedSqlPattern("not_exist_project");
        Assert.assertNotNull(sqlPatterns);
        Assert.assertEquals(0, sqlPatterns.size());

        // delete favorite query
        favoriteQueryDao.delete("sql1".hashCode(), PROJECT);
        favoriteQueries = favoriteQueryDao.getByPage(PROJECT, Integer.MAX_VALUE, 0);
        Assert.assertEquals(5, favoriteQueries.size());
        Assert.assertEquals(5, favoriteQueryDao.getSqlPatternHashSet().get(PROJECT).size());

        // update the status of not existed favorite query
        FavoriteQuery updateNotExistFavoriteQuery = new FavoriteQuery("not_exist_sql_pattern", "not_exist_sql_pattern".hashCode(), PROJECT);
        updateNotExistFavoriteQuery.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);
        favoriteQueryDao.batchUpdateStatus(Lists.newArrayList(updateNotExistFavoriteQuery));

    }
}
