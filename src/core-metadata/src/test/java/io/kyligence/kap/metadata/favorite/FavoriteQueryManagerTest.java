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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class FavoriteQueryManagerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        // when favorite query initial total count is 0
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1", 1000, 0, 0);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2", 1001, 0, 0);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3", 1002, 0, 0);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        // when favorite query initial total count is not 0
        FavoriteQuery favoriteQuery4 = new FavoriteQuery("sql4", 1003, 10, 2000);
        favoriteQuery4.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        FavoriteQuery favoriteQuery5 = new FavoriteQuery("sql5", 1004, 10, 2000);
        favoriteQuery5.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery6 = new FavoriteQuery("sql6", 1005, 10, 2000);
        favoriteQuery6.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        Set<FavoriteQuery> set = new HashSet<>();
        set.add(favoriteQuery1);
        set.add(favoriteQuery2);
        set.add(favoriteQuery3);
        set.add(favoriteQuery4);
        set.add(favoriteQuery5);
        set.add(favoriteQuery6);
        favoriteQueryManager.create(set);
        // assert if sql patterns exist
        Assert.assertEquals(6, favoriteQueryManager.getAll().size());
        Assert.assertTrue(favoriteQueryManager.contains("sql1"));
        Assert.assertTrue(favoriteQueryManager.contains("sql2"));
        Assert.assertTrue(favoriteQueryManager.contains("sql3"));
        Assert.assertTrue(favoriteQueryManager.contains("sql4"));
        Assert.assertTrue(favoriteQueryManager.contains("sql5"));
        Assert.assertTrue(favoriteQueryManager.contains("sql6"));
        Assert.assertFalse(favoriteQueryManager.contains("sql7"));

        // insert sql that is already in fq
        favoriteQueryManager.create(new HashSet<FavoriteQuery>() {
            {
                add(new FavoriteQuery("sql1"));
            }
        });
        Assert.assertEquals(6, favoriteQueryManager.getAll().size());

        // insert sql that is in blacklist
        favoriteQueryManager.create(new HashSet<FavoriteQuery>() {
            {
                add(new FavoriteQuery("SELECT *\nFROM \"TEST_KYLIN_FACT\""));
            }
        });
        Assert.assertEquals(6, favoriteQueryManager.getAll().size());

        // case of map size is zero
        favoriteQueryManager.clearFavoriteQueryMap();
        Assert.assertNull(favoriteQueryManager.getFavoriteQueryMap());
        favoriteQueryManager.updateFavoriteQueryMap(favoriteQuery1);
        Assert.assertTrue(favoriteQueryManager.contains("sql1"));

        // get not exist favorite query
        Assert.assertNull(favoriteQueryManager.get("not_exist_sql_pattern"));

        List<FavoriteQuery> favoriteQueries = favoriteQueryManager.getAll();

        favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        Assert.assertEquals(6, favoriteQueries.size());
        Assert.assertEquals("sql6", favoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals(200, favoriteQueries.get(0).getAverageDuration(), 0.1);
        Assert.assertEquals(0, favoriteQueries.get(0).getSuccessRate(), 0.1);

        favoriteQuery4 = new FavoriteQuery("sql4");
        favoriteQuery4.setTotalCount(10);
        favoriteQuery4.setTotalDuration(1000);
        favoriteQuery4.setSuccessCount(5);

        favoriteQuery5 = new FavoriteQuery("sql5");
        favoriteQuery5.setTotalDuration(0);
        favoriteQuery5.setTotalCount(10);

        favoriteQuery6 = new FavoriteQuery("sql6");
        favoriteQuery6.setTotalCount(10);
        favoriteQuery6.setTotalDuration(0);
        favoriteQuery6.setLastQueryTime(1005);

        // update statistics
        favoriteQueryManager.updateStatistics(Lists.newArrayList(favoriteQuery4, favoriteQuery5, favoriteQuery6));
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(6, favoriteQueries.size());
        Assert.assertEquals(0.25, favoriteQueryManager.get("sql4").getSuccessRate(), 0.01);
        Assert.assertEquals(100, favoriteQueryManager.get("sql5").getAverageDuration(), 0.1);
        Assert.assertEquals(1005, favoriteQueryManager.get("sql6").getLastQueryTime());

        // update not exist sql pattern
        favoriteQueryManager.updateStatistics(Lists.newArrayList(new FavoriteQuery("not_exist_sql_pattern")));
        Assert.assertEquals(6, favoriteQueryManager.getAll().size());

        // update status
        favoriteQueryManager.updateStatus("sql1", FavoriteQueryStatusEnum.ACCELERATING, null);
        favoriteQueryManager.updateStatus("sql2", FavoriteQueryStatusEnum.BLOCKED, "test_comment");
        favoriteQueryManager.updateStatus("sql3", FavoriteQueryStatusEnum.FULLY_ACCELERATED, null);

        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(6, favoriteQueries.size());

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, favoriteQueryManager.get("sql1").getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.BLOCKED, favoriteQueryManager.get("sql2").getStatus());
        Assert.assertEquals("test_comment", favoriteQueryManager.get("sql2").getComment());
        Assert.assertEquals(FavoriteQueryStatusEnum.FULLY_ACCELERATED, favoriteQueryManager.get("sql3").getStatus());

        // get unaccelerated favorite queries
        Assert.assertEquals(4, favoriteQueryManager.getUnAcceleratedSqlPattern().size());

        // update not exist sql pattern status, no exception
        favoriteQueryManager.updateStatus("not_exist_sql_pattern", FavoriteQueryStatusEnum.FULLY_ACCELERATED, null);

        // delete
        favoriteQueryManager.delete(favoriteQueryManager.get("sql1"));
        Assert.assertEquals(5, favoriteQueryManager.getAll().size());

        // delete not exist sql pattern
        FavoriteQuery favoriteQuery = new FavoriteQuery("not_exist_sql_pattern");
        favoriteQueryManager.delete(favoriteQuery);
        Assert.assertEquals(5, favoriteQueryManager.getAll().size());
    }

    @Test
    public void testRealizations() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);

        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1", 1000, 10, 2000);
        FavoriteQueryRealization realization1 = new FavoriteQueryRealization();
        realization1.setModelId("model1");
        realization1.setLayoutId(1);
        FavoriteQueryRealization realization2 = new FavoriteQueryRealization();
        realization2.setModelId("model1");
        realization2.setLayoutId(2);
        favoriteQuery1.setRealizations(Lists.newArrayList(realization1, realization2));

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2", 1001, 10, 2000);
        realization1 = new FavoriteQueryRealization();
        realization1.setModelId("model2");
        realization1.setLayoutId(1);
        realization2 = new FavoriteQueryRealization();
        realization2.setModelId("model1");
        realization2.setLayoutId(1);
        favoriteQuery2.setRealizations(Lists.newArrayList(realization1, realization2));

        Set<FavoriteQuery> set = new HashSet<>();
        set.add(favoriteQuery1);
        set.add(favoriteQuery2);
        favoriteQueryManager.create(set);
        Assert.assertEquals(2, favoriteQueryManager.getAll().size());
        Assert.assertEquals(2, favoriteQueryManager.get("sql1").getRealizations().size());

        // get realizations by condition
        List<FavoriteQueryRealization> realizations = favoriteQueryManager.getRealizationsByConditions("model1", 1L);
        Assert.assertEquals(2, realizations.size());
        Assert.assertEquals("model1", realizations.get(0).getModelId());
        Assert.assertEquals(1, realizations.get(0).getLayoutId());

        // remove realizations
        favoriteQueryManager.removeRealizations("sql2");
        Assert.assertEquals(0, favoriteQueryManager.get("sql2").getRealizations().size());

        // remove not exist sql pattern's realizations, no exception
        favoriteQueryManager.removeRealizations("not_exist_sql_pattern");

        // reset realizations
        FavoriteQueryRealization newRealization = new FavoriteQueryRealization();
        newRealization.setModelId("model3");
        newRealization.setLayoutId(2);

        favoriteQueryManager.resetRealizations("sql1", Lists.newArrayList(newRealization));
        Assert.assertEquals(1, favoriteQueryManager.get("sql1").getRealizations().size());
        Assert.assertEquals("model3", favoriteQueryManager.get("sql1").getRealizations().get(0).getModelId());

        // reset not exist sql pattern's realizations, no exception
        favoriteQueryManager.resetRealizations("not_exist_sql_pattern", Lists.newArrayList(newRealization));
    }

    @Test
    public void testGetLowFrequencyFavoriteQuery() {
        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        long currentTime = System.currentTimeMillis();
        long dayInMillis = 24 * 60 * 60 * 1000L;
        long currentDate = currentTime - currentTime % dayInMillis;

        // a low frequency favorite query, related layout 1 will be considered as garbage
        val fq1 = new FavoriteQuery("sql1");
        fq1.setCreateTime(currentTime - 32 * dayInMillis);
        fq1.setFrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 7 * dayInMillis, 1);
                put(currentDate - 31 * dayInMillis, 100);
            }
        });

        val fqCreatedLongAgo = new FavoriteQuery("sql_long_ago");
        fqCreatedLongAgo.setCreateTime(currentTime - 60 * dayInMillis);
        fqCreatedLongAgo.setFrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate, 2);
            }
        });

        // not reached low frequency threshold, related layouts are 40001 and 40002
        val fq2 = new FavoriteQuery("sql2");
        fq2.setCreateTime(currentTime - 8 * dayInMillis);
        fq2.setFrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 7 * dayInMillis, 1);
                put(currentDate, 2);
            }
        });

        // not a low frequency fq, related layouts are 10001 and 10002
        val fq3 = new FavoriteQuery("sql3");
        fq3.setCreateTime(currentTime - 31 * dayInMillis);
        fq3.setFrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 30 * dayInMillis, 10);
            }
        });

        favoriteQueryManager.create(Sets.newHashSet(fq1, fq2, fq3, fqCreatedLongAgo));

        List<FavoriteQuery> lowFrequencyFQs = favoriteQueryManager.getLowFrequencyFQs();
        Assert.assertEquals(2, lowFrequencyFQs.size());
    }
}
