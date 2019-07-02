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

package io.kyligence.kap.rest.config.initialize;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import lombok.val;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.Test;

import java.io.IOException;

public class FavoriteQueryUpdateListenerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testOnUpdate() throws IOException {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery = new FavoriteQuery("sql1", 1000, 0, 0);
        favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        favoriteQueryManager.create(Sets.newHashSet(favoriteQuery));
        Assert.assertNotNull(favoriteQueryManager.getFavoriteQueryMap().get("sql1"));
        Assert.assertEquals(1000, favoriteQueryManager.getFavoriteQueryMap().get("sql1").getLastQueryTime());
        val copy = JsonUtil.deepCopy(favoriteQueryManager.get("sql1"), FavoriteQuery.class);
        copy.setLastQueryTime(2000);
        val listener = new FavoriteQueryUpdateListener();
        listener.onUpdate(getTestConfig(), new RawResource(favoriteQueryManager.get("sql1").getResourcePath(),
                ByteStreams.asByteSource(JsonUtil.writeValueAsIndentString(copy).getBytes()), 0L, 0));
        val fqInMap = favoriteQueryManager.getFavoriteQueryMap().get("sql1");
        Assert.assertNotNull(fqInMap);
        Assert.assertEquals(2000, fqInMap.getLastQueryTime());
    }

    @Test
    public void testOnDelete() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery = new FavoriteQuery("sql1", 1000, 0, 0);
        favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        favoriteQueryManager.create(Sets.newHashSet(favoriteQuery));
        Assert.assertNotNull(favoriteQueryManager.getFavoriteQueryMap().get("sql1"));
        Assert.assertEquals(1000, favoriteQueryManager.getFavoriteQueryMap().get("sql1").getLastQueryTime());
        val listener = new FavoriteQueryUpdateListener();
        listener.onDelete(getTestConfig(), favoriteQueryManager.get("sql1").getResourcePath());
        Assert.assertNull(favoriteQueryManager.getFavoriteQueryMap());
    }

}
