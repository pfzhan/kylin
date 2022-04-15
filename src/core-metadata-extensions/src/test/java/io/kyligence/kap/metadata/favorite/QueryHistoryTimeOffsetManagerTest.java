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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class QueryHistoryTimeOffsetManagerTest extends NLocalFileMetadataTestCase {
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
        // test save time offset
        QueryHistoryTimeOffsetManager manager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), "default");
        QueryHistoryTimeOffset timeOffset = new QueryHistoryTimeOffset(1000, 1000);
        manager.save(timeOffset);

        timeOffset = manager.get();
        Assert.assertEquals(1000L, timeOffset.getAutoMarkTimeOffset());
        Assert.assertEquals(1000L, timeOffset.getFavoriteQueryUpdateTimeOffset());

        // test update time offset
        timeOffset.setAutoMarkTimeOffset(999);
        timeOffset.setFavoriteQueryUpdateTimeOffset(999);

        manager.save(timeOffset);

        timeOffset = manager.get();

        Assert.assertEquals(999, timeOffset.getAutoMarkTimeOffset());
        Assert.assertEquals(999, timeOffset.getFavoriteQueryUpdateTimeOffset());
    }

    @Test
    public void testProjectHasNoTimeOffsetData() {
        QueryHistoryTimeOffsetManager manager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), "newten");
        QueryHistoryTimeOffset timeOffset = manager.get();
        Assert.assertNotNull(timeOffset);
        Assert.assertNotNull(timeOffset.getAutoMarkTimeOffset());
        Assert.assertNotNull(timeOffset.getFavoriteQueryUpdateTimeOffset());
    }
}
