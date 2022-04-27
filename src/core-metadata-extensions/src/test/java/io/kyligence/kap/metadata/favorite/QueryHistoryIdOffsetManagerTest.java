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


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class QueryHistoryIdOffsetManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testSaveAndUpdateIdOffset() {
        // test save id offset
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), "default");
        manager.save(new QueryHistoryIdOffset(12345L));

        Assert.assertEquals(12345L, manager.get().getOffset());

        // test update id offset
        QueryHistoryIdOffset queryHistoryIdOffset = manager.get();
        queryHistoryIdOffset.setOffset(45678L);
        manager.save(queryHistoryIdOffset);

        Assert.assertEquals(45678L, manager.get().getOffset());
    }

    @Test
    public void testInitIdOffset() {
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), "newten");
        QueryHistoryIdOffset queryHistoryIdOffset = manager.get();
        Assert.assertNotNull(queryHistoryIdOffset);
        Assert.assertEquals(0, queryHistoryIdOffset.getOffset());
    }

}