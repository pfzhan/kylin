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

package io.kyligence.kap.metadata.usergroup;

import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NUserGroupManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCRUD() {
        NUserGroupManager group = NUserGroupManager.getInstance(getTestConfig());
        group.add("g1");
        group.add("g2");
        group.add("g3");
        Assert.assertTrue(group.exists("g1"));
        Assert.assertFalse(group.exists("g4"));
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), group.getAllGroupNames());
        try {
            group.add("g1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (KylinException e) {
            Assert.assertEquals("Group [g1] already exists.", e.getMessage());
        }

        group.delete("g1");
        Assert.assertFalse(group.exists("g1"));

        try {
            group.delete("g1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Invalid values in parameter “group_name“. The value g1 doesn’t exist.",
                    e.getMessage());
        }
    }

    @Test
    public void testCRUDCaseInsensitive() {
        NUserGroupManager group = NUserGroupManager.getInstance(getTestConfig());
        group.add("test1");
        group.add("test2");
        group.add("test3");
        try {
            group.add("TEST1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (KylinException e) {
            Assert.assertEquals("Group [test1] already exists.", e.getMessage());
        }

        group.delete("Test1");
        Assert.assertFalse(group.exists("test1"));

        try {
            group.delete("test1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Invalid values in parameter “group_name“. The value test1 doesn’t exist.",
                    e.getMessage());
        }
    }
}
