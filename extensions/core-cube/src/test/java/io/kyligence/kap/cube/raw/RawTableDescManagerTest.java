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

package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class RawTableDescManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {

        RawTableDescManager mgr = RawTableDescManager.getInstance(getTestConfig());
        final String name = "ci_left_join_cube";

        // init
        RawTableDesc existing = mgr.getRawTableDesc(name);
        assertTrue(existing != null);
        Assert.assertEquals(1, existing.getFuzzyColumns().size());

        // remove
        mgr.removeRawTableDesc(existing);
        mgr.reloadAll();
        RawTableDesc toRemove = mgr.getRawTableDesc(name);
        assertTrue(null == toRemove);

        // create
        existing.setLastModified(0L);
        mgr.createRawTableDesc(existing);
        mgr.reloadAll();
        RawTableDesc toCreate = mgr.getRawTableDesc(name);
        assertTrue(null != toCreate);
        Assert.assertEquals(1, toCreate.getFuzzyColumns().size());

        // update
        RawTableDesc toUpdate = mgr.getRawTableDesc(name);
        toUpdate.setAutoMergeTimeRanges(new long[] { 100, 200 });
        mgr.updateRawTableDesc(toUpdate);
        mgr.reloadAll();
        RawTableDesc updated = mgr.getRawTableDesc(name);
        assertArrayEquals(new long[] { 100, 200 }, updated.getAutoMergeTimeRanges());
    }
}
