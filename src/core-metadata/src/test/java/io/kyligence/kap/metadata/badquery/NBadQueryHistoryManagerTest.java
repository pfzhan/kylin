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

package io.kyligence.kap.metadata.badquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NBadQueryHistoryManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        NBadQueryHistory history = NBadQueryHistoryManager.getInstance(getTestConfig(), "default")
                .getBadQueriesForProject();
        System.out.println(JsonUtil.writeValueAsIndentString(history));

        NavigableSet<BadQueryEntry> entries = history.getEntries();
        assertEquals(3, entries.size());

        BadQueryEntry entry1 = entries.first();
        assertEquals("Pushdown", entry1.getAdj());
        assertEquals("sandbox.hortonworks.com", entry1.getServer());
        assertEquals("select * from test_kylin_fact limit 10", entry1.getSql());

        entries.pollFirst();
        BadQueryEntry entry2 = entries.first();
        assertTrue(entry2.getStartTime() > entry1.getStartTime());
    }

    @Test
    public void testAddEntryToProject() throws IOException {
        KylinConfig kylinConfig = getTestConfig();
        NBadQueryHistoryManager manager = NBadQueryHistoryManager.getInstance(kylinConfig, "default");
        BadQueryEntry entry = new BadQueryEntry("sql", "adj", 1459362239992L, 100, "server", "t-0", "user");
        NBadQueryHistory history = manager.upsertEntryToProject(entry);
        NavigableSet<BadQueryEntry> entries = history.getEntries();
        assertEquals(4, entries.size());

        BadQueryEntry newEntry = entries.last();

        System.out.println(newEntry);
        assertEquals("sql", newEntry.getSql());
        assertEquals("adj", newEntry.getAdj());
        assertEquals(1459362239992L, newEntry.getStartTime());
        assertEquals("server", newEntry.getServer());
        assertEquals("user", newEntry.getUser());
        assertEquals("t-0", newEntry.getThread());

        for (int i = 0; i < kylinConfig.getBadQueryHistoryNum(); i++) {
            BadQueryEntry tmp = new BadQueryEntry("sql", "adj", 1459362239993L + i, 100 + i, "server", "t-0", "user");
            history = manager.upsertEntryToProject(tmp);
        }
        assertEquals(kylinConfig.getBadQueryHistoryNum(), history.getEntries().size());
    }

    @Test
    public void testUpdateEntryToProject() throws IOException {
        KylinConfig kylinConfig = getTestConfig();
        NBadQueryHistoryManager manager = NBadQueryHistoryManager.getInstance(kylinConfig, "default");

        manager.upsertEntryToProject(new BadQueryEntry("sql", "adj", 1459362239000L, 100, "server", "t-0", "user"));
        NBadQueryHistory history = manager
                .upsertEntryToProject(new BadQueryEntry("sql", "adj2", 1459362239000L, 120, "server2", "t-1", "user"));

        NavigableSet<BadQueryEntry> entries = history.getEntries();
        BadQueryEntry newEntry = entries
                .floor(new BadQueryEntry("sql", "adj2", 1459362239000L, 120, "server2", "t-1", "user"));
        System.out.println(newEntry);
        assertEquals("adj2", newEntry.getAdj());
        assertEquals("server2", newEntry.getServer());
        assertEquals("t-1", newEntry.getThread());
        assertEquals("user", newEntry.getUser());
        assertEquals(120, (int) newEntry.getRunningSec());
    }

}
