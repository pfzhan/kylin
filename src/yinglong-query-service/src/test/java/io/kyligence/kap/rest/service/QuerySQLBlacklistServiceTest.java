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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class QuerySQLBlacklistServiceTest extends NLocalFileMetadataTestCase {

    private QuerySQLBlacklistService service = new QuerySQLBlacklistService();;

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    private SQLBlacklistRequest blacklistRequest(SQLBlacklistItemRequest... items) {
        val req = new SQLBlacklistRequest();
        req.setProject("default");
        req.setBlacklistItems(Lists.newArrayList(items));
        return req;
    }

    private SQLBlacklistItemRequest itemRequest(String id, String regex, String sql, int concurrentLimit) {
        val item = new SQLBlacklistItemRequest();
        item.setId(id);
        item.setRegex(regex);
        item.setSql(sql);
        item.setConcurrentLimit(concurrentLimit);
        return item;
    }

    @Test
    public void testCrud() throws IOException {
        // save
        val saveRet = service.saveSqlBlacklist(blacklistRequest(
                itemRequest(null, "a", "b", 8),
                itemRequest(null, "c", "d", 8)
        ));
        Assert.assertNotNull(saveRet);
        Assert.assertEquals("default", saveRet.getProject());
        Assert.assertEquals(2, saveRet.getBlacklistItems().size());
        String id1 = saveRet.getBlacklistItems().get(0).getId();
        String id2 = saveRet.getBlacklistItems().get(1).getId();

        // get
        val getRet = service.getItemById("default", itemRequest(id1, null, null, 0));
        Assert.assertEquals(id1, getRet.getId());
        Assert.assertEquals("a", getRet.getRegex());
        Assert.assertEquals("b", getRet.getSql());
        Assert.assertEquals(8, getRet.getConcurrentLimit());

        val getRet1 = service.getItemByRegex("default", itemRequest(null, "a", null, 0));
        Assert.assertEquals(id1, getRet1.getId());
        Assert.assertEquals("a", getRet1.getRegex());
        Assert.assertEquals("b", getRet1.getSql());
        Assert.assertEquals(8, getRet1.getConcurrentLimit());

        val getRet2 = service.getItemBySql("default", itemRequest(null, null, "b", 0));
        Assert.assertEquals(id1, getRet2.getId());
        Assert.assertEquals("a", getRet2.getRegex());
        Assert.assertEquals("b", getRet2.getSql());
        Assert.assertEquals(8, getRet2.getConcurrentLimit());

        // add
        val addRet = service.addSqlBlacklistItem("default",
                itemRequest(null, "e", "f", 9));
        Assert.assertNotNull(addRet);
        Assert.assertEquals("default", addRet.getProject());
        Assert.assertEquals(3, addRet.getBlacklistItems().size());
        String id3 = addRet.getBlacklistItems().get(2).getId();

        val getRet3 = service.getItemById("default", itemRequest(id3, null, null, 0));
        Assert.assertEquals(id3, getRet3.getId());
        Assert.assertEquals("e", getRet3.getRegex());
        Assert.assertEquals("f", getRet3.getSql());
        Assert.assertEquals(9, getRet3.getConcurrentLimit());

        // delete
        service.deleteSqlBlacklistItem("default", id3);
        Assert.assertNull(service.getItemById("default", itemRequest(id3, null, null, 0)));

        // clear
        service.clearSqlBlacklist("default");
        Assert.assertNull(service.getItemById("default", itemRequest(id1, null, null, 0)));
        Assert.assertNull(service.getItemById("default", itemRequest(id2, null, null, 0)));
        Assert.assertNull(service.getItemById("default", itemRequest(id3, null, null, 0)));
    }

    @Test
    public void testConflict() throws IOException {
        // save
        val saveRet = service.saveSqlBlacklist(blacklistRequest(
                itemRequest(null, "a", "b", 8),
                itemRequest(null, "c", "d", 8)
        ));
        Assert.assertNotNull(saveRet);
        Assert.assertEquals("default", saveRet.getProject());
        Assert.assertEquals(2, saveRet.getBlacklistItems().size());
        String id1 = saveRet.getBlacklistItems().get(0).getId();

        // regex
        Assert.assertNotNull(service.checkConflictRegex("default", itemRequest(id1, "c", null, 0)));

        // sql
        Assert.assertNotNull(service.checkConflictSql("default", itemRequest(id1, null, "d", 0)));
    }

}
