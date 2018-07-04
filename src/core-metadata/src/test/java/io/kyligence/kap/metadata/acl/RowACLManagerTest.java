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

package io.kyligence.kap.metadata.acl;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kylin.metadata.MetadataConstants.TYPE_GROUP;
import static org.apache.kylin.metadata.MetadataConstants.TYPE_USER;

public class RowACLManagerTest extends NLocalFileMetadataTestCase {
    private static String PROJECT = "default";
    private static String PROJECT2 = "default2";
    private static String USER = "u1";
    private KylinConfig configA;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        configA = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws IOException, InterruptedException {
        //add
        RowACLManager manager = new RowACLManager(configA);
        manager.addRowACL(PROJECT, TYPE_USER, "u1", "t1", getColumnToConds());
        manager.addRowACL(PROJECT, TYPE_USER, "u2", "t1", getColumnToConds());
        manager.addRowACL(PROJECT, TYPE_USER, "u1", "t2", getColumnToConds());

        manager.addRowACL(PROJECT, TYPE_GROUP, "g1", "t1", getColumnToConds());
        manager.addRowACL(PROJECT, TYPE_GROUP, "g2", "t2", getColumnToConds());

        manager.addRowACL(PROJECT2, TYPE_USER, "u1", "t1", getColumnToConds());
        manager.addRowACL(PROJECT2, TYPE_GROUP, "u2", "t1", getColumnToConds());
        manager.addRowACL(PROJECT2, TYPE_USER, "u1", "t2", getColumnToConds());

        Assert.assertEquals(2, manager.getRowACLByTable(PROJECT, TYPE_USER, "t1").size());
        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_GROUP, "t1").size());

        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_USER, "t2").size());
        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_GROUP, "t2").size());

        //add duplicated
        try {
            manager.addRowACL(PROJECT, TYPE_USER, "u1", "t1", getColumnToConds());
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, entity:u1, table:t1 already has row ACL!", e.getMessage());
        }

        //delete by user
        manager.deleteRowACL(PROJECT, TYPE_USER, "u1");
        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_USER, "t1").size());
        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_GROUP, "t1").size());

        Assert.assertEquals(0, manager.getRowACLByTable(PROJECT, TYPE_USER, "t2").size());
        Assert.assertEquals(1, manager.getRowACLByTable(PROJECT, TYPE_GROUP, "t2").size());

        //delete by table
        manager.deleteRowACLByTbl(PROJECT, "t1");
        Assert.assertEquals(0, manager.getRowACLByTable(PROJECT, TYPE_USER, "t1").size());
        Assert.assertEquals(0, manager.getRowACLByTable(PROJECT, TYPE_GROUP, "t1").size());

        //specify delete
        manager.addRowACL(PROJECT, TYPE_USER, "u3", "t1", getColumnToConds());
        Assert.assertTrue(manager.getRowACLByTable(PROJECT, TYPE_USER, "t1").containsKey("u3"));
        manager.deleteRowACL(PROJECT, TYPE_USER, "u3");
        Assert.assertFalse(manager.getRowACLByTable(PROJECT, TYPE_USER, "t1").containsKey("u3"));
        //nothing happens
        manager.deleteRowACL(PROJECT, TYPE_USER, "not_exist");
    }

    /*
    @Test
    public void testMultiNodes() throws IOException, InterruptedException {
        final String table = "DEFAULT.TEST_COUNTRY";
        RowACLManager managerA = new RowACLManager(configA);
        RowACLManager managerB = new RowACLManager(configB);
        managerA.addRowACL(PROJECT, TYPE_USER, "u1", table, getColumnToConds());
        Thread.sleep(3000);
        Assert.assertEquals(1, managerB.getRowACLEntitesByTable(PROJECT, TYPE_USER, table).size());
        Assert.assertEquals(1, managerA.getRowACLEntitesByTable(PROJECT, TYPE_USER, table).size());
    
    
        Assert.assertEquals(1, managerB.getConcatCondsByEntity(PROJECT, TYPE_USER, "u1").size());
        Assert.assertEquals(1, managerA.getConcatCondsByEntity(PROJECT, TYPE_USER, "u1").size());
    
        managerA.deleteRowACL(PROJECT, TYPE_USER, "u1", table);
        Thread.sleep(3000);
        Assert.assertEquals(0, managerB.getRowACLEntitesByTable(PROJECT, TYPE_USER, table).size());
        Assert.assertEquals(0, managerA.getRowACLEntitesByTable(PROJECT, TYPE_USER, table).size());
    
    
        Assert.assertEquals(0, managerB.getConcatCondsByEntity(PROJECT, TYPE_USER, "u1").size());
        Assert.assertEquals(0, managerA.getConcatCondsByEntity(PROJECT, TYPE_USER, "u1").size());
    }
    */

    @Test
    @Ignore //todo
    public void testGetConcatCondsByEntity() throws IOException {
        RowACLManager manager = new RowACLManager(configA);
        manager.addRowACL(PROJECT, TYPE_USER, "u1", "DEFAULT.TEST_COUNTRY", getColumnToConds());
        Map<String, String> tableToCond = manager.getConcatCondsByEntity(PROJECT, TYPE_USER, "u1");
        Assert.assertEquals(1, tableToCond.size());
        Assert.assertEquals("((COUNTRY='a') OR (COUNTRY='b') OR (COUNTRY='c')) AND ((NAME='d') OR (NAME='e'))",
                tableToCond.get("DEFAULT.TEST_COUNTRY"));
    }

    @Test
    public void testCaseInsensitiveFromDeserializer() throws IOException {
        RowACLManager manager = new RowACLManager(configA);
        manager.addRowACL(PROJECT, TYPE_USER, USER, "default.test_country", getColumnToConds());
        final String PATH = "/row_acl/" + PROJECT + "/user/" + USER + "/DEFAULT.TEST_COUNTRY.json";
        Assert.assertNotNull(ResourceStore.getKylinMetaStore(configA).getResource(PATH));
        manager.getRowACLByTable(PROJECT, TYPE_USER, "default.test_country").containsKey(USER);
    }

    private static ColumnToConds getColumnToConds() {
        Map<String, List<ColumnToConds.Cond>> colToConds = new HashMap<>();
        List<ColumnToConds.Cond> cond1 = Lists.newArrayList(new ColumnToConds.Cond("a"), new ColumnToConds.Cond("b"),
                new ColumnToConds.Cond("c"));
        List<ColumnToConds.Cond> cond2 = Lists.newArrayList(new ColumnToConds.Cond("d"), new ColumnToConds.Cond("e"));
        colToConds.put("COUNTRY", cond1);
        colToConds.put("NAME", cond2);
        return new ColumnToConds(colToConds);
    }
}
