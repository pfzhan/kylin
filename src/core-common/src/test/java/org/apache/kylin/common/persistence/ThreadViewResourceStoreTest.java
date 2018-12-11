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

package org.apache.kylin.common.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import lombok.val;

public class ThreadViewResourceStoreTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        System.setProperty("kylin.env", "UT");
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
        System.clearProperty("kylin.env");
    }

    private ResourceStore getStore() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        return new ThreadViewResourceStore((InMemResourceStore) underlying, config);
    }

    @Test
    public void testUnderlyingChangedDuringOverlay() {
        //TODO
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void testOverlay() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);

        String dir1 = "/cube";
        String dir2 = "/table";

        String path1 = "/cube/_test.json";
        String path2 = "/table/_test.json";
        String path3 = "/table/_test2.json";
        String pathXyz = "/xyz";
        String pathCubeX = "/cubex";

        StringEntity content1 = new StringEntity("1");
        StringEntity content2 = new StringEntity("2");
        StringEntity content3 = new StringEntity("3");
        StringEntity contentxyz = new StringEntity("xyz");
        StringEntity contentcubex = new StringEntity("cubex");

        //reinit
        underlying.deleteResource(path1);
        underlying.deleteResource(path2);
        underlying.deleteResource(path3);
        underlying.deleteResource(pathXyz);
        underlying.deleteResource(pathCubeX);
        underlying.checkAndPutResource(path1, content1, StringEntity.serializer);
        underlying.checkAndPutResource(path2, content2, StringEntity.serializer);
        underlying.checkAndPutResource(path3, content3, StringEntity.serializer);
        underlying.checkAndPutResource(pathXyz, contentxyz, StringEntity.serializer);
        underlying.checkAndPutResource(pathCubeX, contentcubex, StringEntity.serializer);

        val rs = new ThreadViewResourceStore((InMemResourceStore) underlying, config);

        //test init
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(6, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path2, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertTrue(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test delete
        val old_se_2 = rs.getResource(path2, StringEntity.serializer);

        rs.deleteResource(path2);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(5, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test double delete
        rs.deleteResource(path2);
        assertFalse(rs.exists(path2));

        try {
            rs.checkAndPutResource(path2, old_se_2, StringEntity.serializer);
            fail();
        } catch (IllegalStateException e) {
            //expected
        }

        StringEntity new_se_2 = new StringEntity("new_2");
        rs.checkAndPutResource(path2, new_se_2, StringEntity.serializer);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(6, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path2, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertTrue(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test delete, this time we're deleting from overlay
        rs.deleteResource(path2);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(5, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with path3

        rs.deleteResource(path3);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(4, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(4, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with path1
        rs.deleteResource(path1);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(3, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(3, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathXyz, pathCubeX)));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with pathXyz
        rs.deleteResource(pathXyz);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(2, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(2, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathCubeX)));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertFalse(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // add some new
        StringEntity se1 = new StringEntity("se1");
        StringEntity se2 = new StringEntity("se2");
        rs.checkAndPutResource("/a/b", se1, StringEntity.serializer);
        rs.checkAndPutResource("/z", se2, StringEntity.serializer);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(4, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathCubeX, "/a/b", "/z")));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(4, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathCubeX, "/a", "/z")));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertFalse(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
            assertTrue(rs.exists("/a/b"));
            assertTrue(rs.exists("/z"));
        }

        // normal update
        StringEntity cubex = rs.getResource(pathCubeX, StringEntity.serializer);
        cubex.setStr("cubex2");
        rs.checkAndPutResource(pathCubeX, cubex, StringEntity.serializer);

        StringEntity se2_old = rs.getResource("/z", StringEntity.serializer);
        assertEquals(0, se2_old.getMvcc());
        se2_old.setStr("abccc");
        rs.checkAndPutResource("/z", se2_old, StringEntity.serializer);
        StringEntity se2_new = rs.getResource("/z", StringEntity.serializer);
        assertEquals(1, se2_old.getMvcc());
        assertEquals(1, se2_new.getMvcc());
        assertEquals("abccc", se2_new.getStr());

        se2_new.setStr("abccc2");
        se2_new.setMvcc(0);
        try {
            rs.checkAndPutResource("/z", se2_new, StringEntity.serializer);
            fail();
        } catch (VersionConflictException e) {
            //expected
        }

        // check mvcc
        assertEquals(6, underlying.listResourcesRecursively("/").size());
        assertEquals(0, underlying.getResource(path1, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(path2, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(path3, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource("/UUID", StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(pathCubeX, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(pathXyz, StringEntity.serializer).getMvcc());

        assertEquals(4, rs.listResourcesRecursively("/").size());
        assertEquals(1, rs.getResource(pathCubeX, StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource("/a/b", StringEntity.serializer).getMvcc());
        assertEquals(1, rs.getResource("/z", StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource("/UUID", StringEntity.serializer).getMvcc());

        try {
            Field overlay1 = ThreadViewResourceStore.class.getDeclaredField("overlay");
            overlay1.setAccessible(true);
            InMemResourceStore overlay = (InMemResourceStore) overlay1.get(rs);
            assertEquals(1, overlay.getResource(pathCubeX, StringEntity.serializer).getMvcc());
            assertEquals(0, overlay.getResource("/a/b", StringEntity.serializer).getMvcc());
            assertEquals(1, overlay.getResource("/z", StringEntity.serializer).getMvcc());
            assertFalse(overlay.exists("/UUID"));

            Field data1 = InMemResourceStore.class.getDeclaredField("data");
            data1.setAccessible(true);
            ConcurrentSkipListMap<String, VersionedRawResource> data = (ConcurrentSkipListMap<String, VersionedRawResource>) data1
                    .get(overlay);
            long count = data.entrySet().stream().map(x -> x.getValue())
                    .filter(x -> x == TombVersionedRawResource.getINSTANCE()).count();
            assertEquals(4, count);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAStore() {
        ResourceStoreTestBase.testAStore(getStore());
    }

    @Test
    public void testUUID() {
        ResourceStoreTestBase.testGetUUID(getStore());
    }

}
