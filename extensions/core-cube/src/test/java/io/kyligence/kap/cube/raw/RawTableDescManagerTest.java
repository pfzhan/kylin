package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertEquals;
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
        final String name = "test_kylin_cube_with_slr_desc";

        // init
        RawTableDesc existing = mgr.getRawTableDesc(name);
        assertTrue(existing != null);
        Assert.assertEquals(1, existing.getFuzzyColumnSet().size());

        // remove
        mgr.removeRawTableDesc(existing);
        mgr.reloadAllRawTableDesc();
        RawTableDesc toRemove = mgr.getRawTableDesc(name);
        assertTrue(null == toRemove);

        // create
        existing.setLastModified(0L);
        mgr.createRawTableDesc(existing);
        mgr.reloadAllRawTableDesc();
        RawTableDesc toCreate = mgr.getRawTableDesc(name);
        assertTrue(null != toCreate);
        Assert.assertEquals(1, toCreate.getFuzzyColumnSet().size());

        // update
        RawTableDesc toUpdate = mgr.getRawTableDesc(name);
        toUpdate.setVersion("dummy");
        mgr.updateRawTableDesc(toUpdate);
        mgr.reloadAllRawTableDesc();
        RawTableDesc updated = mgr.getRawTableDesc(name);
        assertEquals("dummy", updated.getVersion());
    }
}
