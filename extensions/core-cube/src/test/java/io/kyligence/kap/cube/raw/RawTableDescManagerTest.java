package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.kylin.cube.CubeDescManager;
import org.junit.After;
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
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());
        RawTableDescManager mgr = RawTableDescManager.getInstance(getTestConfig());

        final String name = "test_kylin_cube_with_slr_desc";
        
        // remove existing
        RawTableDesc existing = mgr.getRawTableDesc(name);
        assertTrue(existing != null);
        mgr.removeRawTableDesc(existing);
        
        // create again
        RawTableDesc rawTableDesc = new RawTableDesc(cubeDescMgr.getCubeDesc(name));
        rawTableDesc = mgr.createRawTableDesc(rawTableDesc);
        
        // reload
        mgr.reloadAllRawTableDesc();
        
        // get and update
        RawTableDesc toUpdate = mgr.getRawTableDesc(name);
        toUpdate.setVersion("dummy");
        mgr.updateRawTableDesc(toUpdate);
        
        // reload
        mgr.reloadAllRawTableDesc();
        
        // get and update
        RawTableDesc existing2 = mgr.getRawTableDesc(name);
        assertEquals("dummy", existing2.getVersion());
    }
}
