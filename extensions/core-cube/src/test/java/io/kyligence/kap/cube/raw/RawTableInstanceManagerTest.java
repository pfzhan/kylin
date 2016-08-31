package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class RawTableInstanceManagerTest extends LocalFileMetadataTestCase {

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
        RawTableInstanceManager mgr = RawTableInstanceManager.getInstance(getTestConfig());

        final String name = "test_kylin_cube_with_slr_desc";

        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());
        CubeInstance cubeInstance = CubeInstance.create(name, cubeDescMgr.getCubeDesc(name));

        // remove existing
        RawTableInstance existing = mgr.getRawTableInstance(name);
        if (existing != null)
            mgr.removeRawTableInstance(existing);

        // create again
        RawTableInstance rawTableInstance = new RawTableInstance(cubeInstance);
        mgr.createRawTableInstance(rawTableInstance);

        // reload
        mgr.reloadAllRawTableInstance();

        // get and update
        RawTableInstance toUpdate = mgr.getRawTableInstance(name);
        toUpdate.setVersion("dummy");
        mgr.updateRawTableInstance(toUpdate);

        // reload
        mgr.reloadAllRawTableInstance();

        // get and update
        RawTableInstance existing2 = mgr.getRawTableInstance(name);
        assertEquals("dummy", existing2.getVersion());
    }
}
