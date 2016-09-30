package io.kyligence.kap.rest.controller;

import java.io.IOException;

import org.apache.kylin.rest.service.JobService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.rest.ServiceTestBase;
import io.kyligence.kap.rest.service.RawTableService;

/**
 * Created by wangcheng on 9/27/16.
 */
public class RawTableControllerTest extends ServiceTestBase {

    private RawTableController rawController;
    private RawTableDescController descController;

    @Autowired
    RawTableService rawService;
    @Autowired
    JobService jobService;

    @Before
    public void setup() throws Exception {
        super.setup();

        rawController = new RawTableController();
        rawController.setCubeService(rawService);
        rawController.setJobService(jobService);

        descController = new RawTableDescController();
        descController.setRawService(rawService);

    }

    @Test
    public void testBasics() throws IOException {
        RawTableDesc desc = descController.getDesc("test_kylin_cube_with_slr_ready");
        Assert.assertNotNull(desc);
        RawTableDesc newDesc = new RawTableDesc();
        String newRawName = desc.getName() + "_test_save";

        newDesc.setName(newRawName);
        newDesc.setModelName(desc.getModelName());
        newDesc.setModel(desc.getModel());
        newDesc.getModel().setLastModified(0);
    }
}
