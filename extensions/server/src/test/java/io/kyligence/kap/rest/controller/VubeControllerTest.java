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

package io.kyligence.kap.rest.controller;

import static org.apache.kylin.metadata.realization.RealizationStatusEnum.DESCBROKEN;

import java.io.StringWriter;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ProjectService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kyligence.kap.rest.request.KapCubeRequest;
import io.kyligence.kap.rest.request.VubeBuildRequest;
import io.kyligence.kap.rest.service.KapSuggestionService;
import io.kyligence.kap.rest.service.RawTableService;
import io.kyligence.kap.rest.service.SchedulerJobService;
import io.kyligence.kap.rest.service.ServiceTestBase;
import io.kyligence.kap.rest.service.VubeService;
import io.kyligence.kap.vube.VubeInstance;

public class VubeControllerTest extends ServiceTestBase {

    private VubeController vubeController;

    @Autowired
    @Qualifier("vubeService")
    private VubeService vubeService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("rawTableService")
    private RawTableService rawTableService;

    @Autowired
    @Qualifier("schedulerJobService")
    private SchedulerJobService schedulerJobService;

    @Autowired
    @Qualifier("kapSuggestionService")
    private KapSuggestionService kapSuggestionService;

    @Before
    public void setup() throws Exception {
        super.setup();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        kylinConfig.setProperty("kylin.engine.provider.96", "io.kyligence.kap.engine.mock.MockCubingEngine");
        kylinConfig.setProperty("kylin.engine.default", "96");

        vubeController = new VubeController();
        vubeController.setVubeService(vubeService);
        vubeController.setCubeService(cubeService);
        vubeController.setJobService(jobService);
        vubeController.setProjectService(projectService);
        vubeController.setRawTableService(rawTableService);
        vubeController.setSchedulerJobService(schedulerJobService);
        vubeController.setKapSuggestionService(kapSuggestionService);
    }

    @Test
    public void testCreateVube() throws Exception {
        CubeDesc cubeDesc = cubeService.getCubeDescManager().getCubeDesc("versioned_cube_version_1");
        Assert.assertNotNull(cubeDesc);
        doSave(cubeDesc);
        Assert.assertEquals(1, vubeService.listAllVubeNames().size());

        VubeInstance vube = vubeService.getVubeInstance("versioned_cube_test");
        Assert.assertEquals(1, vube.getVersionedCubes().size());
        Assert.assertEquals(DESCBROKEN, vube.getStatus());
    }

    @Test
    public void testEditVube() throws Exception {
        CubeDesc cubeDesc1 = cubeService.getCubeDescManager().getCubeDesc("versioned_cube_version_1");
        CubeDesc cubeDesc2 = cubeService.getCubeDescManager().getCubeDesc("versioned_cube_version_2");
        Assert.assertNotNull(cubeDesc1);
        Assert.assertNotNull(cubeDesc2);

        doSave(cubeDesc1);
        doSave(cubeDesc2);

        VubeInstance vube = vubeService.getVubeInstance("versioned_cube_test");
        Assert.assertEquals(2, vube.getVersionedCubes().size());
        Assert.assertEquals("versioned_cube_test_version_2", vube.getVersionedCubes().get(1).getName());
    }

    @Test
    public void testBuildVube() throws Exception {
        CubeDesc cubeDesc = cubeService.getCubeDescManager().getCubeDesc("versioned_cube_version_1");
        VubeBuildRequest request = new VubeBuildRequest();

        doSave(cubeDesc);
        request.setBuildType("BUILD");
        request.setStartTime(0);
        request.setEndTime(10000);

        EnvelopeResponse response = vubeController.rebuild("versioned_cube_test", request);
        JobInstance jobInstance = (JobInstance) response.data;

        Assert.assertEquals(JobStatusEnum.PENDING, jobInstance.getStatus());
        Assert.assertEquals(1, jobInstance.getSteps().size());
    }

    private void doSave(CubeDesc cubeDesc) throws Exception {
        CubeDesc newCube = new CubeDesc();
        String newCubeName = "versioned_cube_test";

        newCube.setName(newCubeName);
        newCube.setModelName(cubeDesc.getModelName());
        newCube.setModel(cubeDesc.getModel());
        newCube.setDimensions(cubeDesc.getDimensions());
        newCube.setHbaseMapping(cubeDesc.getHbaseMapping());
        newCube.setMeasures(cubeDesc.getMeasures());
        newCube.setRowkey(cubeDesc.getRowkey());
        newCube.setAggregationGroups(cubeDesc.getAggregationGroups());
        newCube.setEngineType(96);

        newCube.getModel().setLastModified(0);

        ObjectMapper cubeDescMapper = new ObjectMapper();
        StringWriter cubeDescWriter = new StringWriter();
        cubeDescMapper.writeValue(cubeDescWriter, newCube);

        ObjectMapper modelDescMapper = new ObjectMapper();
        StringWriter modelDescWriter = new StringWriter();
        modelDescMapper.writeValue(modelDescWriter, newCube.getModel());

        KapCubeRequest cubeRequest = new KapCubeRequest();
        cubeRequest.setCubeDescData(cubeDescWriter.toString());
        vubeController.updateCubeDesc(cubeRequest);
    }
}
