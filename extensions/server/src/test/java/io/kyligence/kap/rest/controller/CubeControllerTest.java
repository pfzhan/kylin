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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.CubeService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.rest.controller2.CubeControllerV2;
import io.kyligence.kap.rest.request.KapBuildRequest;
import io.kyligence.kap.rest.request.KapSyncRequest;
import io.kyligence.kap.rest.request.SegmentMgmtRequest;
import io.kyligence.kap.rest.response.KapCubeResponse;
import io.kyligence.kap.rest.service.ServiceTestBase;

/**
 */
public class CubeControllerTest extends ServiceTestBase {

    @Autowired
    private CubeControllerV2 cubeControllerV2;

    @Autowired
    CubeService cubeService;

    private static String G_CUBE_NAME = "ci_left_join_cube";

    @Test
    public void testCubesOrder() throws IOException, InterruptedException {

        EnvelopeResponse firstListResponse = cubeControllerV2.getCubesPaging(null, false, null, "default", 0, 10);

        Assert.assertNotNull(firstListResponse.data);
        Assert.assertTrue(firstListResponse.data instanceof HashMap);
        Assert.assertTrue(((HashMap) firstListResponse.data).get("cubes") instanceof List);

        List<String> firstOrder = new ArrayList<>();
        List<String> secondOrder = new ArrayList<>();

        for (Object object : (List) ((HashMap) firstListResponse.data).get("cubes")) {
            Assert.assertTrue(object instanceof KapCubeResponse);
            KapCubeResponse cubeResponse = (KapCubeResponse) object;
            firstOrder.add(cubeResponse.getName());
            CubeDesc cubeDesc = cubeService.getCubeDescManager().getCubeDesc(cubeResponse.getDescName());
            cubeService.updateCubeAndDesc(cubeService.getCubeManager().getCube(cubeResponse.getName()), cubeDesc,
                    "default", true);
            Thread.sleep(1000);
        }

        EnvelopeResponse secondListResponse = cubeControllerV2.getCubesPaging(null, false, null, "default", 0, 10);
        for (Object object : (List) ((HashMap) secondListResponse.data).get("cubes")) {
            KapCubeResponse cubeResponse = (KapCubeResponse) object;
            secondOrder.add(cubeResponse.getName());
        }

        Assert.assertEquals(firstOrder.size(), secondOrder.size());

        for (int i = 0; i < firstOrder.size(); i++) {
            Assert.assertEquals(firstOrder.get(i), secondOrder.get(firstOrder.size() - 1 - i));
        }
    }

    private void prepare() throws IOException {

        String[] mps = { "LSTG_FORMAT_NAME" };

        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(G_CUBE_NAME);
        cubeMgr.updateCubeStatus(cubeInstance, RealizationStatusEnum.READY);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        kapModel.setMutiLevelPartitionColStrs(mps);
        DataModelManager.getInstance(config).updateDataModelDesc(kapModel);
    }

    @Test
    public void testBuildSegment() throws IOException {
        prepare();

        prepareBuildSegments();
    }

    @Test
    public void testDropSegment() throws IOException {
        prepare();

        Segments<CubeSegment> segments = prepareBuildSegments();
        List<String> segmentList = Lists.newArrayList();
        for (CubeSegment cs : segments) {
            segmentList.add(cs.getName());
        }
        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("DROP");
        request.setMpValues("ABIN");
        request.setSegments(segmentList);
        request.setForce(true);

        CubeInstance cube = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME,
                new String[] { "ABIN" });
        Segments<CubeSegment> segs = cube.getSegments();
        Assert.assertEquals(segs.size(), 2);
        EnvelopeResponse response = cubeControllerV2.manageSegments(G_CUBE_NAME, request);
        Assert.assertEquals(response.code, "000");

        // empty MP Cube is auto-deleted
        cube = CubeManager.getInstance(getTestConfig()).getCube(cube.getName());
        Assert.assertEquals(null, cube);
    }

    @Test
    public void testMergeSegment() throws IOException {
        prepare();

        Segments<CubeSegment> segments = prepareBuildSegments();
        List<String> segmentList = Lists.newArrayList();
        for (CubeSegment cs : segments) {
            segmentList.add(cs.getName());
        }

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("MERGE");
        request.setMpValues("ABIN");
        request.setSegments(segmentList);
        request.setForce(true);

        CubeInstance cube = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME,
                new String[] { "ABIN" });
        Segments<CubeSegment> segs = cube.getSegments();
        Assert.assertEquals(segs.size(), 2);
        EnvelopeResponse response = cubeControllerV2.manageSegments(G_CUBE_NAME, request);
        Assert.assertEquals(response.code, "000");

        cube = CubeManager.getInstance(getTestConfig()).getCube(cube.getName());
        segs = cube.getSegments();
        Assert.assertEquals(segs.size(), 3);
    }

    @Test
    public void testRefreshSegment() throws IOException {
        prepare();

        Segments<CubeSegment> segments = prepareBuildSegments();
        List<String> segmentList = Lists.newArrayList();
        for (CubeSegment cs : segments) {
            segmentList.add(cs.getName());
        }

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("REFRESH");
        request.setMpValues("ABIN");
        request.setSegments(segmentList);
        request.setForce(true);

        CubeInstance cube = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME,
                new String[] { "ABIN" });
        Segments<CubeSegment> segs = cube.getSegments();
        Assert.assertEquals(segs.size(), 2);
        EnvelopeResponse response = cubeControllerV2.manageSegments(G_CUBE_NAME, request);
        Assert.assertEquals(response.code, "000");

        cube = CubeManager.getInstance(getTestConfig()).getCube(cube.getName());
        segs = cube.getSegments();
        Assert.assertEquals(4, segs.size());
    }

    private Segments<CubeSegment> prepareBuildSegments() throws IOException {
        KapBuildRequest request = new KapBuildRequest();
        request.setBuildType("BUILD");
        request.setMpValues("ABIN");
        request.setStartTime(0L);
        request.setEndTime(1508866031000L);
        request.setForce(true);

        EnvelopeResponse response = cubeControllerV2.build(G_CUBE_NAME, request);
        Assert.assertEquals(response.code, "000");

        request.setStartTime(1508866031000L);
        request.setEndTime(1509270087926L);
        response = cubeControllerV2.build(G_CUBE_NAME, request);
        Assert.assertEquals(response.code, "000");

        CubeInstance cube = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME,
                new String[] { "ABIN" });

        Segments<CubeSegment> segments = cube.getSegments();
        for (CubeSegment cs : segments) {
            readySegment(cube, cs);
        }
        cube = CubeManager.getInstance(getTestConfig()).getCube(cube.getName());
        Assert.assertEquals(segments.size(), 2);

        return segments;
    }

    @Test
    public void testBatchSync() throws IOException, InterruptedException {
        String cubeName = "ci_left_join_cube";
        String[] mps = { "ORDER_ID" };

        List<KapSyncRequest> requestList = Lists.newArrayList();
        cubeControllerV2.batchSync(cubeName, requestList);

        convertCommonToMPMaster(cubeName, mps);

        KapSyncRequest request1 = new KapSyncRequest();
        request1.setMpValues("12");
        request1.setPointList(Lists.newArrayList(new Long[] { 12L, 16L, 56L, 99L, 121L }));
        request1.setRangeList(Lists.newArrayList(new Long[][] { new Long[] { 14L, 18L }, new Long[] { 77L, 110L } }));
        requestList.add(request1);

        cubeControllerV2.batchSync(cubeName, requestList);

        CubeInstance cube = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME,
                new String[] { "12" });

        Segments<CubeSegment> segments = cube.getSegments();
        Assert.assertEquals(segments.size(), 5);
        Assert.assertEquals(segments.get(0).getName(), "12_13");
        Assert.assertEquals(segments.get(1).getName(), "14_19");
        Assert.assertEquals(segments.get(2).getName(), "56_57");
        Assert.assertEquals(segments.get(3).getName(), "77_111");
        Assert.assertEquals(segments.get(4).getName(), "121_122");
    }

    private void convertCommonToMPMaster(String cubeName, String[] mps) throws IOException {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        kapModel.setMutiLevelPartitionColStrs(mps);
        kapModel.getPartitionDesc().setPartitionDateFormat("");
        DataModelManager.getInstance(config).updateDataModelDesc(kapModel);
    }

    @Test // https://github.com/Kyligence/KAP/issues/3150
    public void testBuildAndRefreshFullBuild() throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeInstance nonPartCube = cubeMgr.getCube("fifty_dim_full_build_cube");
        Assert.assertEquals(0, nonPartCube.getSegments().size());

        // first build
        KapBuildRequest buildReq = new KapBuildRequest();
        buildReq.setBuildType("BUILD");
        cubeControllerV2.build(nonPartCube.getName(), buildReq);
        nonPartCube = cubeMgr.getCube("fifty_dim_full_build_cube"); // load again
        Assert.assertEquals(1, nonPartCube.getSegments().size());
        Assert.assertEquals("FULL_BUILD", nonPartCube.getSegments().get(0).getName());

        // hack the READY status
        readySegment(nonPartCube, nonPartCube.getSegments().get(0));

        // refresh build
        SegmentMgmtRequest refreshReq = new SegmentMgmtRequest();
        refreshReq.setBuildType("REFRESH");
        refreshReq.setSegments(Lists.newArrayList("FULL_BUILD"));
        cubeControllerV2.manageSegments("fifty_dim_full_build_cube", refreshReq);
        nonPartCube = cubeMgr.getCube("fifty_dim_full_build_cube"); // load again
        Assert.assertEquals(2, nonPartCube.getSegments().size());
        Assert.assertEquals("FULL_BUILD", nonPartCube.getSegments().get(0).getName());
        Assert.assertEquals("FULL_BUILD", nonPartCube.getSegments().get(1).getName());
    }

    private CubeInstance readySegment(CubeInstance cube, CubeSegment seg) throws IOException {
        return CubeManager.getInstance(getTestConfig()).updateCubeSegStatus(seg, SegmentStatusEnum.READY);
    }
}
