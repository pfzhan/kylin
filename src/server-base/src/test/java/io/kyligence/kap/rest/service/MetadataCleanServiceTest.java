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

import java.io.IOException;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataCleanServiceTest extends ServiceTestBase {

    private static final String PROJECT = "default";
    private static final String MODEL_NAME = "nmodel_basic";

    private MetadataCleanupService gcService = new MetadataCleanupService();

    @Before
    public void init() throws IOException {
        staticCreateTestMetadata();
        initData();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    private static void initData() throws IOException {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), PROJECT);
        val model = modelMgr.getDataModelDesc(MODEL_NAME);
        val cube = cubeMgr.findMatchingCubePlan(MODEL_NAME);
        val layoutIds = cube.getAllCuboidLayouts().stream().map(NCuboidLayout::getId).collect(Collectors.toList());

        modelMgr.updateDataModel("nmodel_basic", copyForWrite -> copyForWrite.setSemanticVersion(2));

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val mocks = Lists.<FavoriteQuery> newArrayList();
        for (int i = 0; i < 3 * 8; i++) {
            val fq = new FavoriteQuery("sql" + i);
            fq.setTotalCount(1);
            fq.setSuccessCount(1);
            fq.setLastQueryTime(10000 + i);
            fq.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);

            val fqr = new FavoriteQueryRealization();
            fqr.setModelId(model.getId());
            fqr.setSemanticVersion(i % 3);
            fqr.setCubePlanId(cube.getId());
            fqr.setCuboidLayoutId(layoutIds.get(i % 8));

            val fqr2 = new FavoriteQueryRealization();
            fqr2.setModelId(model.getId());
            fqr2.setSemanticVersion(2);
            fqr2.setCubePlanId(cube.getId());
            fqr2.setCuboidLayoutId(4002);
            fq.setRealizations(Lists.newArrayList(fqr, fqr2));
            mocks.add(fq);
            log.debug("prepare {} {}", fq, fqr2);
        }
        favoriteQueryManager.create(mocks);

        // add some new layouts for cube
        cubeMgr.updateCubePlan(cube.getName(), copyForWrite -> {
            val newDesc = new NCuboidDesc();
            newDesc.setId(4000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(1000, 1001, 1005));
            val layout = new NCuboidLayout();
            layout.setId(4001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100, 1001, 1005));
            layout.setAuto(true);
            newDesc.setLayouts(Lists.newArrayList(layout));
            val layout3 = new NCuboidLayout();
            layout3.setId(4002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100, 1001, 1005));
            layout3.setAuto(true);
            newDesc.setLayouts(Lists.newArrayList(layout3));

            val newDesc2 = new NCuboidDesc();
            newDesc2.setId(NCuboidDesc.TABLE_INDEX_START_ID + 4000);
            newDesc2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            val layout2 = new NCuboidLayout();
            layout2.setId(NCuboidDesc.TABLE_INDEX_START_ID + 4001);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            layout2.setManual(true);
            newDesc2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.getCuboids().add(newDesc);
            copyForWrite.getCuboids().add(newDesc2);
        });
    }

    @Test
    public void testCleanup() throws Exception {
        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), PROJECT);
        cubeMgr.updateCubePlan("ncube_basic", copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), NCuboidLayout::equals, true, false);
        });
        gcService.clean();

        val allFqs = favoriteQueryManager.getAll();
        val usedIds = Sets.<Long> newHashSet();
        int waitingSize = 0;
        for (FavoriteQuery fq : allFqs) {
            val fqrs = fq.getRealizations();
            log.debug("fq {}, realization {}", fq, fqrs);
            if (fq.getStatus() == FavoriteQueryStatusEnum.WAITING) {
                Assert.assertEquals(0, fqrs.size());
                waitingSize++;
            } else {
                for (FavoriteQueryRealization realization : fqrs) {
                    Assert.assertTrue(realization.getSemanticVersion() == 2 && realization.getCuboidLayoutId() != 1L);
                    usedIds.add(realization.getCuboidLayoutId());
                }
            }
        }
        Assert.assertEquals(17, waitingSize);

        val dataflowMgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val df = dataflowMgr.getDataflowByModelName(MODEL_NAME);

        for (NDataSegment segment : df.getSegments()) {
            val cuboids = segment.getSegDetails().getCuboids();
            val ids = cuboids.stream().map(NDataCuboid::getCuboidLayoutId).collect(Collectors.toList());
            Assert.assertFalse(ids.contains(1L));
        }

        // check remove useless layouts
        val cube = cubeMgr.getCubePlan("ncube_basic");
        Assert.assertNull(cube.getCuboidLayout(4001L));
        Assert.assertFalse(cube.getCuboidLayout(NCuboidDesc.TABLE_INDEX_START_ID + 4001L).isAuto());
        Assert.assertEquals(9, cube.getAllCuboidLayouts().size());
    }

}
