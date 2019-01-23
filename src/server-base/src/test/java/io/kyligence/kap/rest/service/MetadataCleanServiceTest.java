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
import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.rest.security.ACLManager;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataCleanServiceTest extends ServiceTestBase {

    private static final String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String PROJECT_ID = "a8f4da94-a8a4-464b-ab6f-b3012aba04d5";

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
        val aclManager = ACLManager.getInstance(getTestConfig());
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val record = new AclRecord();
        record.setUuid(UUID.randomUUID().toString());

        //project not exist
        val info = new ObjectIdentityImpl("project", PROJECT_ID);
        record.setDomainObjectInfo(info);
        aclManager.save(record);

        val acl = aclManager.get(PROJECT_ID);
        Assert.assertNotNull(acl);
        val model = modelMgr.getDataModelDesc(MODEL_ID);
        val cube = indePlanManager.getIndexPlan(MODEL_ID);
        val layoutIds = cube.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> copyForWrite.setSemanticVersion(2));

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val mocks = new HashSet<FavoriteQuery>();
        for (int i = 0; i < 3 * 8; i++) {
            val fq = new FavoriteQuery("sql" + i);
            fq.setTotalCount(1);
            fq.setSuccessCount(1);
            fq.setLastQueryTime(10000 + i);
            fq.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);

            val fqr = new FavoriteQueryRealization();
            fqr.setModelId(model.getId());
            fqr.setSemanticVersion(i % 3);
            fqr.setLayoutId(layoutIds.get(i % 8));

            val fqr2 = new FavoriteQueryRealization();
            fqr2.setModelId(model.getId());
            fqr2.setSemanticVersion(2);
            fqr2.setLayoutId(40002);
            fq.setRealizations(Lists.newArrayList(fqr, fqr2));
            mocks.add(fq);
            log.debug("prepare {} {}", fq, fqr2);
        }
        favoriteQueryManager.create(mocks);

        // add some new layouts for cube
        indePlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            val newDesc = new IndexEntity();
            newDesc.setId(40000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(100000, 100001, 100005));
            val layout = new LayoutEntity();
            layout.setId(40001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100005));
            layout.setAuto(true);
            newDesc.setLayouts(Lists.newArrayList(layout));
            val layout3 = new LayoutEntity();
            layout3.setId(40002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100000, 100001, 100005));
            layout3.setAuto(true);
            newDesc.setLayouts(Lists.newArrayList(layout3));

            val newDesc2 = new IndexEntity();
            newDesc2.setId(IndexEntity.TABLE_INDEX_START_ID + 40000);
            newDesc2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            val layout2 = new LayoutEntity();
            layout2.setId(IndexEntity.TABLE_INDEX_START_ID + 40001);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            layout2.setManual(true);
            newDesc2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.getIndexes().add(newDesc);
            copyForWrite.getIndexes().add(newDesc2);
        });
    }

    @Test
    public void testCleanup() throws Exception {
        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indePlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), LayoutEntity::equals, true, false);
        });
        gcService.clean();


        val acl = ACLManager.getInstance(getTestConfig()).get(PROJECT_ID);
        Assert.assertNull(acl);

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
                    Assert.assertTrue(realization.getSemanticVersion() == 2 && realization.getLayoutId() != 1L);
                    usedIds.add(realization.getLayoutId());
                }
            }
        }
        Assert.assertEquals(17, waitingSize);

        val dataflowMgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val df = dataflowMgr.getDataflow(MODEL_ID);

        for (NDataSegment segment : df.getSegments()) {
            val cuboids = segment.getSegDetails().getLayouts();
            val ids = cuboids.stream().map(NDataLayout::getLayoutId).collect(Collectors.toList());
            Assert.assertFalse(ids.contains(1L));
        }

        // check remove useless layouts
        val cube = indePlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertNull(cube.getCuboidLayout(40001L));
        Assert.assertFalse(cube.getCuboidLayout(IndexEntity.TABLE_INDEX_START_ID + 40001L).isAuto());
        Assert.assertEquals(9, cube.getAllLayouts().size());
    }

    @Test
    public void testCleanupBroken() throws Exception {
        val projectName = "broken_test";
        val project = NProjectManager.getInstance(getTestConfig()).getProject(projectName);
        gcService.cleanupProject(project);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), projectName);
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels(true).size());
    }

}
