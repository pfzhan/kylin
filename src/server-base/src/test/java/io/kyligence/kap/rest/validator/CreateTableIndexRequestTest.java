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
package io.kyligence.kap.rest.validator;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import lombok.val;

public class CreateTableIndexRequestTest extends NLocalFileMetadataTestCase {

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        val cubeManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        val cube = cubeManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();
            val newCuboid = new IndexEntity();
            newCuboid.setDimensions(Lists.newArrayList(1, 2, 3));
            newCuboid.setId(copyForWrite.getNextTableIndexId());
            val layout1 = new LayoutEntity();
            layout1.setId(newCuboid.getId() + 1);
            layout1.setName("index1");
            layout1.setColOrder(Lists.newArrayList(1, 2, 3));
            layout1.setManual(true);
            newCuboid.setLayouts(Lists.newArrayList(layout1));

            cuboids.add(newCuboid);
            copyForWrite.setIndexes(cuboids);
        });

        val req = CreateTableIndexRequest.builder().project(cube.getProject()).modelId(cube.getUuid()).name("index1")
                .build();
        Assert.assertTrue(req.isNameExisting());

        req.setId(cube.getNextTableIndexId() - IndexEntity.INDEX_ID_STEP + 1);
        Assert.assertFalse(req.isNameExisting());
    }

}
