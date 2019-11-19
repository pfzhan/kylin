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

package io.kyligence.kap.metadata.cube.model;

import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class EnormousDimTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/enormous_dim");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHierarchy() {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val cube = mgr.getIndexPlanByModelAlias("test_enormous_dim");
        Assert.assertEquals(1, cube.getAllIndexes().size());
        Assert.assertEquals(100, cube.getAllIndexes().get(0).getDimensions().size());
    }

    @Test
    public void testMandatoryDim() {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val cube = mgr.getIndexPlanByModelAlias("test_enormous_dim2");
        List<IndexEntity> allIndexes = cube.getAllIndexes();
        allIndexes.sort(Comparator.comparingInt(o -> o.getDimensions().size()));
        Assert.assertEquals(1023, cube.getAllIndexes().size());
        Assert.assertEquals(10, allIndexes.get(0).getDimensions().size());
        Assert.assertEquals(100, allIndexes.get(allIndexes.size() - 1).getDimensions().size());
    }

    @Test
    public void testJointDim() {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val cube = mgr.getIndexPlanByModelAlias("test_enormous_dim3");
        List<IndexEntity> allIndexes = cube.getAllIndexes();
        allIndexes.sort(Comparator.comparingInt(o -> o.getDimensions().size()));
        Assert.assertEquals(100, cube.getAllIndexes().size());
        Assert.assertEquals(1, allIndexes.get(0).getDimensions().size());
        Assert.assertEquals(100, allIndexes.get(allIndexes.size() - 1).getDimensions().size());
    }
}
