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

package io.kyligence.kap.modeling.smart.proposer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.ModelingContextBuilder;
import io.kyligence.kap.query.mockup.Utils;

public class DerivedDimensionProposerTest {
    private static KylinConfig kylinConfig = Utils.newKylinConfig("src/test/resources/learn_kylin/meta");

    @BeforeClass
    public static void beforeClass() {
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
    }

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testOnStarModel() throws JsonProcessingException {
        DataModelDesc modelDesc = MetadataManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model_star");
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromModelDesc(modelDesc, new String[0]);

        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        Assert.assertEquals(23, initCubeDesc.getDimensions().size());

        DerivedDimensionProposer proposer = new DerivedDimensionProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        Assert.assertEquals(18, newCubeDesc.getDimensions().size());
    }

    @Test
    public void testOnSnowModel() throws JsonProcessingException {
        DataModelDesc modelDesc = MetadataManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model");
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromModelDesc(modelDesc, new String[0]);

        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        Assert.assertEquals(25, initCubeDesc.getDimensions().size());

        DerivedDimensionProposer proposer = new DerivedDimensionProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        Assert.assertEquals(18, newCubeDesc.getDimensions().size());
    }
}
