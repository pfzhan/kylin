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

package io.kyligence.kap.smart.cube;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NEncodingDesc;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;

public class NCubeMasterTest extends NTestBase {

    @Test
    public void test() throws IOException {
        NSmartContext.NModelContext mdCtx = getModelContext();
        Assert.assertNotNull(mdCtx);

        NCubeMaster cubeMaster = new NCubeMaster(mdCtx);

        // propose initial cube
        NCubePlan cubePlan = cubeMaster.proposeInitialCube();
        {
            Assert.assertNotNull(cubePlan);
            Assert.assertTrue(cubePlan.getCuboids().isEmpty());
            Assert.assertTrue(cubePlan.getCubePlanOverrideEncodings().isEmpty());
        }

        cubePlan = cubeMaster.proposeDimensions(cubePlan);
        {
            Assert.assertNotNull(cubePlan);
            Map<Integer, NEncodingDesc> encs = cubePlan.getCubePlanOverrideEncodings();
            Assert.assertFalse(encs.isEmpty());
            Assert.assertEquals("dict", encs.get(0).getName());
            Assert.assertEquals("date", encs.get(1).getName());
            Assert.assertEquals("dict", encs.get(2).getName());
            Assert.assertEquals("integer:8", encs.get(3).getName());
        }

        // propose again, should return same result
        NCubePlan cp1 = cubeMaster.proposeDimensions(cubePlan);
        Assert.assertEquals(cp1, cubePlan);

        cubePlan = cubeMaster.proposeCuboids(cubePlan);
        {
            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals(2, cuboidDescs.size());

            for (NCuboidDesc c : cuboidDescs) {
                if (c.getLayouts().size() == 2) {
                    Assert.assertEquals(2, c.getDimensions().length);
                    Assert.assertEquals(1, c.getMeasures().length);
                    Assert.assertSame(cubePlan, c.getCubePlan());

                    NCuboidLayout c11 = c.getLayouts().get(0);
                    Assert.assertSame(c11.getCuboidDesc(), c);
                    Assert.assertEquals(3, c11.getColOrder().size());

                    NCuboidLayout c12 = c.getLayouts().get(1);
                    Assert.assertSame(c12.getCuboidDesc(), c);
                    Assert.assertEquals(3, c12.getColOrder().size());
                    Set<String> indexes = Sets.newHashSet();
                    indexes.add(c12.getColIndexType(0));
                    indexes.add(c12.getColIndexType(1));
                    Assert.assertEquals(2, indexes.size());

                } else if (c.getLayouts().size() == 1) {
                    Assert.assertEquals(1, c.getDimensions().length);
                    Assert.assertEquals(2, c.getMeasures().length);
                    Assert.assertSame(cubePlan, c.getCubePlan());

                    NCuboidLayout c21 = c.getLayouts().get(0);
                    Assert.assertSame(c21.getCuboidDesc(), c);
                    Assert.assertEquals(3, c21.getColOrder().size());
                    Assert.assertEquals(new Integer(1), c21.getColOrder().get(0));
                    Assert.assertEquals("eq", c21.getColIndexType(0));
                } else {
                    throw new IllegalStateException("Should not come here");
                }
            }
        }

        // propose again, should return same result
        NCubePlan cp2 = cubeMaster.proposeCuboids(cubePlan);
        Assert.assertEquals(cp2, cubePlan);
    }

    private NSmartContext.NModelContext getModelContext() throws IOException {
        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt" //
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.optimizeModel();
        smartMaster.saveModel();

        NSmartContext ctx = smartMaster.getContext();
        return ctx.getModelContexts().get(0);
    }
}
