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

package io.kyligence.kap.smart;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.smart.common.AutoTestOnLearnKylinData;
import io.kyligence.kap.smart.index.IndexMaster;

public class IndexMasterTest extends AutoTestOnLearnKylinData {

    @Test
    public void test() {
        AbstractContext.ModelContext mdCtx = getModelContext();
        Assert.assertNotNull(mdCtx);

        IndexMaster indexMaster = new IndexMaster(mdCtx);

        // propose initial cube
        IndexPlan indexPlan = indexMaster.proposeInitialIndexPlan();
        {
            Assert.assertNotNull(indexPlan);
            Assert.assertTrue(indexPlan.getAllIndexes().isEmpty());
        }

        indexPlan = indexMaster.proposeCuboids(indexPlan);
        {
            List<IndexEntity> indexEntities = indexPlan.getIndexes();
            Assert.assertEquals(4, indexEntities.size());

            for (IndexEntity c : indexEntities) {
                if (c.getLayouts().size() == 2) {
                    Assert.assertFalse(c.isTableIndex());
                    Assert.assertEquals(2, c.getDimensions().size());
                    Assert.assertEquals(2, c.getMeasures().size());
                    Assert.assertSame(indexPlan, c.getIndexPlan());

                    LayoutEntity c11 = c.getLayouts().get(0);
                    Assert.assertSame(c11.getIndex(), c);
                    Assert.assertEquals(4, c11.getColOrder().size());

                    LayoutEntity c12 = c.getLayouts().get(1);
                    Assert.assertSame(c12.getIndex(), c);
                    Assert.assertEquals(4, c12.getColOrder().size());

                } else if (c.getLayouts().size() == 1 && c.getMeasures().size() > 0) {
                    Assert.assertFalse(c.isTableIndex());
                    Assert.assertEquals(1, c.getDimensions().size());
                    Assert.assertEquals(2, c.getMeasures().size());
                    Assert.assertSame(indexPlan, c.getIndexPlan());

                    LayoutEntity c21 = c.getLayouts().get(0);
                    Assert.assertSame(c21.getIndex(), c);
                    Assert.assertEquals(3, c21.getColOrder().size());
                    Assert.assertEquals(new Integer(7), c21.getColOrder().get(0));
                    Assert.assertEquals("eq", c21.getColIndexType(0));

                } else if (c.getLayouts().size() == 1 && c.getDimensions().size() == 4) {
                    Assert.assertTrue(c.isTableIndex());
                    Assert.assertEquals(0, c.getMeasures().size());
                    Assert.assertSame(indexPlan, c.getIndexPlan());

                    LayoutEntity c31 = c.getLayouts().get(0);
                    Assert.assertSame(c31.getIndex(), c);
                    Assert.assertEquals(4, c31.getColOrder().size());
                    Assert.assertEquals(new Integer(7), c31.getColOrder().get(0));
                    Assert.assertEquals("eq", c31.getColIndexType(0));

                } else if (c.getLayouts().size() == 1) {
                    Assert.assertTrue(c.isTableIndex());
                    Assert.assertEquals(3, c.getDimensions().size());
                    Assert.assertEquals(0, c.getMeasures().size());
                    Assert.assertSame(indexPlan, c.getIndexPlan());

                    LayoutEntity c41 = c.getLayouts().get(0);
                    Assert.assertSame(c41.getIndex(), c);
                    Assert.assertEquals(3, c41.getColOrder().size());
                    Assert.assertEquals(new Integer(7), c41.getColOrder().get(0));
                    Assert.assertEquals("eq", c41.getColIndexType(0));

                } else {
                    throw new IllegalStateException("Should not come here");
                }
            }
        }

        // propose again, should return same result
        IndexPlan cp2 = indexMaster.proposeCuboids(indexPlan);
        Assert.assertEquals(cp2, indexPlan);
    }

    private AbstractContext.ModelContext getModelContext() {
        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                // follow items for table index
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'",
                "select lstg_format_name, part_dt, price from kylin_sales where part_dt = '2012-01-01'",
                "select lstg_format_name, part_dt, price, item_count from kylin_sales where part_dt = '2012-01-01'" };

        SmartContext context = new SmartContext(getTestConfig(), proj, sqls) {
            @Override
            public ChainedProposer createProposers() {
                ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                        new SQLAnalysisProposer(this), //
                        new ModelOptProposer(this));
                return new ChainedProposer(this, proposers);
            }
        };
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.executePropose();
        return context.getModelContexts().get(0);
    }
}
