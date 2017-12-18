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

import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;

public class NCubeMasterTest extends NTestBase {
    @Test
    public void test() {
        TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(kylinConfig);
        NDataModelManager dataModelManager = (NDataModelManager) DataModelManager.getInstance(kylinConfig);
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig);
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("kylin_sales", proj);

        NSmartContext.NModelContext mdCtx = getModelContext();
        Assert.assertNotNull(mdCtx);

        NCubeMaster cubeMaster = new NCubeMaster(mdCtx);

        // propose initial cube
        NCubePlan cubePlan = cubeMaster.proposeInitialCube();
        Assert.assertNotNull(cubePlan);
    }

    private NSmartContext.NModelContext getModelContext() {
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
        NSmartContext ctx = smartMaster.getContext();
        return ctx.getModelContexts().get(0);
    }
}
