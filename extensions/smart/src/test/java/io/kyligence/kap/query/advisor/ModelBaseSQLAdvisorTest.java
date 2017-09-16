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
package io.kyligence.kap.query.advisor;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.query.advisor.ModelBasedSQLAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;

public class ModelBaseSQLAdvisorTest extends TestBase {
    @Test
    public void testModelAdvice() {
        String modelName = "lineitem_model";
        String successSql =
                "select\n" +
                        "    l_returnflag,\n" +
                        "    l_linestatus,\n" +
                        "    sum(l_quantity) as sum_qty,\n" +
                        "    sum(l_extendedprice) as sum_base_price,\n" +
                        "    sum(l_saleprice) as sum_disc_price,\n" +
                        "    sum(l_saleprice) + sum(l_taxprice) as sum_charge,\n" +
                        "    avg(l_quantity) as avg_qty,\n" +
                        "    avg(l_extendedprice) as avg_price,\n" +
                        "    avg(l_discount) as avg_disc,\n" +
                        "    count(*) as count_order\n" +
                        "from\n" +
                        "    v_lineitem\n" +
                        "where\n" +
                        "    l_shipdate <= '1998-09-16'\n" +
                        "group by\n" +
                        "    l_returnflag,\n" +
                        "    l_linestatus\n" +
                        "order by\n" +
                        "    l_returnflag,\n" +
                        "    l_linestatus;";
        String failSql =
                "select\n" +
                        "    sum(l_saleprice) as revenue\n" +
                        "from\n" +
                        "    v_lineitem\n" +
                        "    left join part on l_partkey = p_partkey\n" +
                        "where\n" +
                        "    (\n" +
                        "        p_brand = 'Brand#32'\n" +
                        "        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n" +
                        "        and l_quantity >= 7 and l_quantity <= 7 + 10\n" +
                        "        and p_size between 1 and 5\n" +
                        "        and l_shipmode in ('AIR', 'AIR REG')\n" +
                        "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
                        "    )\n" +
                        "    or\n" +
                        "    (\n" +
                        "        p_brand = 'Brand#35'\n" +
                        "        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
                        "        and l_quantity >= 15 and l_quantity <= 15 + 10\n" +
                        "        and p_size between 1 and 10\n" +
                        "        and l_shipmode in ('AIR', 'AIR REG')\n" +
                        "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
                        "    )\n" +
                        "    or\n" +
                        "    (\n" +
                        "        p_brand = 'Brand#24'\n" +
                        "        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
                        "        and l_quantity >= 26 and l_quantity <= 26 + 10\n" +
                        "        and p_size between 1 and 15\n" +
                        "        and l_shipmode in ('AIR', 'AIR REG')\n" +
                        "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
                        "    );";
        DataModelDesc modelDesc = getDataModelDesc(modelName);

        ModelBasedSQLAdvisor modelBasedSQLAdvisor = new ModelBasedSQLAdvisor(modelDesc);

        setModelQueryResult(modelName, successSql);
        List<SQLAdvice> sqlAdvices = modelBasedSQLAdvisor.provideAdvice(sqlResult, olapContexts);
        Assert.assertTrue(CollectionUtils.isEmpty(sqlAdvices));
        setModelQueryResult(modelName, failSql);
        sqlAdvices = modelBasedSQLAdvisor.provideAdvice(sqlResult, olapContexts);
        Assert.assertTrue(CollectionUtils.isNotEmpty(sqlAdvices));
    }
}
