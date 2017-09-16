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

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationCheck;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.advisor.ModelBasedSQLAdviceProposer;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;

public class ModelBaseSQLAdviceProposerTest extends TestBase {
    @Test
    public void testModelAdviceProposer() {
        String modelName = "lineitem_model";
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
        DataModelDesc dataModelDesc = getDataModelDesc(modelName);
        setModelQueryResult(modelName, failSql);
        ModelBasedSQLAdviceProposer modelBaseSQLAdviceProposer = new ModelBasedSQLAdviceProposer(dataModelDesc);
        List<OLAPContext> olapContexts = Lists.newArrayList(this.olapContexts);
        List<RealizationCheck.IncapableReason> incapableReasons = olapContexts.get(0).realizationCheck.getModelIncapableReasons().get(dataModelDesc);
        SQLAdvice advice = modelBaseSQLAdviceProposer.propose(incapableReasons.get(0), olapContexts.get(0));
        Assert.assertTrue(advice != null && advice.getIncapableReason() != null && advice.getSuggestion() != null);
    }
}
