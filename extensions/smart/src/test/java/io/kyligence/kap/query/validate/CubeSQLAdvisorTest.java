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
package io.kyligence.kap.query.validate;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.validator.SQLValidateResult;

public class CubeSQLAdvisorTest extends TestBase {
    @Test
    public void testBadSQL() {
        List<String> sqls = Lists.newArrayList("select a,b,c,d from t");
        Map<String, SQLValidateResult> validateResultMap = validateCube("src/test/resources/smart/tpch/meta",
                "lineitem_cube", sqls);

        SQLValidateResult sqlValidateResult = validateResultMap.get(sqls.get(0));
        Assert.assertTrue(validateResultMap != null && sqlValidateResult.isCapable() == false
                && CollectionUtils.isNotEmpty(sqlValidateResult.getSQLAdvices()));
    }

    @Test
    public void testColumnNotFound() {
        String sql = "with tmp3 as (\n"
                + "    select sum(l_partkey), l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey\n"
                + "    from v_lineitem\n" + "    inner join supplier on l_suppkey = S_SUPPKEY\n"
                + "    inner join nation on s_nationkey = n_nationkey\n"
                + "    inner join part on l_partkey = p_partkey\n"
                + "    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'\n"
                + "    and n_name = 'CANADA'\n" + "    and p_name like 'forest%' and L_SALEPRICE = 'test'\n"
                + "    group by l_partkey, l_suppkey\n" + ")\n" + "\n" + "select\n" + "    s_name,\n"
                + "    s_address\n" + "from\n" + "    v_partsupp\n"
                + "    inner join supplier on ps_suppkey = s_suppkey\n"
                + "    inner join tmp3 on ps_partkey = l_partkey and ps_suppkey = l_suppkey\n" + "where\n"
                + "    ps_availqty > sum_quantity\n" + "group by\n" + "    s_name, s_address\n" + "order by\n"
                + "    s_name;";
        List<String> sqls = Lists.newArrayList(sql);
        Map<String, SQLValidateResult> validateResultMap = validateCube("src/test/resources/smart/tpch/meta",
                "lineitem_cube", sqls);

        SQLValidateResult sqlValidateResult = validateResultMap.get(sqls.get(0));
        Assert.assertTrue(validateResultMap != null && sqlValidateResult.isCapable() == false
                && CollectionUtils.isNotEmpty(sqlValidateResult.getSQLAdvices()));
    }

    @Test
    public void testDimensionUnmatched() {
        String sql = "select\n" + "    l_returnflag,\n" + "    l_linestatus,\n" + "    sum(l_quantity) as sum_qty,\n"
                + "    sum(l_extendedprice) as sum_base_price,\n" + "    sum(l_saleprice) as sum_disc_price,\n"
                + "    sum(l_saleprice) + sum(l_taxprice) as sum_charge,\n" + "    avg(l_quantity) as avg_qty,\n"
                + "    avg(l_extendedprice) as avg_price,\n" + "    avg(l_discount) as avg_disc,\n"
                + "    count(*) as count_order\n" + "from\n" + "    v_lineitem\n" + "where\n"
                + "    l_shipdate <= '1998-09-16' and L_SALEPRICE > 100\n" + "group by\n" + "    l_returnflag,\n"
                + "    l_linestatus\n" + "order by\n" + "    l_returnflag,\n" + "    l_linestatus;";
        List<String> sqls = Lists.newArrayList(sql);
        Map<String, SQLValidateResult> validateResultMap = validateCube("src/test/resources/smart/tpch/meta",
                "lineitem_cube", sqls);

        SQLValidateResult sqlValidateResult = validateResultMap.get(sqls.get(0));
        Assert.assertTrue(validateResultMap != null && sqlValidateResult.isCapable() == false
                && CollectionUtils.isNotEmpty(sqlValidateResult.getSQLAdvices()));
    }

    @Test
    public void testMeasureUnmatched() {
        String sql = "select\n" + "    l_returnflag,\n" + "    l_linestatus,\n" + "    sum(l_quantity) as sum_qty,\n"
                + "    sum(l_extendedprice) as sum_base_price,\n" + "    sum(l_saleprice) as sum_disc_price,\n"
                + "    count(distinct l_saleprice) + count(l_taxprice) as sum_charge,\n"
                + "    avg(l_quantity) as avg_qty,\n" + "    avg(l_extendedprice) as avg_price,\n"
                + "    avg(l_discount) as avg_disc,\n" + "    count(*) as count_order\n" + "from\n" + "    v_lineitem\n"
                + "where\n" + "    l_shipdate <= '1998-09-16'\n" + "group by\n" + "    l_returnflag,\n"
                + "    l_linestatus\n" + "order by\n" + "    l_returnflag,\n" + "    l_linestatus;";
        List<String> sqls = Lists.newArrayList(sql);
        Map<String, SQLValidateResult> validateResultMap = validateCube("src/test/resources/smart/tpch/meta",
                "lineitem_cube", sqls);

        SQLValidateResult sqlValidateResult = validateResultMap.get(sqls.get(0));
        Assert.assertTrue(validateResultMap != null && sqlValidateResult.isCapable() == false
                && CollectionUtils.isNotEmpty(sqlValidateResult.getSQLAdvices()));
    }

    @Test
    public void testOtherCubeFail() {
        String sql = "with tmp3 as (\n" + "    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey\n"
                + "    from v_lineitem\n" + "    left join supplier on l_suppkey = s_suppkey\n"
                + "    inner join nation on s_nationkey = n_nationkey\n"
                + "    inner join part on l_partkey = p_partkey\n"
                + "    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'\n"
                + "    and n_name = 'CANADA'\n" + "    and p_name like 'forest%'\n"
                + "    group by l_partkey, l_suppkey\n" + ")\n" + "\n" + "select\n" + "    s_name,\n"
                + "    s_address\n" + "from\n" + "    v_partsupp\n"
                + "    inner join supplier on ps_suppkey = s_suppkey\n"
                + "    inner join tmp3 on ps_partkey = l_partkey and ps_suppkey = l_suppkey\n" + "where\n"
                + "    ps_availqty > sum_quantity\n" + "group by\n" + "    s_name, s_address\n" + "order by\n"
                + "    s_name;";
        List<String> sqls = Lists.newArrayList(sql);
        Map<String, SQLValidateResult> validateResultMap = validateCube("src/test/resources/smart/tpch/meta",
                "partsupp_cube", sqls);

        SQLValidateResult sqlValidateResult = validateResultMap.get(sqls.get(0));
        Assert.assertTrue(validateResultMap != null && sqlValidateResult.isCapable() == false
                && CollectionUtils.isNotEmpty(sqlValidateResult.getSQLAdvices()));
    }
}
