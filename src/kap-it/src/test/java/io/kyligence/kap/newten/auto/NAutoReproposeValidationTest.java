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

package io.kyligence.kap.newten.auto;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.val;

public class NAutoReproposeValidationTest extends NAutoTestBase {

    /**
     * Validate proposed layouts change with table stats. First round without stats, second round with stats
     */
    @Test
    public void testReproposeChangedWithTableStats() throws Exception {
        val firstRoundProposed = new TestScenario(CompareLevel.SAME, "sql", 35, 36).execute(true);

        // set 'kap.smart.conf.rowkey.uhc.min-cardinality' = 50 to test
        getTestConfig().setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", "50");
        val secondRoundProposed = new TestScenario(CompareLevel.SAME, "sql", 35, 36).execute(true);

        firstRoundProposed.forEach((sql, entity) -> {
            Assert.assertFalse(entity.getAccelerateLayouts()
                    .equalsIgnoreCase(secondRoundProposed.get(sql).getAccelerateLayouts()));
        });
        final long rowkeyUHCCardinalityMin = SmartConfig.getInstanceFromEnv().getRowkeyUHCCardinalityMin();
        getTestConfig().setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", rowkeyUHCCardinalityMin + "");
    }

    @Test
    public void testProposedLayoutConsistency() throws Exception {

        new TestScenario(CompareLevel.SAME, "auto/repropose_sql").execute();

        final List<String> sqlList = collectQueries(new TestScenario(CompareLevel.SAME, "auto/repropose_sql"));
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqlList.toArray(new String[0]));
        smartMaster.runAll();
        final Map<String, AccelerateInfo> secondRoundMap = smartMaster.getContext().getAccelerateInfoMap();
        final Set<String> secondRoundProposedColOrders = collectColOrders(secondRoundMap.values());

        // Suggested layouts should be independent of the order of input sqls
        {
            Collections.shuffle(sqlList); // shuffle and propose
            smartMaster = new NSmartMaster(kylinConfig, getProject(), sqlList.toArray(new String[0]));
            smartMaster.runAll();
            final Map<String, AccelerateInfo> thirdRoundMap = smartMaster.getContext().getAccelerateInfoMap();
            final Set<String> thirdRoundProposedColOrders = collectColOrders(thirdRoundMap.values());
            Assert.assertTrue(Objects.equals(secondRoundProposedColOrders, thirdRoundProposedColOrders));
        }

        // Suggested layouts should be independent of modeling by a single batch or multi-batches
        {
            List<String> batchOne = sqlList.subList(0, sqlList.size() / 2);
            smartMaster = new NSmartMaster(kylinConfig, getProject(), batchOne.toArray(new String[0]));
            smartMaster.runAll();
            final Map<String, AccelerateInfo> batchOneMap = smartMaster.getContext().getAccelerateInfoMap();

            List<String> batchTwo = sqlList.subList(sqlList.size() / 2, sqlList.size());
            smartMaster = new NSmartMaster(kylinConfig, getProject(), batchTwo.toArray(new String[0]));
            smartMaster.runAll();
            final Map<String, AccelerateInfo> batchTwoMap = smartMaster.getContext().getAccelerateInfoMap();

            Set<String> batchProposedColOrders = Sets.newHashSet();
            batchProposedColOrders.addAll(collectColOrders(batchOneMap.values()));
            batchProposedColOrders.addAll(collectColOrders(batchTwoMap.values()));
            Assert.assertTrue(Objects.equals(secondRoundProposedColOrders, batchProposedColOrders));
        }
    }

    private Set<String> collectColOrders(Collection<AccelerateInfo> accelerateInfoList) {
        Set<String> allProposedColOrder = Sets.newHashSet();
        accelerateInfoList.forEach(accelerateInfo -> {
            final Set<AccelerateInfo.QueryLayoutRelation> layouts = accelerateInfo.getRelatedLayouts();
            Set<String> colOrders = RecAndQueryCompareUtil.collectAllColOrders(kylinConfig, getProject(), layouts);
            allProposedColOrder.addAll(colOrders);
        });
        return allProposedColOrder;
    }
}
