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

import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import org.junit.Assert;
import org.junit.Test;
import io.kyligence.kap.smart.NSmartMaster;

import java.util.Map;

public class NSmartSQLFunctionTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testNotSupportedIntersectCount() {
        String[] sqls = new String[]{"select lstg_format_name, " +
                "intersect_count(seller_id, part_dt, array[date '2012-01-01']) first_day, " +
                "intersect_count(seller_id, part_dt, array[date '2012-01-02']) second_day , " +
                "intersect_count(seller_id, part_dt, array[date '2012-01-03']) third_day " +
                "from kylin_sales where part_dt in (date '2010-01-01') group by lstg_format_name limit 1"};
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();

        if(accelerateInfoMap.size() != 0) {
            for (AccelerateInfo accelerateInfo : accelerateInfoMap.values()) {
                Assert.assertTrue(accelerateInfo.isFailed());
                Assert.assertEquals("INTERSECT_COUNT function not supported", accelerateInfo.getFailedCause().getMessage());
            }
        }
    }

}
