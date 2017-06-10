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

package io.kyligence.kap.source.hive.tablestats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class ModelStatsTest {
    public ModelStatsTest() {
    }

    @Test
    public void testErrorMessage() {
        ModelStats modelStats = new ModelStats();
        // Duplication Keys
        List<ModelStats.DuplicatePK> dupPKList = new ArrayList<>();
        ModelStats.DuplicatePK dupPK = new ModelStats.DuplicatePK();
        Map<String, Integer> tmpMap = new HashMap<>();
        tmpMap.put("0", 10);
        tmpMap.put("1", 20);
        dupPK.setDuplication(tmpMap);
        dupPK.setPrimaryKeys("lookup.id");
        dupPK.setLookUpTable("lookup");
        dupPKList.add(dupPK);
        modelStats.setDuplicatePrimaryKeys(dupPKList);
        System.out.println(modelStats.getDuplicationResult());

        // Join Result
        List<ModelStats.JoinResult> joinResults = new ArrayList<>();
        ModelStats.JoinResult joinResult = new ModelStats.JoinResult();
        joinResult.setJoinTableName("lookup1");
        joinResult.setPrimaryKey("lookup1.id");
        joinResult.setJoinResultValidCount(1000);
        joinResult.setFactTableCount(500000);
        joinResults.add(joinResult);

        joinResult = new ModelStats.JoinResult();
        joinResult.setJoinTableName("lookup2");
        joinResult.setPrimaryKey("lookup2.name");
        joinResult.setJoinResultValidCount(100);
        joinResult.setFactTableCount(10000);
        joinResults.add(joinResult);
        modelStats.setJoinResult(joinResults);
        System.out.println(modelStats.getJointResult());

        // Data Skew
        Map<String, List<ModelStats.SkewResult>> skewFks = new HashMap<>();
        List<ModelStats.SkewResult> skewList = new ArrayList<>();
        ModelStats.SkewResult skewResult = new ModelStats.SkewResult();
        skewResult.setDataSkewValue("0");
        skewResult.setDataSkewCount(100000000L);
        skewList.add(skewResult);

        skewResult = new ModelStats.SkewResult();
        skewResult.setDataSkewValue("2");
        skewResult.setDataSkewCount(500000000L);
        skewList.add(skewResult);
        skewFks.put("fact.id", skewList);
        modelStats.setDataSkew(skewFks);
        System.out.println(modelStats.getSkewResult());
    }
}
