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

package io.kyligence.kap.smart.cube.proposer;

import org.apache.kylin.common.KylinConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class ProposerProviderTest {
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
    public void testGetAggrGroupProposer() {
        CubeContext ctx = new CubeContext(kylinConfig);
        ProposerProvider provider = ProposerProvider.create(ctx);

        System.setProperty("kap.smart.conf.aggGroup.strategy", "default");
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof ModelAggrGroupProposer);

        System.setProperty("kap.smart.conf.aggGroup.strategy", "mixed");
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof MixedAggrGroupProposer);

        System.setProperty("kap.smart.conf.aggGroup.strategy", "whitelist");
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof QueryAggrGroupProposer);

        System.setProperty("kap.smart.conf.aggGroup.strategy", "auto");

        ctx.setModelStats(null);
        ctx.setQueryStats(null);
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof ModelAggrGroupProposer);

        ctx.setModelStats(new ModelStats());
        ctx.setQueryStats(null);
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof ModelAggrGroupProposer);

        ctx.setModelStats(null);
        ctx.setQueryStats(new QueryStats());
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof QueryAggrGroupProposer);

        ctx.setModelStats(new ModelStats());
        ctx.setQueryStats(new QueryStats());
        Assert.assertTrue(provider.getAggrGroupProposer() instanceof MixedAggrGroupProposer);
    }
}
