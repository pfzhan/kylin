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
package io.kyligence.kap.metadata.cube.cuboid;

import java.io.IOException;

import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NAggregationGroupTest extends NLocalFileMetadataTestCase {

    private static final String AGG_GROUP = "{\"includes\":[0,1,2,3,4],\"select_rule\":{\"mandatory_dims\":[0,1],\"hierarchy_dims\":[],\"joint_dims\":[]}}";


    @Before
    public void init() {
        this.createTestMetadata();
    }

    @After
    public void cleanup() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCalculateCuboidCombinationWithZeroDimcap() throws IOException {
        NAggregationGroup group = JsonUtil.readValue(AGG_GROUP, NAggregationGroup.class);
        group.ruleBasedAggIndex = new NRuleBasedIndex();
        Assert.assertEquals(7, group.calculateCuboidCombination());
    }

}