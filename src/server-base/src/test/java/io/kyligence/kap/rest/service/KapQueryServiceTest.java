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

package io.kyligence.kap.rest.service;

import java.util.List;

import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class KapQueryServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private KapQueryService kapQueryService = Mockito.spy(new KapQueryService());

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(kapQueryService, "aclEvaluate", aclEvaluate);
    }

    @Test
    public void testSqlsFormat() {
        List<String> sqls = Lists.newArrayList("select * from A", "select A.a, B.b from A join B on A.a2=B.b2",
                "Select sum(a), b from A group by b", "select * from A as c limit 1");

        List<String> expectedFormattedSqls = Lists.newArrayList("SELECT\n  *\nFROM \"A\"",
                "SELECT\n  \"A\".\"A\",\n  \"B\".\"B\"\nFROM \"A\"\n  INNER JOIN \"B\" ON \"A\".\"A2\" = \"B\".\"B2\"",
                "SELECT\n  SUM(\"A\"),\n  \"B\"\nFROM \"A\"\nGROUP BY\n  \"B\"",
                "SELECT\n  *\nFROM \"A\" AS \"C\"\nLIMIT 1");
        val formated = kapQueryService.format(sqls);

        Assert.assertEquals(sqls.size(), formated.size());
        for (int n = 0; n < sqls.size(); n++) {
            Assert.assertEquals(expectedFormattedSqls.get(n), formated.get(n));
        }
    }
}
