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
package io.kyligence.kap.metadata.model.util;

import java.util.HashMap;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class FunctionDescTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @Test
    public void testProposeReturnType() {
        Assert.assertEquals("bigint", FunctionDesc.proposeReturnType("COUNT", "int"));
        Assert.assertEquals("topn(100, 4)", FunctionDesc.proposeReturnType("TOP_N", "int"));
        Assert.assertEquals("bigint", FunctionDesc.proposeReturnType("SUM", "tinyint"));
        Assert.assertEquals("decimal(19,4)", FunctionDesc.proposeReturnType("SUM", null));
        Assert.assertEquals("decimal(19,4)", FunctionDesc.proposeReturnType("SUM", "decimal"));
        Assert.assertEquals("decimal(19,2)", FunctionDesc.proposeReturnType("SUM", "decimal(19,2)"));
        Assert.assertEquals("hllc(10)",
                FunctionDesc.proposeReturnType("COUNT_DISTINCT", "int", new HashMap<String, String>() {
                    {
                        put("COUNT_DISTINCT", "hllc(10)");
                    }
                }));
    }
}
