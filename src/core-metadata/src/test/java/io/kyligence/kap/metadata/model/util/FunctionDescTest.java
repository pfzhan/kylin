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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class FunctionDescTest extends NLocalFileMetadataTestCase {

    private static NDataModel model;
    private static final String DF_NAME = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
    private static final String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        model = modelMgr.getDataModelDesc(DF_NAME);
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

    @Test
    public void testRewriteFieldName() {
        FunctionDesc function = FunctionDesc.newInstance("count",
                newParameters(model.findColumn("TRANS_ID")), "bigint");
        Assert.assertEquals("_KY_COUNT_TEST_KYLIN_FACT_TRANS_ID_", function.getRewriteFieldName());

        FunctionDesc function1 = FunctionDesc.newInstance("count", newParameters("1"), "bigint");
        Assert.assertEquals("_KY_COUNT__", function1.getRewriteFieldName());
    }

    @Test
    public void testEquals() {
        FunctionDesc functionDesc = FunctionDesc.newInstance("COUNT", newParameters("1"), "bigint");
        FunctionDesc functionDesc1 = FunctionDesc.newInstance("COUNT", null, null);
        Assert.assertTrue(functionDesc.equals(functionDesc1));
        Assert.assertTrue(functionDesc1.equals(functionDesc));

        FunctionDesc functionDesc2 = FunctionDesc.newInstance("COUNT",
                newParameters(TblColRef.mockup(TableDesc.mockup("test"), 1, "name", null)), "bigint");
        Assert.assertFalse(functionDesc1.equals(functionDesc2));

        FunctionDesc functionDesc3 = FunctionDesc.newInstance("COUNT",
                newParameters(TblColRef.mockup(TableDesc.mockup("test"), 1, "name", null)), "bigint");
        Assert.assertTrue(functionDesc2.equals(functionDesc3));
    }

    @Test
    public void testRewriteFieldType() {
        FunctionDesc cnt1 = FunctionDesc.newInstance("COUNT", newParameters("1"), "bigint");
        Assert.assertEquals(DataType.getType("bigint"), cnt1.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), cnt1.getRewriteFieldType());

        FunctionDesc cnt2 = FunctionDesc.newInstance("COUNT", newParameters("1"), "integer");
        Assert.assertEquals(DataType.getType("integer"), cnt2.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), cnt2.getRewriteFieldType());

        FunctionDesc cnt3 = FunctionDesc.newInstance("COUNT", null, null);
        Assert.assertNull(cnt3.getReturnDataType());
        Assert.assertEquals(DataType.ANY, cnt3.getRewriteFieldType());

        FunctionDesc sum1 = FunctionDesc.newInstance("SUM", newParameters("1"), "bigint");
        Assert.assertEquals(DataType.getType("bigint"), sum1.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), sum1.getRewriteFieldType());

        FunctionDesc max = FunctionDesc.newInstance("MAX",
                newParameters(TblColRef.mockup(null, 0, "col", "integer")), "bigint");
        Assert.assertEquals(DataType.getType("bigint"), max.getReturnDataType());
        Assert.assertEquals(DataType.getType("integer"), max.getRewriteFieldType());
    }

    private static List<ParameterDesc> newParameters(Object... objs) {
        List<ParameterDesc> params = new ArrayList<>();
        for (Object obj : objs) {
            params.add(ParameterDesc.newInstance(obj));
        }
        return params;
    }
}
