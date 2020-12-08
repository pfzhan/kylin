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
package org.apache.kylin.sdk.datasource.framework.def;

import java.sql.Types;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class DataSourceDefTest extends NLocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef defaultDef = provider.getDefault();

        Assert.assertNotNull(defaultDef);
        Assert.assertEquals("default", defaultDef.getId());

        // functions
        List<String> funcDefU = defaultDef.getFuncDefsByName("NULLIF");
        List<String> funcDefL = defaultDef.getFuncDefsByName("nullif");
        Assert.assertEquals(funcDefL, funcDefU);
        Assert.assertFalse(funcDefU.isEmpty());
        Assert.assertNotNull(defaultDef.getFuncDefSqlNode(funcDefU.get(0)));

        // types
        List<TypeDef> typeDefU = defaultDef.getTypeDefsByName("STRING");
        List<TypeDef> typeDefL = defaultDef.getTypeDefsByName("string");
        Assert.assertEquals(typeDefU, typeDefL);
        Assert.assertFalse(typeDefL.isEmpty());
        Assert.assertNotNull(defaultDef.getTypeDef(typeDefL.get(0).getId()));

        // properties
        Assert.assertNotNull(defaultDef.getPropertyValue("sql.default-converted-enabled", null));
        Assert.assertNull(defaultDef.getPropertyValue("invalid-key", null));

        // dataTypeMappings
        DataSourceDef testingDsDef = provider.getById("testing");

        Assert.assertEquals(Types.VARCHAR, (int) testingDsDef.getDataTypeValue("CHARACTER VARYING"));
        Assert.assertEquals(Types.DOUBLE, (int) testingDsDef.getDataTypeValue("DOUBLE PRECISION"));
        Assert.assertEquals(Types.DOUBLE, (int) testingDsDef.getDataTypeValue("double precision"));
    }

    @Test
    public void testOverrideXml() {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef defaultDef = provider.getDefault();
        Assert.assertEquals("true", defaultDef.getPropertyValue("metadata.enable-cache", null)); //in default.xml is false,but in default.xml.override is true
    }
}
