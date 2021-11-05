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

package io.kyligence.kap.metadata.model;

import static org.apache.kylin.metadata.datatype.DataType.BOOLEAN;
import static org.apache.kylin.metadata.datatype.DataType.INT;
import static org.apache.kylin.metadata.datatype.DataType.STRING;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.TempMetadataBuilder;

public class NMutiPartitionDescTest {
    private static final String DEFAULT_PROJECT = "default";
    KylinConfig config;
    NDataModelManager mgr;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
        mgr = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
    }

    @Test
    public void testGenerateFormattedValue() {
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(INT), "1").equals("'1'");
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(STRING), "1").equals("'1'");
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(BOOLEAN), "1").equals("cast('1' as boolean)");

    }
}
