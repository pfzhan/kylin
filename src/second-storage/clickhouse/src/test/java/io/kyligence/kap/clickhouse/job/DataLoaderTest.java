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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataLoaderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void clickHouseType() {
        Assert.assertEquals("UInt8", DataLoader.clickHouseType(DataType.getType(DataType.BOOLEAN)));
        Assert.assertEquals("Int8", DataLoader.clickHouseType(DataType.getType(DataType.BYTE)));

        Assert.assertEquals("Int8", DataLoader.clickHouseType(DataType.getType(DataType.TINY_INT)));

        Assert.assertEquals("Int16", DataLoader.clickHouseType(DataType.getType(DataType.SHORT)));
        Assert.assertEquals("Int16", DataLoader.clickHouseType(DataType.getType(DataType.SMALL_INT)));

        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INT)));
        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INT4)));
        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INTEGER)));

        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.LONG)));
        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.LONG8)));
        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.BIGINT)));

        Assert.assertEquals("Float32", DataLoader.clickHouseType(DataType.getType(DataType.FLOAT)));
        Assert.assertEquals("Float64", DataLoader.clickHouseType(DataType.getType(DataType.DOUBLE)));

        Assert.assertEquals("Decimal(19,4)", DataLoader.clickHouseType(DataType.getType(DataType.DECIMAL)));
        Assert.assertEquals("Decimal(19,4)", DataLoader.clickHouseType(DataType.getType(DataType.NUMERIC)));

        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.VARCHAR)));
        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.CHAR)));
        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.STRING)));

        Assert.assertEquals("Date", DataLoader.clickHouseType(DataType.getType(DataType.DATE)));

        Assert.assertEquals("DateTime", DataLoader.clickHouseType(DataType.getType(DataType.DATETIME)));
        Assert.assertEquals("DateTime", DataLoader.clickHouseType(DataType.getType(DataType.TIMESTAMP)));

    }
}