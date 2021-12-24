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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.source.hive;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HiveMetadataExplorerTest extends NLocalFileMetadataTestCase {

    @Test
    public void testCheckIsRangePartitionTable() throws Exception {
        List<HiveTableMeta.HiveTableColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("B", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("C", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("D", "varchar", ""));
        Assert.assertEquals(Boolean.FALSE, columnMetas.stream().collect(Collectors.groupingBy(p -> p.name)).values()
                .stream().anyMatch(p -> p.size() > 1));
        HiveMetadataExplorer hiveMetadataExplorer = Mockito.mock(HiveMetadataExplorer.class);
        Mockito.when(hiveMetadataExplorer.checkIsRangePartitionTable(Mockito.anyList())).thenCallRealMethod();
        Assert.assertEquals(Boolean.FALSE, hiveMetadataExplorer.checkIsRangePartitionTable(columnMetas));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        Assert.assertEquals(Boolean.TRUE, columnMetas.stream().collect(Collectors.groupingBy(p -> p.name)).values()
                .stream().anyMatch(p -> p.size() > 1));
        Assert.assertEquals(Boolean.TRUE, hiveMetadataExplorer.checkIsRangePartitionTable(columnMetas));
    }

    @Test
    public void testGetColumnDescs() {
        createTestMetadata();
        HiveMetadataExplorer hiveMetadataExplorer = Mockito.mock(HiveMetadataExplorer.class);
        Mockito.when(hiveMetadataExplorer.getColumnDescs(Mockito.anyList())).thenCallRealMethod();
        List<HiveTableMeta.HiveTableColumnMeta> columnMetas = new ArrayList<>();
        List<ColumnDesc> columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertNotNull(columnDescs);
        Assert.assertEquals(0, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("B", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("C", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("D", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(4, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(4, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("E", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(5, columnDescs.size());
    }
}
