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

package io.kyligence.kap.engine.mr;

import io.kyligence.kap.cube.ColumnIndexFactory;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.File;
import java.io.IOException;

/**
 * Created by shishaofeng on 4/9/16.
 */
public class ColumnIndexWriter {

    private int colOffset;
    private int colLength;
    private IColumnForwardIndex.Builder forwardIndexBuilder;
    private IColumnInvertedIndex.Builder invertedIndexBuilder;

    public ColumnIndexWriter(CubeSegment cubeSegment, TblColRef col, int offset, int length, File fwdIdx, File ivdIdx) {
        this.colOffset = offset;
        this.colLength = length;
        forwardIndexBuilder = ColumnIndexFactory.createLocalForwardIndex(cubeSegment, col, fwdIdx.getAbsolutePath()).rebuild();
        invertedIndexBuilder = ColumnIndexFactory.createLocalInvertedIndex(cubeSegment, col, ivdIdx.getAbsolutePath()).rebuild();
    }

    public void write(byte[] bytes) throws IOException {
        int value = BytesUtil.readUnsigned(bytes, colOffset, colLength);
        //write the value to the index files
        forwardIndexBuilder.putNextRow(value);
        invertedIndexBuilder.putNextRow(value);
    }

    public void close() throws IOException {
        forwardIndexBuilder.close();
        invertedIndexBuilder.close();
    }
}
