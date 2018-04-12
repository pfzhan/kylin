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

package org.apache.kylin.dict.global;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kylin.dict.BytesConverter;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the metadata for a particular version of the global dictionary.
 * Usually each version of a global dictionary stores its metadata in an index file.
 */
public class GlobalDictMetadata {
    public final int baseId;
    public final int maxId;
    public final int maxValueLength;
    public final int nValues;
    public int partitions = -1;
    public final BytesConverter bytesConverter;
    public final TreeMap<AppendDictSliceKey, String> sliceFileMap; // slice key -> slice file name

    public GlobalDictMetadata(int baseId, int maxId, int maxValueLength, int nValues, BytesConverter bytesConverter,
            NavigableMap<AppendDictSliceKey, String> sliceFileMap) {

        Preconditions.checkNotNull(sliceFileMap, "sliceFileMap");

        this.baseId = baseId;
        this.maxId = maxId;
        this.maxValueLength = maxValueLength;
        this.nValues = nValues;
        this.bytesConverter = bytesConverter;
        this.sliceFileMap = new TreeMap<>(sliceFileMap);
    }

    public GlobalDictMetadata(int baseId, int maxId, int maxValueLength, int nValues, int partitions,
            BytesConverter bytesConverter, NavigableMap<AppendDictSliceKey, String> sliceFileMap) {
        this(baseId, maxId, maxValueLength, nValues, bytesConverter, sliceFileMap);
        this.partitions = partitions;
    }
}
