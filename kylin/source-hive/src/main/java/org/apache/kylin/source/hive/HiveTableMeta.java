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

import java.util.List;

class HiveTableMeta {
    static class HiveTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public HiveTableColumnMeta(String name, String dataType, String comment) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "HiveTableColumnMeta{" + "name='" + name + '\'' + ", dataType='" + dataType + '\'' + ", comment='" + comment + '\'' + '}';
        }
    }

    String tableName;
    String sdLocation;//sd is short for storage descriptor
    String sdInputFormat;
    String sdOutputFormat;
    String owner;
    String tableType;
    int skipHeaderLineCount;
    int lastAccessTime;
    long fileSize;
    long fileNum;
    boolean isNative;
    List<HiveTableColumnMeta> allColumns;
    List<HiveTableColumnMeta> partitionColumns;

    public HiveTableMeta(String tableName, String sdLocation, String sdInputFormat, String sdOutputFormat, String owner, String tableType, int lastAccessTime, long fileSize, long fileNum, int skipHeaderLineCount, boolean isNative, List<HiveTableColumnMeta> allColumns, List<HiveTableColumnMeta> partitionColumns) {
        this.tableName = tableName;
        this.sdLocation = sdLocation;
        this.sdInputFormat = sdInputFormat;
        this.sdOutputFormat = sdOutputFormat;
        this.owner = owner;
        this.tableType = tableType;
        this.lastAccessTime = lastAccessTime;
        this.fileSize = fileSize;
        this.fileNum = fileNum;
        this.isNative = isNative;
        this.allColumns = allColumns;
        this.partitionColumns = partitionColumns;
        this.skipHeaderLineCount = skipHeaderLineCount;
    }

    @Override
    public String toString() {
        return "HiveTableMeta{" + "tableName='" + tableName + '\'' + ", sdLocation='" + sdLocation + '\'' + ", sdInputFormat='" + sdInputFormat + '\'' + ", sdOutputFormat='" + sdOutputFormat + '\'' + ", owner='" + owner + '\'' + ", tableType='" + tableType + '\'' + ", lastAccessTime=" + lastAccessTime + ", fileSize=" + fileSize + ", fileNum=" + fileNum + ", isNative=" + isNative + ", allColumns=" + allColumns + ", partitionColumns=" + partitionColumns + '}';
    }
}
