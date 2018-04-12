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

package org.apache.kylin.metadata.realization;

import java.util.Arrays;

import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TsConditionEraser;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;

/**
 *
 * A encapsulation of {@link SQLDigest},
 * This class makes {@link SQLDigest} being able to compare with other {@link SQLDigest}
 * regardless of the timestamp conditions(In top level where conditions concatenated by ANDs)
 */
public class StreamSQLDigest {

    private final SQLDigest sqlDigest;

    private final int hashCode;
    private final byte[] filterSerialized;

    public StreamSQLDigest(SQLDigest sqlDigest, TblColRef tsCol) {
        this.sqlDigest = sqlDigest;

        //must use new instance of IgnoreTsCondition
        TsConditionEraser decorator = new TsConditionEraser(tsCol, sqlDigest.filter);
        filterSerialized = TupleFilterSerializer.serialize(sqlDigest.filter, decorator, StringCodeSystem.INSTANCE);

        int nonFilterHashCode = calculateNonFilterHashCode();
        this.hashCode = 31 * nonFilterHashCode + Arrays.hashCode(filterSerialized);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        StreamSQLDigest other = (StreamSQLDigest) o;

        if (filterSerialized != null ? !Arrays.equals(filterSerialized, other.filterSerialized) : other.filterSerialized != null)
            return false;
        if (sqlDigest.aggregations != null ? !sqlDigest.aggregations.equals(other.sqlDigest.aggregations) : other.sqlDigest.aggregations != null)
            return false;
        if (sqlDigest.allColumns != null ? !sqlDigest.allColumns.equals(other.sqlDigest.allColumns) : other.sqlDigest.allColumns != null)
            return false;
        if (sqlDigest.factTable != null ? !sqlDigest.factTable.equals(other.sqlDigest.factTable) : other.sqlDigest.factTable != null)
            return false;
        if (sqlDigest.filterColumns != null ? !sqlDigest.filterColumns.equals(other.sqlDigest.filterColumns) : other.sqlDigest.filterColumns != null)
            return false;
        if (sqlDigest.groupbyColumns != null ? !sqlDigest.groupbyColumns.equals(other.sqlDigest.groupbyColumns) : other.sqlDigest.groupbyColumns != null)
            return false;
        if (sqlDigest.joinDescs != null ? !sqlDigest.joinDescs.equals(other.sqlDigest.joinDescs) : other.sqlDigest.joinDescs != null)
            return false;
        if (sqlDigest.metricColumns != null ? !sqlDigest.metricColumns.equals(other.sqlDigest.metricColumns) : other.sqlDigest.metricColumns != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    public int calculateNonFilterHashCode() {
        int result = sqlDigest.factTable != null ? sqlDigest.factTable.hashCode() : 0;
        result = 31 * result + (sqlDigest.joinDescs != null ? sqlDigest.joinDescs.hashCode() : 0);
        result = 31 * result + (sqlDigest.allColumns != null ? sqlDigest.allColumns.hashCode() : 0);
        result = 31 * result + (sqlDigest.groupbyColumns != null ? sqlDigest.groupbyColumns.hashCode() : 0);
        result = 31 * result + (sqlDigest.filterColumns != null ? sqlDigest.filterColumns.hashCode() : 0);
        result = 31 * result + (sqlDigest.metricColumns != null ? sqlDigest.metricColumns.hashCode() : 0);
        result = 31 * result + (sqlDigest.aggregations != null ? sqlDigest.aggregations.hashCode() : 0);
        return result;
    }

}
