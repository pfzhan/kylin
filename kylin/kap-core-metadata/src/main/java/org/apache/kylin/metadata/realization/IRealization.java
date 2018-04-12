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

import java.util.List;
import java.util.Set;

import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public interface IRealization extends IStorageAware {

    /**
     * Given the features of a query, check how capable the realization is to answer the query.
     */
    CapabilityResult isCapable(SQLDigest digest);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     */
    String getType();

    KylinConfig getConfig();

    NDataModel getModel();

    Set<TblColRef> getAllColumns();

    Set<ColumnDesc> getAllColumnDescs();

    List<TblColRef> getAllDimensions();

    List<MeasureDesc> getMeasures();

    boolean isReady();

    String getName();

    String getCanonicalName();

    long getDateRangeStart();

    long getDateRangeEnd();

    boolean supportsLimitPushDown();

    int getCost();

    boolean hasPrecalculatedFields();

    LookupStringTable getLookupTable(String lookupTableName);
}
