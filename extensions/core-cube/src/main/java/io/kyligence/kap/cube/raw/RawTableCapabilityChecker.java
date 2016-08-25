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

package io.kyligence.kap.cube.raw;

import java.util.Collection;

import org.apache.kylin.cube.JoinChecker;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class RawTableCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(RawTableCapabilityChecker.class);

    public static CapabilityResult check(RawTableInstance rawTable, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // match joins
        boolean isJoinMatch = JoinChecker.isJoinMatch(digest.joinDescs, rawTable);
        if (!isJoinMatch) {
            logger.info("Exclude RawTableInstance " + rawTable.getName() + " because unmatched joins");
            return result;
        }

        Collection<TblColRef> missingColumns = Sets.newHashSet(digest.allColumns);
        missingColumns.removeAll(rawTable.getAllColumns());
        if (missingColumns.size() > 0) {
            logger.info("Exclude rawtable " + rawTable.getName() + " because missing column(s):" + missingColumns);
        }

        // cost will be minded by caller
        result.capable = true;
        return result;
    }

}
