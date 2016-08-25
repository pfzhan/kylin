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

package io.kyligence.kap.storage.parquet.cube;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class CubeStorageQuery extends GTCubeStorageQueryBase {
    private static final Logger logger = LoggerFactory.getLogger(CubeStorageQuery.class);

    public CubeStorageQuery(CubeInstance cube) {
        super(cube);
    }

    protected boolean skipZeroInputSegment(CubeSegment cubeSegment) {
        return true;
    }

    @Override
    protected String getGTStorage() {
        return KapConfig.getInstanceFromEnv().getSparkCubeGTStorage();
    }

    public boolean isNeedStorageAggregation(Cuboid cuboid, Collection<TblColRef> groupD, Collection<TblColRef> singleValueD, boolean isExactAggregation) {

        logger.info("GroupD :" + groupD);
        logger.info("SingleValueD :" + singleValueD);
        logger.info("Cuboid columns :" + cuboid.getColumns());

        HashSet<TblColRef> temp = Sets.newHashSet();
        temp.addAll(groupD);
        temp.addAll(singleValueD);
        if (cuboid.getColumns().size() == temp.size()) {
            logger.info("Does not need storage aggregation");
            return false;
        } else {
            logger.info("Need storage aggregation");
            return true;
        }
    }
}
