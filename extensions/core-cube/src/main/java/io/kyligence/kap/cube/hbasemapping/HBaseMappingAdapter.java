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

package io.kyligence.kap.cube.hbasemapping;

import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMappingAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(HBaseMappingAdapter.class);
    
    private static final int MAX_ENGINE_NUM_OF_HBASE = 2;
    private static final int MIN_ENGINE_NUM_OF_KYSTORAGE = 99;

    // Initiate HBaseMapping in CubeDesc according to storage engine type and column family version. 
    public static void initHBaseMapping(CubeDesc cubeDesc) {
        
        int storageType = cubeDesc.getStorageType();
        HBaseMappingDesc hbaseMapping = cubeDesc.getHbaseMapping();

        if(storageType <= MAX_ENGINE_NUM_OF_HBASE) {
            
            // Initiate as meta data indicates since current HBase mapping is compatible with original HBase mapping.  
            if (hbaseMapping != null) {
                hbaseMapping.init(cubeDesc);
                logger.info("init cf info for " + cubeDesc.getName() + " as cf in HBase");
            }
        
        } else if(storageType >= MIN_ENGINE_NUM_OF_KYSTORAGE) {
            
            // For cubes with equal or higher version than column family adopted version: 
            // initiate as meta data indicates; 
            // For cubes with lower version than column family adopted version, or cube with no info about HBase mapping: 
            // distribute all measures into separate column families since legacy cubes do not handle HBase mapping.  
            if (hbaseMapping != null && KylinVersion.compare(cubeDesc.getVersion(), "2.2.0.20500") >= 0) {
                hbaseMapping.init(cubeDesc);
                logger.info("init cf info for " + cubeDesc.getName() + " as cf in KyStorage");
                    
            } else {
                cubeDesc.setHbaseMapping(new HBaseMappingDesc());
                hbaseMapping = cubeDesc.getHbaseMapping();
                hbaseMapping.initAsSeparatedColumns(cubeDesc);
                logger.info("adapt cf info for " + cubeDesc.getName() + " as separated cf in KyStorage");
            }
        }
    }
    
}
