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

import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;

public class HBaseMappingAdapter implements IKeepNames {

    private static final Logger logger = LoggerFactory.getLogger(HBaseMappingAdapter.class);

    private static final int MAX_ENGINE_NUM_OF_HBASE = 2;
    private static final int MIN_ENGINE_NUM_OF_KYSTORAGE = 99;

    // Initiate HBaseMapping in CubeDesc according to storage engine type and column family version. 
    public static void initHBaseMapping(CubeDesc cubeDesc) {

        int storageType = cubeDesc.getStorageType();
        HBaseMappingDesc hbaseMapping = cubeDesc.getHbaseMapping();

        if (storageType <= MAX_ENGINE_NUM_OF_HBASE) {

            // Initiate as meta data indicates since current HBase mapping is compatible with original HBase mapping.  
            if (hbaseMapping != null) {
                hbaseMapping.init(cubeDesc);
                logger.trace("init cf info for " + cubeDesc.getName() + " as cf in HBase");
            }

        } else if (storageType >= MIN_ENGINE_NUM_OF_KYSTORAGE) {

            // For cubes with equal or higher version than column family adopted version: 
            // initiate as meta data indicates; 
            // For cubes with lower version than column family adopted version, or cube with no info about HBase mapping: 
            // distribute all measures into separate column families since legacy cubes do not handle HBase mapping.  
            if (hbaseMapping != null && KylinVersion.compare(cubeDesc.getVersion(), "2.2.0.20500") >= 0) {
                hbaseMapping.init(cubeDesc);
                reorderMeasuresInColumnFamily(cubeDesc.getMeasures(), hbaseMapping);
                logger.trace("init cf info for " + cubeDesc.getName() + " as cf in KyStorage");
            } else {
                cubeDesc.setHbaseMapping(new HBaseMappingDesc());
                hbaseMapping = cubeDesc.getHbaseMapping();
                hbaseMapping.initAsSeparatedColumns(cubeDesc);
                logger.trace("adapt cf info for " + cubeDesc.getName() + " as separated cf in KyStorage");
            }
        }
    }
    
    private static void reorderMeasuresInColumnFamily(List<MeasureDesc> measures, HBaseMappingDesc hbaseMapping) {
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < measures.size(); i++) {
            measureIndexLookup.put(measures.get(i).getName(), i);
        }
        
        for (HBaseColumnFamilyDesc cf : hbaseMapping.getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                Map<Integer, String> mapInOrder = new TreeMap<Integer, String>();
                for (String measureRef : c.getMeasureRefs()) {
                    mapInOrder.put(measureIndexLookup.get(measureRef), measureRef);
                }
                String[] measureRefInOrder = new String[c.getMeasureRefs().length];
                int idx = 0;
                for (Integer i : mapInOrder.keySet()) {
                    measureRefInOrder[idx++] = mapInOrder.get(i);
                }
                c.setMeasureRefs(measureRefInOrder);
            }
        }
    }
    
    // Initiate measure reference to column family with checking for measure order.  
    public static void initMeasureReferenceToColumnFamilyWithChecking(CubeDesc cubeDesc) {
        
        int storageType = cubeDesc.getStorageType();
        
        cubeDesc.initMeasureReferenceToColumnFamily();
        
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < measures.size(); i++)
            measureIndexLookup.put(measures.get(i).getName(), i);
        
        if (storageType >= MIN_ENGINE_NUM_OF_KYSTORAGE && KylinVersion.compare(cubeDesc.getVersion(), "2.2.0.20500") >= 0) {
            Set<String> measureSet = Sets.newHashSet();
            for (HBaseColumnFamilyDesc cf : cubeDesc.getHbaseMapping().getColumnFamily()) {
                for (HBaseColumnDesc c : cf.getColumns()) {
                    String[] colMeasureRefs = c.getMeasureRefs();int[] measureIndex = new int[colMeasureRefs.length];
                    int lastMeasureIndex = -1;
                    for (int i = 0; i < colMeasureRefs.length; i++) {                        
                        checkState(!measureSet.contains(colMeasureRefs[i]), "measure (%s) duplicates", colMeasureRefs[i]);
                        measureSet.add(colMeasureRefs[i]);
                        measureIndex[i] = measureIndexLookup.get(colMeasureRefs[i]);
                        checkState(measureIndex[i] > lastMeasureIndex, "measure (%s) is not in order", colMeasureRefs[i]);
                        lastMeasureIndex = measureIndex[i];
                    }
                }
            }
        }
        
    }

}
