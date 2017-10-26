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

package io.kyligence.kap.storage.parquet.cube;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.metadata.model.KapModel;

public class CubeStorageQuery extends GTCubeStorageQueryBase {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CubeStorageQuery.class);

    public CubeStorageQuery(CubeInstance cube) {
        super(cube);
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        this.cubeInstance = replaceMPCubeIfNeeded(cubeInstance, sqlDigest);

        return super.search(context, sqlDigest, returnTupleInfo);
    }

    private CubeInstance replaceMPCubeIfNeeded(CubeInstance cube, SQLDigest sqlDigest) {
        KapModel model = (KapModel) cube.getModel();
        if (model.isMultiLevelPartitioned() == false)
            return cube;

        String[] mpValues = extractMPValues(model, sqlDigest.filter);

        MPCubeManager mgr = MPCubeManager.getInstance(cube.getConfig());
        CubeInstance ret = null;
        try {
            ret = mgr.convertToMPCubeIfNeeded(cube.getName(), mpValues);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Invalid convert cube mpmaster to mpcube. cube name '" + cube.getName() + "'", e);
        }
        return ret;
    }

    String[] extractMPValues(KapModel model, TupleFilter filter) {
        TblColRef[] mpCols = model.getMutiLevelPartitionCols();
        List<TblColRef> mpColList = Lists.newArrayList(mpCols);

        Set<CompareTupleFilter> singleValueFilters = findSingleValuesCompFilters(filter);
        Set<TblColRef> singleValueCols = findSingleValueColumns(filter);

        checkQueryConditions(mpColList, singleValueCols);

        List<String> mpValueList = Lists.newArrayList();
        Map<TblColRef, String> mpValues = extractMPValues(mpColList, singleValueFilters);
        for (TblColRef col : mpColList) {
            if (null == mpValues.get(col)) {
                throw new IllegalStateException(
                        "Invalid. Missing muti-level partition condition on column: " + col);
            }
            mpValueList.add(mpValues.get(col));
        }

        String[] result = new String[mpValueList.size()];
        return mpValueList.toArray(result);
    }

    private void checkQueryConditions(List<TblColRef> mpCols, Set<TblColRef> querySingleValueCols) {
        if (querySingleValueCols.containsAll(mpCols) == false) {
            Set<TblColRef> missing = new HashSet<>(mpCols);
            missing.removeAll(querySingleValueCols);
            throw new IllegalStateException(
                    "Invalid query, multi-level partitioned query must provide all partition values as '=' filter condition. "
                            + "Missing filter on " + missing + ".");
        }
    }

    private Map<TblColRef, String> extractMPValues(List<TblColRef> colList, Set<CompareTupleFilter> compFilterSet) {
        Map<TblColRef, String> tblMap = new HashMap<TblColRef, String>();
        for (CompareTupleFilter compFilter : compFilterSet) {
            TblColRef tblColRef = compFilter.getColumn();
            if (colList.contains(tblColRef)) {
                tblMap.put(tblColRef, compFilter.getFirstValue().toString());
            }
        }

        return tblMap;
    }

    protected boolean skipZeroInputSegment(CubeSegment cubeSegment) {
        return true;
    }

    @Override
    protected String getGTStorage() {
        return KapConfig.getInstanceFromEnv().getSparkCubeGTStorage();
    }

}
