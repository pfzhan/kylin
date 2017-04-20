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

package io.kyligence.kap.source.hive.modelstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelDiagnose {

    private static final Logger logger = LoggerFactory.getLogger(ModelDiagnose.class);

    private static final long FACT_SKEW_THRESHOLD = 5000000L;
    private static final float FLAT_TABLE_RECORDS_RATION = 0.01F;
    private static final float LEFT_JOIN_NULL_TOLERANCE = 0.1F;

    /**
     *
     * Diagnose if there are duplicate primary key on lookup tables. It supposes that
     * all lookup tables can be built as snapshot.
     * @throws java.io.IOException
     */
    public static void checkDuplicatePKOnLookups(ModelStats modelStats, DataModelDesc dataModelDesc, KylinConfig config) throws IOException {
        List<ModelStats.DuplicatePK> dupPKList = new ArrayList<>();
        for (JoinTableDesc fTable : dataModelDesc.getJoinTables()) {
            List<TblColRef> primaryKeys = new ArrayList<>();
            primaryKeys.addAll(Arrays.asList(fTable.getJoin().getPrimaryKeyColumns()));
            TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(fTable.getTable());
            ReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
            SnapshotTable snapshot = SnapshotManager.getInstance(config).buildSnapshot(hiveTable, tableDesc);
            ModelStats.DuplicatePK dupPK = checkLookup(tableDesc, primaryKeys, snapshot);
            dupPKList.add(dupPK);
        }
        modelStats.setDuplicatePrimaryKeys(dupPKList);
        ModelStatsManager.getInstance(config).saveModelStats(modelStats);

        String ret = modelStats.getDuplicationResult();
        if (!StringUtils.isEmpty(ret)) {
            throw new IllegalStateException("Duplicate key found: " + ret);
        }
    }

    private static ModelStats.DuplicatePK checkLookup(TableDesc tableDesc, List<TblColRef> keyColumns, SnapshotTable table) throws IOException {
        int[] keyIndex = new int[keyColumns.size()];
        String[] keyValues = new String[keyColumns.size()];
        String[] keyNames = new String[keyColumns.size()];

        for (int i = 0; i < keyColumns.size(); i++) {
            String keyName = keyColumns.get(i).getCanonicalName();
            keyNames[i] = keyName;
            keyIndex[i] = tableDesc.findColumnByName(keyName).getZeroBasedIndex();
        }

        Map<String[], MutableInt> dupMap = new HashedMap();
        ModelStats.DuplicatePK dupPK = new ModelStats.DuplicatePK();
        dupPK.setLookUpTable(tableDesc.getName());
        dupPK.setPrimaryKeys(toString(keyNames));
        ReadableTable.TableReader reader = table.getReader();
        while (reader.next()) {
            for (int i = 0; i < keyColumns.size(); i++) {
                keyValues[i] = reader.getRow()[keyIndex[i]];
            }

            Array<String> key = new Array<>(keyValues);

            MutableInt m = dupMap.get(key);
            if (null == m) {
                dupMap.put(keyValues, MutableInt.getInstance());
            } else
                m.increment();
        }
        Map<String, Integer> tmpMap = new HashMap<>();
        for (Map.Entry<String[], MutableInt> e : dupMap.entrySet()) {
            if (e.getValue().getCount() > 1) {
                tmpMap.put(toString(e.getKey()), e.getValue().getCount());
            }
        }
        dupPK.setDuplication(tmpMap);
        IOUtils.closeQuietly(reader);
        return dupPK;
    }

    private static String toString(String[] cols) {
        StringBuilder b = new StringBuilder();
        b.append("{");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0)
                b.append(",");
            b.append(cols[i]);
        }
        b.append("}");
        return b.toString();
    }

    /**
     *
     * Diagnose if it exists possible data skew. Suppose that the root fact table has available stats.
     */
    public static void checkDataSkewOnFactTable(DataModelDesc dataModelDesc, ModelStats modelStats, KylinConfig config) throws IOException {

        String factTableName = dataModelDesc.getRootFactTable().getTableIdentity();
        TableExtDesc tableExtDesc = MetadataManager.getInstance(config).getTableExt(factTableName);
        TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(factTableName);
        if (tableExtDesc.getColumnStats().size() == 0) {
            logger.warn("The root fact table: {} has no available stats, will skip data skew check!", factTableName);
            return;
        }

        long allCount = tableExtDesc.getTotalRows();

        // If the all records is less than FACT_SKEW_THRESHOLD, skip
        if (allCount < FACT_SKEW_THRESHOLD)
            return;

        Map<String, List<ModelStats.SkewResult>> skewFks = new HashMap<>();
        for (JoinTableDesc fTable : dataModelDesc.getJoinTables()) {
            List<TblColRef> foreignKeys = new ArrayList<>();
            foreignKeys.addAll(Arrays.asList(fTable.getJoin().getForeignKeyColumns()));
            // Only process foreignKeys.size = 1
            if (foreignKeys.size() != 1)
                continue;
            String fkColName = foreignKeys.get(0).getIdentity();
            int fkColIndex = tableDesc.findColumnByName(fkColName).getZeroBasedIndex();
            List<ModelStats.SkewResult> skewList = new ArrayList<>();
            Long null_count = tableExtDesc.getColumnStats().get(fkColIndex).getNullCount();
            // Check 'NULL' data skew on FK
            if (null_count > FACT_SKEW_THRESHOLD) {
                logger.warn("There might be data skew on column: {}, value: NULL", fkColName);
                ModelStats.SkewResult skewResult = new ModelStats.SkewResult();
                skewResult.setDataSkewValue("NULL");
                skewResult.setDataSkewCount(null_count);
                skewList.add(skewResult);
            }
            // List all possible data skew column value
            Map<String, Long> dataSkew = tableExtDesc.getColumnStats().get(fkColIndex).getDataSkewSamples();
            for (Map.Entry<String, Long> ele : dataSkew.entrySet()) {
                Long estimatedFkOccurrence = ele.getValue();
                if (estimatedFkOccurrence > FACT_SKEW_THRESHOLD) {
                    logger.warn("There might be data skew on column: " + fkColName);
                    ModelStats.SkewResult skewResult = new ModelStats.SkewResult();
                    skewResult.setDataSkewValue(ele.getKey());
                    skewResult.setDataSkewCount(estimatedFkOccurrence);
                    skewList.add(skewResult);
                }
            }
            skewFks.put(fkColName, skewList);
        }
        modelStats.setDataSkew(skewFks);
        ModelStatsManager.getInstance(config).saveModelStats(modelStats);
    }

    /**
     *
     * Diagnose if the model's flat table is reasonable.
     *
     */
    public static void checkJointResult(DataModelDesc modelDesc, ModelStats modelStats, KylinConfig config) throws IOException {
        String factTableName = modelDesc.getRootFactTable().getTableIdentity();
        float countFact = (float) (MetadataManager.getInstance(config).getTableExt(factTableName).getTotalRows());
        if (countFact <= 0) {
            logger.warn("The root fact table: {} has no available stats, will skip data skew check!", factTableName);
            return;
        }

        Map<String, ModelStats.JoinResult> joinResults = new HashMap<>();
        float countFlatTable = (float) modelStats.getCounter();
        if (countFlatTable / countFact < FLAT_TABLE_RECORDS_RATION) {
            logger.warn("The records of model's flat table is too few, please check the join details");
            ModelStats.JoinResult joinResult = new ModelStats.JoinResult();
            joinResult.setJoinResultValidCount(modelStats.getCounter());
            joinResult.setJoinResultRatio(countFlatTable / countFact);
            joinResults.put(ModelStats.JOIN_RESULT_OVERALL, joinResult);
            modelStats.setJoinResult(joinResults);
            ModelStatsManager.getInstance(config).saveModelStats(modelStats);
            return;
        }

        for (JoinTableDesc fTable : modelDesc.getJoinTables()) {
            if (!"left".equalsIgnoreCase(fTable.getJoin().getType()))
                continue;

            List<TblColRef> pkKeys = new ArrayList<>();
            pkKeys.addAll(Arrays.asList(fTable.getJoin().getPrimaryKeyColumns()));
            if (pkKeys.size() < 1)
                continue;

            String pkName = pkKeys.get(0).getCanonicalName();
            long null_counter = modelStats.getColumnNullMap().get(pkName);
            float ratio = (countFlatTable - (float) null_counter) / countFlatTable;
            if (ratio < LEFT_JOIN_NULL_TOLERANCE) {
                ModelStats.JoinResult joinResult = new ModelStats.JoinResult();
                joinResult.setJoinResultValidCount(modelStats.getCounter() - null_counter);
                joinResult.setJoinResultRatio(ratio);
                joinResults.put(pkName, joinResult);
                logger.warn("There are too many null value on fact table: {} left join lookup table: {}", factTableName, fTable.getTable());
            }
        }
        modelStats.setJoinResult(joinResults);
        ModelStatsManager.getInstance(config).saveModelStats(modelStats);
    }

    static class MutableInt {
        int count = 1;

        public static MutableInt getInstance() {
            return new MutableInt();
        }

        public void increment() {
            count++;
        }

        public int getCount() {
            return this.count;
        }
    }
}
