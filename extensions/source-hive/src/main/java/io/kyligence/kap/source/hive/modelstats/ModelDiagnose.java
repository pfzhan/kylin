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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelDiagnose {

    private static final Logger logger = LoggerFactory.getLogger(ModelDiagnose.class);

    private static final long FACT_SKEW_THRESHOLD = KapConfig.getInstanceFromEnv().getJointDataSkewThreshold();
    private static final float FLAT_TABLE_RECORDS_RATIO = 0.01F;
    private static final float LEFT_JOIN_NULL_TOLERANCE = 0.1F;
    private static final float DUPLICATION_TOLERANCE = 0.1F;
    private static final int DUPLICATION_THRESHOLD = 10;

    /**
     *
     * Diagnose if there are duplicate primary key on lookup tables. It supposes that
     * all lookup tables can be built as snapshot.
     * @throws java.io.IOException
     */
    public static void checkDuplicatePKOnLookups(ModelStats modelStats, DataModelDesc dataModelDesc, KylinConfig config)
            throws IOException {
        List<ModelStats.DuplicatePK> dupPKList = new ArrayList<>();
        MetadataManager metadataManager = MetadataManager.getInstance(config);
        List<String> tables = new ArrayList<>();

        for (TableRef tblRef : dataModelDesc.getLookupTables()) {
            JoinTableDesc fTable = getJoinTableDesc(tblRef, dataModelDesc.getJoinTables());

            if (fTable == null)
                throw new IllegalArgumentException("Lookup table: " + tblRef.getTableName() + " has no JoinTableDesc.");

            String tableName = fTable.getTable();

            if (tables.contains(tableName))
                continue;
            else
                tables.add(tableName);
            TableDesc tableDesc = metadataManager.getTableDesc(fTable.getTable(), dataModelDesc.getProject());
            List<TblColRef> primaryKeys = new ArrayList<>();
            primaryKeys.addAll(Arrays.asList(fTable.getJoin().getPrimaryKeyColumns()));
            IReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
            SnapshotTable snapshot = SnapshotManager.getInstance(config).buildSnapshot(hiveTable, tableDesc);
            ModelStats.DuplicatePK dupPK = checkLookup(tableDesc, primaryKeys, snapshot);
            dupPKList.add(dupPK);
        }
        modelStats.setDuplicatePrimaryKeys(dupPKList);
        ModelStatsManager.getInstance(config).saveModelStats(modelStats);
    }

    private static JoinTableDesc getJoinTableDesc(TableRef tbl, JoinTableDesc[] joinTableDescs) {
        for (JoinTableDesc joinTableDesc : joinTableDescs) {
            if (joinTableDesc.getTableRef() == tbl)
                return joinTableDesc;
        }
        return null;
    }

    private static ModelStats.DuplicatePK checkLookup(TableDesc tableDesc, List<TblColRef> keyColumns,
            SnapshotTable table) throws IOException {
        int[] keyIndex = new int[keyColumns.size()];
        String[] keyValues = new String[keyColumns.size()];
        String[] keyNames = new String[keyColumns.size()];

        long rowCount = 0;
        for (int i = 0; i < keyColumns.size(); i++) {
            String keyName = keyColumns.get(i).getCanonicalName();
            keyNames[i] = keyName;
            keyIndex[i] = tableDesc.findColumnByName(keyName).getZeroBasedIndex();
        }

        Map<String, MutableInt> dupMap = new HashedMap();
        ModelStats.DuplicatePK dupPK = new ModelStats.DuplicatePK();
        dupPK.setLookUpTable(tableDesc.getName());
        dupPK.setPrimaryKeys(toString(keyNames));
        IReadableTable.TableReader reader = table.getReader();
        while (reader.next()) {
            for (int i = 0; i < keyColumns.size(); i++) {
                keyValues[i] = reader.getRow()[keyIndex[i]];
            }

            String key = getUniqueKey(keyValues);

            MutableInt m = dupMap.get(key);
            if (null == m) {
                dupMap.put(key, MutableInt.getInstance());
            } else
                m.increment();
            rowCount++;
        }
        Map<String, Integer> tmpMap = new HashMap<>();
        for (Map.Entry<String, MutableInt> e : dupMap.entrySet()) {
            int count = e.getValue().getCount();
            if (count > 1) {
                tmpMap.put(e.getKey(), e.getValue().getCount());
                if ((float) count / (float) rowCount > DUPLICATION_TOLERANCE && count > DUPLICATION_THRESHOLD) {
                    throw new IllegalStateException("Duplicate key found and can not be tolerant: (Key=" + e.getKey()
                            + ", Value=" + count + ")");
                }
            }
        }
        dupPK.setDuplication(tmpMap);
        IOUtils.closeQuietly(reader);
        return dupPK;
    }

    private static String getUniqueKey(String[] cols) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String s : cols) {
            s.replace(',', '.');
            if (i > 0)
                sb.append(',');
            sb.append(s);
            i++;
        }
        return sb.toString();
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
    public static void checkDataSkewOnFactTable(DataModelDesc dataModelDesc, ModelStats modelStats, KylinConfig config)
            throws IOException {

        String factTableName = dataModelDesc.getRootFactTable().getTableIdentity();
        TableExtDesc tableExtDesc = MetadataManager.getInstance(config).getTableExt(factTableName,
                dataModelDesc.getProject());
        TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(factTableName,
                dataModelDesc.getProject());
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
            TblColRef tblColRef = foreignKeys.get(0);
            String fkColName = tblColRef.getIdentity();
            if (null == tableDesc.findColumnByName(fkColName))
                continue;
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
            int frequency = tableExtDesc.getFrequency();
            Map<String, Long> dataSkew = tableExtDesc.getColumnStats().get(fkColIndex).getDataSkewSamples();
            for (Map.Entry<String, Long> ele : dataSkew.entrySet()) {
                Long estimatedFkOccurrence = ele.getValue();
                if (estimatedFkOccurrence * frequency > FACT_SKEW_THRESHOLD) {
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
    public static void checkJointResult(DataModelDesc modelDesc, ModelStats modelStats, KylinConfig config)
            throws IOException {
        String factTableName = modelDesc.getRootFactTable().getTableIdentity();
        long countFact = MetadataManager.getInstance(config).getTableExt(factTableName, modelDesc.getProject())
                .getTotalRows();
        if (countFact <= 0) {
            logger.warn("The root fact table: {} has no available stats, will skip data skew check!", factTableName);
            return;
        }

        List<ModelStats.JoinResult> joinResults = new ArrayList<>();
        long countAfterJoin = modelStats.getCounter();
        if ((float) countAfterJoin / (float) countFact < FLAT_TABLE_RECORDS_RATIO) {
            logger.warn("The records of model's flat table is too few, please check the join details");
            ModelStats.JoinResult joinResult = new ModelStats.JoinResult();
            joinResult.setJoinResultValidCount(countAfterJoin);
            joinResult.setFactTableCount(countFact);
            joinResults.add(joinResult);
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

            String pkName = pkKeys.get(0).getIdentity();
            long null_counter = modelStats.getColumnNullMap().get(pkName);
            float ratio = ((float) countAfterJoin - (float) null_counter) / (float) countAfterJoin;
            if (ratio < LEFT_JOIN_NULL_TOLERANCE) {
                ModelStats.JoinResult joinResult = new ModelStats.JoinResult();
                joinResult.setJoinTableName(fTable.getAlias());
                joinResult.setPrimaryKey(pkName);
                joinResult.setJoinResultValidCount(modelStats.getCounter() - null_counter);
                joinResult.setFactTableCount(countFact);
                joinResults.add(joinResult);
                logger.warn("There are too many null value on fact table: {} left join lookup table: {}", factTableName,
                        fTable.getTable());
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
