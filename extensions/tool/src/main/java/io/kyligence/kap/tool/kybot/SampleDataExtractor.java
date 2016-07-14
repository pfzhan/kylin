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

package io.kyligence.kap.tool.kybot;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.storage.hbase.util.HiveCmdBuilder;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.tool.kybot.hive.udf.HiveUdfRegister;
import io.kyligence.kap.tool.kybot.hive.udf.ObfuscateUDF;

public class SampleDataExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(SampleDataExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(true).withDescription("Specify which model to extract").create("model");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SAMPLE_RATE = OptionBuilder.withArgName("sampleRate").hasArg().isRequired(false).withDescription("Specify sampling rate.").create("sampleRate");

    KylinConfig kylinConfig;
    CliCommandExecutor cliCommandExecutor;
    MetadataManager metadataManager;

    public SampleDataExtractor() throws IOException {
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_SAMPLE_RATE);
        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        cliCommandExecutor = kylinConfig.getCliCommandExecutor();
    }

    private Set<DataModelDesc> getModels(String modelSeed) {
        Set<DataModelDesc> selectedModelDescs = Sets.newHashSet();
        List<DataModelDesc> allModelDescs = metadataManager.getModels();
        String[] modelSeeds = null;
        if (!StringUtils.isEmpty(modelSeed)) {
            modelSeeds = modelSeed.split(",");
        }

        for (DataModelDesc modelDesc : allModelDescs) {
            // only extract hive tables
            if (modelSeeds == null || ArrayUtils.contains(modelSeeds, modelDesc.getName())) {
                if (metadataManager.getTableDesc(modelDesc.getFactTable()).getSourceType() == ISourceAware.ID_HIVE) {
                    selectedModelDescs.add(modelDesc);
                    logger.info("Add Data model: {}", modelDesc.getName());
                }
            }
        }
        return selectedModelDescs;
    }

    private boolean isDateColumn(String tableName, String columnName, DataModelDesc dataModelDesc) {
        String partitionDateColumn = dataModelDesc.getPartitionDesc().getPartitionDateColumn();
        String partitionTimeColumn = dataModelDesc.getPartitionDesc().getPartitionTimeColumn();
        partitionDateColumn = (partitionDateColumn == null ? null : partitionDateColumn.substring(partitionDateColumn.lastIndexOf(".") + 1));
        partitionTimeColumn = (partitionTimeColumn == null ? null : partitionTimeColumn.substring(partitionTimeColumn.lastIndexOf(".") + 1));
        if (columnName.equals(partitionDateColumn) || columnName.equals(partitionTimeColumn)) {
            return true;
        }
        String dataType = metadataManager.getTableDesc(tableName).findColumnByName(columnName).getDatatype();
        return dataType.equalsIgnoreCase("date") || dataType.equalsIgnoreCase("timestamp");
    }

    private Pair<Set<String>, Set<String>> getColumnFiltered(String tableName, String[] columns, DataModelDesc dataModelDesc) {
        Set<String> obfuscateColumns = Sets.newTreeSet();
        Set<String> nonObfuscateColumns = Sets.newTreeSet();
        for (String column : columns) {
            if (isDateColumn(tableName, column, dataModelDesc)) {
                nonObfuscateColumns.add(column);
            } else {
                obfuscateColumns.add(column);
            }
        }
        return new Pair<>(obfuscateColumns, nonObfuscateColumns);
    }

    private Map<String, Pair<Set<String>, Set<String>>> getTableColumnsPairs(Set<DataModelDesc> selectedModelDescs) {
        // k:table v:columns
        Map<String, Pair<Set<String>, Set<String>>> tableColumnPair = Maps.newHashMap();
        for (DataModelDesc dataModelDesc : selectedModelDescs) {
            String factTable = dataModelDesc.getFactTable();
            for (ModelDimensionDesc modelDimensionDesc : dataModelDesc.getDimensions()) {
                tableColumnPair.put(modelDimensionDesc.getTable(), new Pair<Set<String>, Set<String>>(Sets.<String> newTreeSet(), Sets.<String> newTreeSet()));
            }

            for (ModelDimensionDesc modelDimensionDesc : dataModelDesc.getDimensions()) {
                String tableName = modelDimensionDesc.getTable();
                Pair<Set<String>, Set<String>> dimensionPair = getColumnFiltered(tableName, modelDimensionDesc.getColumns(), dataModelDesc);
                tableColumnPair.get(tableName).getFirst().addAll(dimensionPair.getFirst());
                tableColumnPair.get(tableName).getSecond().addAll(dimensionPair.getSecond());
                if (tableName.equals(factTable)) {
                    for (LookupDesc lookupDesc : dataModelDesc.getLookups()) {
                        Pair<Set<String>, Set<String>> fkPair = getColumnFiltered(tableName, lookupDesc.getJoin().getForeignKey(), dataModelDesc);
                        tableColumnPair.get(tableName).getFirst().addAll(fkPair.getFirst());
                        tableColumnPair.get(tableName).getSecond().addAll(fkPair.getSecond());
                    }
                    Pair<Set<String>, Set<String>> metricsPair = getColumnFiltered(tableName, dataModelDesc.getMetrics(), dataModelDesc);
                    tableColumnPair.get(tableName).getFirst().addAll(metricsPair.getFirst());
                    tableColumnPair.get(tableName).getSecond().addAll(metricsPair.getSecond());
                    String partitionDateColumn = dataModelDesc.getPartitionDesc().getPartitionDateColumn();
                    String partitionTimeColumn = dataModelDesc.getPartitionDesc().getPartitionTimeColumn();
                    if (partitionDateColumn != null) {
                        tableColumnPair.get(tableName).getSecond().add(partitionDateColumn.substring(partitionDateColumn.lastIndexOf(".") + 1));
                    }
                    if (partitionTimeColumn != null) {
                        tableColumnPair.get(tableName).getSecond().add(partitionTimeColumn.substring(partitionTimeColumn.lastIndexOf(".") + 1));
                    }
                }
            }
        }

        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableColumnPair.entrySet()) {
            logger.info("Table {}, obfuscateColumns: {}, nonObfuscateColumns: {}", entry.getKey(), entry.getValue().getFirst(), entry.getValue().getSecond());
        }

        return tableColumnPair;
    }

    private Set<String> getFactTables(Set<DataModelDesc> selectedModelDescs) {
        Set<String> factTables = Sets.newHashSet();
        for (DataModelDesc dataModelDesc : selectedModelDescs) {
            factTables.add(dataModelDesc.getFactTable());
        }
        return factTables;
    }

    private String generateFields(Set<String> obfuscateColumns, Set<String> nonObfuscateColumns, String functionName, boolean needObfuscate) {
        StringBuilder hql = new StringBuilder();
        Set<String> obsColumns = Sets.newTreeSet();
        if (obfuscateColumns.size() == 0) {
            hql.append(StringUtils.join(nonObfuscateColumns, ", "));
        } else {
            if (needObfuscate) {
                for (String obfuscateColumn : obfuscateColumns) {
                    obsColumns.add(functionName + "(" + obfuscateColumn + ")");
                }
                hql.append(StringUtils.join(obsColumns, ", "));
                if (nonObfuscateColumns.size() != 0) {
                    hql.append(", ");
                }
                hql.append(StringUtils.join(nonObfuscateColumns, ", "));
            } else {
                hql.append(StringUtils.join(obfuscateColumns, ", "));
                if (nonObfuscateColumns.size() != 0) {
                    hql.append(", ");
                }
                hql.append(StringUtils.join(nonObfuscateColumns, ", "));
            }
        }
        return hql.toString();
    }

    private String generateInsertOverwriteHql(boolean isFactTable, String exportDir, Set<String> obfuscateColumns, Set<String> nonObfuscateColumns, String functionName, String tableName, int sampleRate) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT OVERWRITE LOCAL DIRECTORY '").append(exportDir).append("' ").append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ").append("SELECT ");
        if (!isFactTable) {
            stringBuilder.append(generateFields(obfuscateColumns, nonObfuscateColumns, functionName, true)).append(" FROM ").append(tableName).append(";");
        } else {
            stringBuilder.append(generateFields(obfuscateColumns, nonObfuscateColumns, functionName, true)).append(" FROM ").append("(SELECT ROW_NUMBER() OVER() rank, ").append(generateFields(obfuscateColumns, nonObfuscateColumns, functionName, false)).append(" FROM ").append(tableName).append(") table ").append(" WHERE table.rank%").append(sampleRate).append("==1;");
        }
        return stringBuilder.toString();
    }

    private String generateCreateTableHql(String tableName, Set<String> obfuscateColumns, Set<String> nonObfuscateColumns) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE ").append(tableName).append("(").append(StringUtils.join(obfuscateColumns, " String, "));
        if (obfuscateColumns.size() != 0 && nonObfuscateColumns.size() != 0) {
            stringBuilder.append(" String, ");
        }
        stringBuilder.append(StringUtils.join(nonObfuscateColumns, " String, ")).append(" String )").append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' lines terminated BY '\\n';\n");
        return stringBuilder.toString();
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        String modelInput = optionsHelper.getOptionValue(options.getOption("model"));
        int sampleRate = optionsHelper.hasOption(OPTION_SAMPLE_RATE) ? Integer.parseInt(optionsHelper.getOptionValue(OPTION_SAMPLE_RATE)) : 10;

        Set<DataModelDesc> models = getModels(modelInput);
        Map<String, Pair<Set<String>, Set<String>>> tableColumnPair = getTableColumnsPairs(models);
        Set<String> factTables = getFactTables(models);

        String udfFuncName = new ObfuscateUDF().getFuncName();
        HiveUdfRegister hiveUdfRegister = new HiveUdfRegister(ObfuscateUDF.class);
        hiveUdfRegister.register();

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(hiveUdfRegister.getCreateFuncStatement());

        File createScriptFile = new File(exportDir, "createTable.sql");
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableColumnPair.entrySet()) {
            String tableName = entry.getKey();
            Set<String> obfuscateColumns = entry.getValue().getFirst();
            Set<String> nonObfuscateColumns = entry.getValue().getSecond();
            String selectSampleDataHql = factTables.contains(tableName) ? generateInsertOverwriteHql(true, new File(exportDir, tableName).getAbsolutePath(), obfuscateColumns, nonObfuscateColumns, udfFuncName, tableName, sampleRate) : generateInsertOverwriteHql(false, new File(exportDir, tableName).getAbsolutePath(), obfuscateColumns, nonObfuscateColumns, udfFuncName, tableName, sampleRate);
            String createTableHql = generateCreateTableHql(tableName, obfuscateColumns, nonObfuscateColumns);
            FileUtils.writeStringToFile(createScriptFile, createTableHql, true);
            hiveCmdBuilder.addStatement(selectSampleDataHql);
        }

        // log and execute
        String hiveCmd = hiveCmdBuilder.build();
        logger.info("Hive Cmd: {}", hiveCmd);
        cliCommandExecutor.execute(hiveCmd);

        hiveUdfRegister.unregister();

        // export sampling info
        File samplingInfo = new File(exportDir, "sampling.info");
        FileUtils.writeStringToFile(samplingInfo, "SampleRate: " + sampleRate);
    }
}