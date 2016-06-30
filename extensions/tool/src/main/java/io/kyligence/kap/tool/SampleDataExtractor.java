package io.kyligence.kap.tool;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.kap.tool.udf.ObfuscateUdf;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.storage.hbase.util.HiveCmdBuilder;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        List<DataModelDesc> dataModelDescs = MetadataManager.getInstance(kylinConfig).getModels();
        if (modelSeed.equalsIgnoreCase("all")) {
            selectedModelDescs.addAll(dataModelDescs);
        } else {
            for (DataModelDesc dataModelDesc : dataModelDescs) {
                if (dataModelDesc.getName().equals(modelSeed)) {
                    selectedModelDescs.add(dataModelDesc);
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

    private ColumnCounterPair getColumnFiltered(String tableName, String[] columns, DataModelDesc dataModelDesc) {
        Set<String> obfuscateColumns = Sets.newTreeSet();
        Set<String> nonObfuscateColumns = Sets.newTreeSet();
        for (String column : columns) {
            if (isDateColumn(tableName, column, dataModelDesc)) {
                nonObfuscateColumns.add(column);
            } else {
                obfuscateColumns.add(column);
            }
        }
        return new ColumnCounterPair(obfuscateColumns, nonObfuscateColumns);
    }

    private Map<String, ColumnCounterPair> getTableColumnsPairs(Set<DataModelDesc> selectedModelDescs) {
        //k:table v:columns
        Map<String, ColumnCounterPair> tableColumnPair = Maps.newHashMap();
        for (DataModelDesc dataModelDesc : selectedModelDescs) {
            String factTable = dataModelDesc.getFactTable();
            for (ModelDimensionDesc modelDimensionDesc : dataModelDesc.getDimensions()) {
                tableColumnPair.put(modelDimensionDesc.getTable(), new ColumnCounterPair(Sets.<String>newTreeSet(), Sets.<String>newTreeSet()));
            }
            for (ModelDimensionDesc modelDimensionDesc : dataModelDesc.getDimensions()) {
                String tableName = modelDimensionDesc.getTable();
                ColumnCounterPair dimensionPair = getColumnFiltered(tableName, modelDimensionDesc.getColumns(), dataModelDesc);
                tableColumnPair.get(tableName).getObfuscateColumns().addAll(dimensionPair.getObfuscateColumns());
                tableColumnPair.get(tableName).getNonObfuscateColumns().addAll(dimensionPair.getNonObfuscateColumns());
                if (tableName.equals(factTable)) {
                    for (LookupDesc lookupDesc : dataModelDesc.getLookups()) {
                        ColumnCounterPair fkPair = getColumnFiltered(tableName, lookupDesc.getJoin().getForeignKey(), dataModelDesc);
                        tableColumnPair.get(tableName).getObfuscateColumns().addAll(fkPair.getObfuscateColumns());
                        tableColumnPair.get(tableName).getNonObfuscateColumns().addAll(fkPair.getNonObfuscateColumns());
                    }
                    ColumnCounterPair metricsPair = getColumnFiltered(tableName, dataModelDesc.getMetrics(), dataModelDesc);
                    tableColumnPair.get(tableName).getObfuscateColumns().addAll(metricsPair.getObfuscateColumns());
                    tableColumnPair.get(tableName).getNonObfuscateColumns().addAll(metricsPair.getNonObfuscateColumns());
                    String partitionDateColumn = dataModelDesc.getPartitionDesc().getPartitionDateColumn();
                    String partitionTimeColumn = dataModelDesc.getPartitionDesc().getPartitionTimeColumn();
                    if (partitionDateColumn != null) {
                        tableColumnPair.get(tableName).getNonObfuscateColumns().add(partitionDateColumn.substring(partitionDateColumn.lastIndexOf(".") + 1));
                    }
                    if (partitionTimeColumn != null) {
                        tableColumnPair.get(tableName).getNonObfuscateColumns().add(partitionTimeColumn.substring(partitionTimeColumn.lastIndexOf(".") + 1));
                    }
                }
            }
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

    private String appendFunctionalHql(Set<String> obfuscateColumns, Set<String> nonObfuscateColumns,String functionName, boolean needObfuscate) {
        StringBuilder hql = new StringBuilder();
        Set<String> obsColumns = Sets.newTreeSet();
        if (obfuscateColumns.size() == 0) {
            hql.append(StringUtils.join(nonObfuscateColumns,", "));
        } else {
            if (needObfuscate) {
                for (String obfuscateColumn : obfuscateColumns) {
                    obsColumns.add(functionName + "(" + obfuscateColumn + ")");
                }
                hql.append(StringUtils.join(obsColumns,", "));
                if (nonObfuscateColumns.size() != 0) {
                    hql.append(", ");
                }
                hql.append(StringUtils.join(nonObfuscateColumns,", "));
            } else {
                hql.append(StringUtils.join(obfuscateColumns,", "));
                if (nonObfuscateColumns.size() != 0) {
                    hql.append(", ");
                }
                hql.append(StringUtils.join(nonObfuscateColumns, ", "));
            }
        }
        return hql.toString();
    }

    private String generateSelectHql(boolean isFactTable,String exportDir,Set<String> obfuscateColumns, Set<String> nonObfuscateColumns,String functionName,String tableName,int sampleRate) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT OVERWRITE LOCAL DIRECTORY '")
                .append(exportDir).append("' ")
                .append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
                .append("SELECT ");
        if (!isFactTable) {
            stringBuilder.append(appendFunctionalHql(obfuscateColumns, nonObfuscateColumns, functionName, true))
                    .append(" FROM ")
                    .append(tableName)
                    .append(";");
        } else {
            stringBuilder.append(appendFunctionalHql(obfuscateColumns, nonObfuscateColumns, functionName, true))
                    .append(" FROM ")
                    .append("(SELECT ROW_NUMBER() OVER() rank, ")
                    .append(appendFunctionalHql(obfuscateColumns, nonObfuscateColumns, functionName, false))
                    .append(" FROM ")
                    .append(tableName).append(") table ")
                    .append(" WHERE table.rank%").append(sampleRate).append("==1;");
        }
        return stringBuilder.toString();
    }

    private String createTableHql (String tableName,Set<String> obfuscateColumns, Set<String> nonObfuscateColumns) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE ").append(tableName).append("(")
                .append(StringUtils.join(obfuscateColumns," String, "));
        if (obfuscateColumns.size() != 0 && nonObfuscateColumns.size()!= 0 ) {
            stringBuilder.append(" String, ");
        }
        stringBuilder.append(StringUtils.join(nonObfuscateColumns," String, "))
                .append(" String )")
                .append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' lines terminated BY '\\n';\n");
        return stringBuilder.toString();
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        String modelInput = optionsHelper.getOptionValue(options.getOption("model"));
        int sampleRate = optionsHelper.hasOption(OPTION_SAMPLE_RATE) ? Integer.parseInt(optionsHelper.getOptionValue(OPTION_SAMPLE_RATE)) : 10;
        Map<String, ColumnCounterPair> tableColumnPair = getTableColumnsPairs(getModels(modelInput));
        Set<String> factTables = getFactTables(getModels(modelInput));
        File createTableFile = new File(exportDir,"createTableHql");
        String functionName = "kylin_obfuscate";

        HiveUdfRegister hiveUdfRegister = new HiveUdfRegister(ObfuscateUdf.class.getName());
        String createFunctionHql = hiveUdfRegister.registerToHive(functionName);
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(createFunctionHql);

        for (Map.Entry<String, ColumnCounterPair> entry : tableColumnPair.entrySet()) {
            String tableName = entry.getKey();
            Set<String> obfuscateColumns = entry.getValue().getObfuscateColumns();
            Set<String> nonObfuscateColumns = entry.getValue().getNonObfuscateColumns();
            String selectSampleDataHql = factTables.contains(tableName) ?
                    generateSelectHql(true,new File(exportDir, tableName).getAbsolutePath(),obfuscateColumns,nonObfuscateColumns,functionName,tableName,sampleRate) :
                    generateSelectHql(false, new File(exportDir,tableName).getAbsolutePath(),obfuscateColumns,nonObfuscateColumns,functionName,tableName,sampleRate);
            String createTableHql = createTableHql(tableName,obfuscateColumns,nonObfuscateColumns);
            FileUtils.writeStringToFile(createTableFile,createTableHql,true);
            hiveCmdBuilder.addStatement(selectSampleDataHql);
        }
        cliCommandExecutor.execute(hiveCmdBuilder.build());
        hiveUdfRegister.deregisterFromHive(functionName);
    }
}

class ColumnCounterPair {
    private Set<String> obfuscateColumns;
    private Set<String> nonObfuscateColumns;

    public ColumnCounterPair(Set<String> obfuscateColumns, Set<String> nonObfuscateColumns) {
        this.obfuscateColumns = obfuscateColumns;
        this.nonObfuscateColumns = nonObfuscateColumns;
    }

    public Set<String> getNonObfuscateColumns() {
        return nonObfuscateColumns;
    }

    public void setNonObfuscateColumns(Set<String> nonObfuscateColumns) {
        this.nonObfuscateColumns = nonObfuscateColumns;
    }

    public Set<String> getObfuscateColumns() {
        return obfuscateColumns;
    }

    public void setObfuscateColumns(Set<String> obfuscateColumns) {
        this.obfuscateColumns = obfuscateColumns;
    }
}