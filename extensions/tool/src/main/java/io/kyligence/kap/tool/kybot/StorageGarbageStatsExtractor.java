package io.kyligence.kap.tool.kybot;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.util.HiveCmdBuilder;
import org.apache.kylin.tool.AbstractInfoExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StorageGarbageStatsExtractor extends AbstractInfoExtractor {

    private static final Logger logger = LoggerFactory.getLogger(StorageGarbageStatsExtractor.class);

    KylinConfig kylinConfig;
    CubeManager cubeManager;
    ExecutableManager executableManager;

    public StorageGarbageStatsExtractor() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
        executableManager = ExecutableManager.getInstance(kylinConfig);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        File target = new File(exportDir, "storage");
        Map<String, Object> output = Maps.newHashMap();

        Configuration hbaseConf = HBaseConfiguration.create();
        List<String> hbaseTables = collectUnusedHBaseTables(hbaseConf);
        output.put("hbase.table.count", hbaseTables.size());

        Configuration hdfsConf = HadoopUtil.getCurrentConfiguration();
        Map<String, Long> hdfsFiles = collectUnusedHdfsFiles(hdfsConf);
        Map<String, Long> hiveTables = collectUnusedIntermediateHiveTable();

        long hdfsBytes = 0;
        for (Long size : hdfsFiles.values()) {
            hdfsBytes += size;
        }
        long hiveBytes = 0;
        for (Long size : hiveTables.values()) {
            hiveBytes += size;
        }
        output.put("hdfs.file.count", hdfsFiles.size());
        output.put("hdfs.file.byte", hdfsBytes);

        output.put("hive.table.count", hiveTables.size());
        output.put("hive.table.byte", hiveBytes);

        // write json
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(target, output);
    }

    private List<String> collectUnusedHBaseTables(Configuration conf) throws IOException {
        List<String> allTablesNeedToBeDropped = Lists.newArrayList();

        try (HBaseAdmin hbaseAdmin = new HBaseAdmin(conf)) {
            String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
            HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");

            for (HTableDescriptor desc : tableDescriptors) {
                String host = desc.getValue(IRealizationConstants.HTableTag);
                if (KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                    // add all tables now
                    allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
                }
            }

            // remove every segment htable from drop list
            for (CubeInstance cube : cubeManager.listAllCubes()) {
                for (CubeSegment seg : cube.getSegments()) {
                    String tablename = seg.getStorageLocationIdentifier();
                    if (allTablesNeedToBeDropped.contains(tablename)) {
                        allTablesNeedToBeDropped.remove(tablename);
                    }
                }
            }
        }
        return allTablesNeedToBeDropped;
    }

    private Map<String, Long> collectUnusedHdfsFiles(Configuration conf) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());

        FileSystem fs = FileSystem.get(conf);
        Map<String, Long> allHdfsPathsNeedToBeDeleted = Maps.newHashMap();
        FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));
        for (FileStatus status : fStatus) {
            String path = status.getPath().getName();
            if (path.startsWith("kylin-")) {
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + path;
                allHdfsPathsNeedToBeDeleted.put(kylinJobPath, fs.getContentSummary(status.getPath()).getLength());
            }
        }

        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobId);
                allHdfsPathsNeedToBeDeleted.remove(path);
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeManager.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobUuid);
                    allHdfsPathsNeedToBeDeleted.remove(path);
                }
            }
        }

        return allHdfsPathsNeedToBeDeleted;
    }

    private Map<String, Long> collectUnusedIntermediateHiveTable() throws IOException {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final CliCommandExecutor cmdExec = config.getCliCommandExecutor();
        final int uuidLength = 36;

        final String useDatabaseHql = "USE " + config.getHiveDatabaseForIntermediateTable() + ";";
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(useDatabaseHql);
        hiveCmdBuilder.addStatement("show tables " + "\'kylin_intermediate_*\'" + "; ");

        Pair<Integer, String> result = cmdExec.execute(hiveCmdBuilder.build());
        Set<String> allHiveTablesNeedToBeDeleted = Sets.newHashSet();

        String outputStr = result.getSecond();
        try (BufferedReader reader = new BufferedReader(new StringReader(outputStr))) {
            String line = null;
            List<String> allJobs = executableManager.getAllJobIds();
            List<String> workingJobList = new ArrayList<String>();

            for (String jobId : allJobs) {
                // only remove FINISHED and DISCARDED job intermediate table
                final ExecutableState state = executableManager.getOutput(jobId).getState();
                if (!state.isFinalState()) {
                    workingJobList.add(jobId);
                }
            }

            while ((line = reader.readLine()) != null) {
                try {
                    if (line.startsWith("kylin_intermediate_")) {
                        boolean isNeedDel = false;
                        String uuid = line.substring(line.length() - uuidLength, line.length());
                        uuid = uuid.replace("_", "-");
                        //Check whether it's a hive table in use
                        if (allJobs.contains(uuid) && !workingJobList.contains(uuid)) {
                            isNeedDel = true;
                        }

                        if (isNeedDel) {
                            allHiveTablesNeedToBeDeleted.add(line);
                        }
                    }
                } catch (Exception e) {
                    logger.error("illegal hive tmp table: {}", line, e);
                }
            }
        }

        Map<String, Long> resultMap = Maps.newHashMap();
        if (!allHiveTablesNeedToBeDeleted.isEmpty()) {
            final Pattern hiveTableStatsPattern = Pattern.compile(".*\\nTable .* stats: \\[numFiles=\\d*, numRows=\\d*, totalSize=(\\d*), rawDataSize=\\d*\\]\\n.*", Pattern.MULTILINE | Pattern.DOTALL);
            for (String hiveTable : allHiveTablesNeedToBeDeleted) {
                hiveCmdBuilder.reset();
                hiveCmdBuilder.addStatement(String.format("analyze table %s compute statistics noscan;", hiveTable));
                Pair<Integer, String> hiveCmdOutput = cmdExec.execute(hiveCmdBuilder.build());
                if (hiveCmdOutput.getFirst() == 0) {
                    String hiveCmdOutputStr = hiveCmdOutput.getSecond();
                    Matcher matcher = hiveTableStatsPattern.matcher(hiveCmdOutputStr);
                    if (matcher.matches()) {
                        resultMap.put(hiveTable, Long.parseLong(matcher.group(1)));
                    }
                }
            }
        }

        return resultMap;
    }
}
