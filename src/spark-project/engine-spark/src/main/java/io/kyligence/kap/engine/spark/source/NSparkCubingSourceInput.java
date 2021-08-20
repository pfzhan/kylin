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
package io.kyligence.kap.engine.spark.source;

import static org.apache.kylin.common.exception.ServerErrorCode.READ_TRANSACTIONAL_TBALE_FAILED;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;

public class NSparkCubingSourceInput implements NSparkCubingEngine.NSparkCubingSource {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingSourceInput.class);

    @Override
    public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> getSourceData) {
        ColumnDesc[] columnDescs = table.getColumns();
        List<String> tblColNames = Lists.newArrayListWithCapacity(columnDescs.length);
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : columnDescs) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(),
                        SparderTypeUtil.toSparkType(columnDesc.getType(), false), true);
                tblColNames.add("`" + columnDesc.getName() + "`");
            }
        }
        String[] colNames = tblColNames.toArray(new String[0]);
        String colString = Joiner.on(",").join(colNames);
        String sql;
        KylinBuildEnv kylinBuildEnv = KylinBuildEnv.get();
        String jobId = kylinBuildEnv.buildJobInfos().getJobId();
        String project = kylinBuildEnv.buildJobInfos().getProject();
        KylinConfig kylinConfig = kylinBuildEnv.kylinConfig();
        if (table.isTransactional() && kylinConfig.isReadTransactionalTableEnabled() && !kylinConfig.isUTEnv()) {
            String dir = kylinConfig.getJobTmpTransactionalTableDir(project, jobId);
            String tableDir = getTableDir(table.getTransactionalTableIdentity(), dir);
            boolean firstCheck = checkInterTableExist(tableDir);
            logger.info("first check is table ready : {} ", firstCheck);
            if (!firstCheck) {
                generateTxTable(table);
            }
            boolean secondCheck = checkInterTableExist(tableDir);
            logger.info("second check is table ready : {} ", secondCheck);
            if (secondCheck) {
                sql = String.format(Locale.ROOT, "select %s from %s", colString, table.getTransactionalTableIdentity());
            } else {
                throw new KylinException(READ_TRANSACTIONAL_TBALE_FAILED,
                        String.format(Locale.ROOT, "Can't read transactional table, jobId %s.", jobId));
            }
        } else {
            sql = String.format(Locale.ROOT, "select %s from %s", colString, table.getIdentity());
        }
        Dataset<Row> df = ss.sql(sql);
        StructType sparkSchema = df.schema();
        logger.debug("Source data sql is: {}", sql);
        logger.debug("Kylin schema: {}", kylinSchema.treeString());
        return df.select(SparderTypeUtil.alignDataType(sparkSchema, kylinSchema));
    }

    public static final String QUOTE = "`";

    public static String quote(String identifier) {
        return QUOTE + identifier + QUOTE;
    }

    public static String generateHiveInitStatements(String flatTableDatabase) {
        return "USE " + flatTableDatabase + ";\n";
    }

    public static String generateInsertDataStatement(ColumnDesc[] columnDescs, String originTable, String interTable) {
        final String sep = "\n";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);
        for (int i = 0; i < columnDescs.length; i++) {
            ColumnDesc col = columnDescs[i];
            if (i > 0) {
                sql.append(",");
            }
            sql.append(quote(col.getName())).append(sep);
        }
        sql.append("FROM ").append(quote(originTable)).append(" ").append(sep);
        return "INSERT OVERWRITE TABLE " + quote(interTable) + " " + sql.toString() + ";\n";
    }

    public static String getCreateTableStatement(String originTable, String interTable, ColumnDesc[] columnDescs,
            String storageDfsDir, String storageFormat, String fieldDelimiter) {
        return generateDropTableStatement(interTable)
                + generateCreateTableStatement(interTable, storageDfsDir, columnDescs, storageFormat, fieldDelimiter)
                + generateInsertDataStatement(columnDescs, originTable, interTable);
    }

    public static String generateDropTableStatement(String interTable) {
        return "DROP TABLE IF EXISTS " + quote(interTable) + ";" + "\n";
    }

    public static String generateCreateTableStatement(String interTable, String storageDfsDir, ColumnDesc[] columnDescs,
            String storageFormat, String fieldDelimiter) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(quote(interTable)).append("\n");
        ddl.append("(" + "\n");
        for (int i = 0; i < columnDescs.length; i++) {
            ColumnDesc col = columnDescs[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(quote(col.getName())).append(" ").append(getHiveDataType(col.getDatatype())).append("\n");
        }
        ddl.append(")" + "\n");
        if ("TEXTFILE".equalsIgnoreCase(storageFormat)) {
            ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
                    .append(StringEscapeUtils.escapeJava(fieldDelimiter)).append("'\n");
        }
        ddl.append("STORED AS ").append(storageFormat).append("\n");
        ddl.append("LOCATION '").append(getTableDir(interTable, storageDfsDir)).append("';").append("\n");
        ddl.append("ALTER TABLE ").append(quote(interTable)).append(" SET TBLPROPERTIES('auto.purge'='true');\n");
        return ddl.toString();
    }

    public static String getTableDir(String tableName, String storageDfsDir) {
        return storageDfsDir + "/" + tableName;
    }

    public static String getHiveDataType(String javaDataType) {
        String originDataType = javaDataType.toLowerCase(Locale.ROOT);
        String hiveDataType;
        if (originDataType.startsWith("varchar")) {
            hiveDataType = "string";
        } else if (originDataType.startsWith("integer")) {
            hiveDataType = "int";
        } else if (originDataType.startsWith("bigint")) {
            hiveDataType = "bigint";
        } else {
            hiveDataType = originDataType;
        }

        return hiveDataType;
    }

    private boolean checkInterTableExist(String tableDir) {
        try {
            logger.info("check intermediate table dir : {}", tableDir);
            Path path = new Path(tableDir);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                return true;
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return false;
    }

    private void generateTxTable(TableDesc table) {
        KylinBuildEnv kylinBuildEnv = KylinBuildEnv.get();
        String jobId = kylinBuildEnv.buildJobInfos().getJobId();
        logger.info("job wait for generate intermediate table, job id : " + jobId);
        String project = kylinBuildEnv.buildJobInfos().getProject();
        KylinConfig config = kylinBuildEnv.kylinConfig();
        String database = table.getCaseSensitiveDatabase().endsWith("null") ? "default"
                : table.getCaseSensitiveDatabase();
        ColumnDesc[] filtered = Arrays.stream(table.getColumns()).filter(t -> !t.isComputedColumn())
                .toArray(ColumnDesc[]::new);

        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(config);
        hiveCmdBuilder.addStatement(generateHiveInitStatements(database));
        hiveCmdBuilder.addStatement(getCreateTableStatement(table.getIdentity(), table.getTransactionalTableIdentity(),
                filtered, config.getJobTmpTransactionalTableDir(project, jobId), config.getFlatTableStorageFormat(),
                config.getFlatTableFieldDelimiter()));
        final String cmd = hiveCmdBuilder.toString();
        CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
        try {
            CliCommandExecutor.CliCmdExecResult result = cliCommandExecutor.execute(cmd, null);
            if (result.getCode() != 0) {
                logger.error("execute create intermediate table return fail, jobId : {}", jobId);
            }
        } catch (ShellException e) {
            logger.error("failed to execute create intermediate table, jobId : {}, result : {}", jobId, e);
        }
    }

}
