package io.kyligence.kap.tool.kybot.hive.udf;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.storage.hbase.util.HiveCmdBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HiveUdfRegister {
    private static final Logger logger = LoggerFactory.getLogger(HiveUdfRegister.class);

    private final CliCommandExecutor cliCommandExecutor;
    private final FileSystem fs;
    private final String jarHdfsParentPath;
    private final Class udfClass;
    String hdfsJarPath;
    String functionName;

    public HiveUdfRegister(Class udfClass) throws IOException, IllegalAccessException, InstantiationException {
        Preconditions.checkState(AbstractUdf.class.isAssignableFrom(udfClass));

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        this.udfClass = udfClass;
        this.functionName = ((AbstractUdf) udfClass.newInstance()).getFuncName();
        this.fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        this.jarHdfsParentPath = kylinConfig.getHdfsWorkingDirectory() + "diagnosis/";
        this.cliCommandExecutor = kylinConfig.getCliCommandExecutor();
    }

    public void register() {
        String localJarPath = getLocalJarFilePath(udfClass);
        hdfsJarPath = fs.getUri() + jarHdfsParentPath + new File(localJarPath).getName();
        uploadFileToHdfs(localJarPath, hdfsJarPath);
        addJarFileToHiveClassPath(hdfsJarPath);

        logger.info("Hive HDF registered.");
    }

    private String getLocalJarFilePath(Class udfClass) {
        return udfClass.getProtectionDomain().getCodeSource().getLocation().getPath();
    }

    private void uploadFileToHdfs(String sourceFile, String targetFile) {
        try {
            fs.copyFromLocalFile(false, new Path(sourceFile), new Path(targetFile));
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload file.", e);
        }
    }

    private void addJarFileToHiveClassPath(String filePath) {
        String addJarHql = "add jar " + filePath;
        logger.info("Hive Cmd: {}", addJarHql);

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(addJarHql);
        try {
            cliCommandExecutor.execute(hiveCmdBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException("Failed to add jar file to hive.", e);
        }
    }

    public void unregister() {
        String dropFunctionHql = "DROP TEMPORARY FUNCTION IF EXISTS " + functionName + ";";
        logger.info("Hive Cmd: {}", dropFunctionHql);

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(dropFunctionHql);
        try {
            cliCommandExecutor.execute(hiveCmdBuilder.build());
        } catch (IOException e) {
            logger.error("Failed to drop function, please manually drop it in hive. ", e);
        }

        logger.info("Hive HDF unregistered.");
    }

    public String getCreateFuncStatement() {
        return "CREATE TEMPORARY FUNCTION " + functionName + " AS '" + udfClass.getName() + "' USING JAR '" + this.hdfsJarPath + "';";
    }

}
