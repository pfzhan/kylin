package io.kyligence.kap.tool;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.storage.hbase.util.HiveCmdBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class HiveUdfRegister {
    final Logger logger = LoggerFactory.getLogger(HiveUdfRegister.class);
    CliCommandExecutor cliCommandExecutor;
    FileSystem fs;
    String jarHdfsParentPath;
    String hdfsJarPath;
    String classFile;

    public HiveUdfRegister(String classFile) throws IOException {
        this.classFile = classFile;
        this.fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        this.jarHdfsParentPath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "diagnose/";
        this.cliCommandExecutor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        initRegister();
    }

    protected void initRegister() {
        String localJarPath = getLocalJarFilePath(classFile);
        hdfsJarPath = fs.getUri() + jarHdfsParentPath + new File(localJarPath).getName();
        uploadFileToHdfs(localJarPath,hdfsJarPath);
        addJarFileToHiveClassPath(hdfsJarPath);
    }

    private String getLocalJarFilePath(String udfClassPath) {
        String classPath = "";
        try {
            classPath = Class.forName(udfClassPath).getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (ClassNotFoundException e) {
            logger.error("Failed to get jar path" + e);
        }
        return classPath;
    }

    private void uploadFileToHdfs(String sourceFile, String targetFile) {
        try {
            fs.copyFromLocalFile(false, new Path(sourceFile), new Path(targetFile));
        } catch (IOException e) {
            logger.warn("Failed to upload file " + e);
        }
    }

    private void addJarFileToHiveClassPath(String filePath) {
        String addJarHql = "add jar " + filePath;
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(addJarHql);
        try {
            cliCommandExecutor.execute(hiveCmdBuilder.build());
        } catch (IOException e) {
            logger.warn("Failed to add jar file to hive" + e);
        }
    }

    public void deregisterFromHive(String functionName) {
        String dropFunctionHql = "DROP FUNCTION IF EXISTS " + functionName + ";";
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(dropFunctionHql);
        try {
            cliCommandExecutor.execute(hiveCmdBuilder.build());
        } catch (IOException e) {
            logger.warn("Failed to drop function " + e);
        }
    }

    public String registerToHive(String functionName) {
        return "CREATE FUNCTION " + functionName + " AS '" + this.classFile + "' USING JAR '" + this.hdfsJarPath + "';";
    }

}
