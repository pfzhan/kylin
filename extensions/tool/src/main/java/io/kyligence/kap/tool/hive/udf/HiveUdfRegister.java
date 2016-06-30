package io.kyligence.kap.tool.hive.udf;

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

    String hdfsJarPath;
    String functionName;

    private final Class classFile;

    public HiveUdfRegister(Class udfClass) throws IOException, IllegalAccessException, InstantiationException {
        Preconditions.checkState(udfClass.isAssignableFrom(AbstractUdf.class));

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        this.classFile = udfClass;
        this.functionName = ((AbstractUdf) udfClass.newInstance()).getFuncName();
        this.fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        this.jarHdfsParentPath = kylinConfig.getHdfsWorkingDirectory() + "diagnosis/";
        this.cliCommandExecutor = kylinConfig.getCliCommandExecutor();

        initRegister();
    }

    protected void initRegister() {
        String localJarPath = getLocalJarFilePath(classFile);
        hdfsJarPath = fs.getUri() + jarHdfsParentPath + new File(localJarPath).getName();
        uploadFileToHdfs(localJarPath, hdfsJarPath);
        addJarFileToHiveClassPath(hdfsJarPath);
    }

    private String getLocalJarFilePath(Class udfClass) {
        String classPath = "";
        classPath = udfClass.getProtectionDomain().getCodeSource().getLocation().getPath();
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

    public void deregisterFromHive() {
        String dropFunctionHql = "DROP FUNCTION IF EXISTS " + functionName + ";";
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(dropFunctionHql);
        try {
            cliCommandExecutor.execute(hiveCmdBuilder.build());
        } catch (IOException e) {
            logger.warn("Failed to drop function " + e);
        }
    }

    public String registerToHive() {
        return "CREATE FUNCTION " + functionName + " AS '" + this.classFile + "' USING JAR '" + this.hdfsJarPath + "';";
    }

}
