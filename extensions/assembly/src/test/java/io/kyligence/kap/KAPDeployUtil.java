package io.kyligence.kap;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class KAPDeployUtil {
    private static final Logger logger = LoggerFactory.getLogger(KAPDeployUtil.class);

    public static void initCliWorkDir() throws IOException {
        execCliCommand("rm -rf " + getHadoopCliWorkingDir());
        execCliCommand("mkdir -p " + config().getKylinJobLogDir());
    }

    public static void deployMetadata() throws IOException {
        // install metadata to hbase
        ResourceTool.reset(config());
        ResourceTool.copy(KylinConfig.createInstanceFromUri(LocalFileMetadataTestCase.KYLIN_META_TEST_DATA), config());
        ResourceTool.copy(KylinConfig.createInstanceFromUri(LocalFileMetadataTestCase.KAP_META_TEST_DATA), config());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }

    public static void overrideJobJarLocations() {
        File jobJar = getJobJarFile();
        File coprocessorJar = getCoprocessorJarFile();

        config().overrideMRJobJarPath(jobJar.getAbsolutePath());
        config().overrideCoprocessorLocalJar(coprocessorJar.getAbsolutePath());
        config().overrideSparkJobJarPath(getSparkJobJarFile().getAbsolutePath());
    }

    private static String getPomVersion() {
        try {
            MavenXpp3Reader pomReader = new MavenXpp3Reader();
            Model model = pomReader.read(new FileReader("../../pom.xml"));
            return model.getVersion();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static File getJobJarFile() {
        return new File("../assembly/target", "kap-assembly-" + getPomVersion() + "-job.jar");
    }

    private static File getCoprocessorJarFile() {
        return new File("../storage-hbase/target", "kap-storage-hbase-" + getPomVersion() + "-coprocessor.jar");
    }

    private static File getSparkJobJarFile() {
        return new File("../engine-spark/target", "kap-engine-spark-" + getPomVersion() + "-job.jar");
    }

    private static void execCliCommand(String cmd) throws IOException {
        config().getCliCommandExecutor().execute(cmd);
    }

    private static String getHadoopCliWorkingDir() {
        return config().getCliWorkingDir();
    }

    private static KylinConfig config() {
        return KylinConfig.getInstanceFromEnv();
    }
}
