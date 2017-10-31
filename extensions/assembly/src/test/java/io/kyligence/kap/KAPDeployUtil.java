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

package io.kyligence.kap;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import io.kyligence.kap.common.util.TempMetadataBuilder;

public class KAPDeployUtil {

    public static void initCliWorkDir() throws IOException {
        execCliCommand("rm -rf " + getHadoopCliWorkingDir());
        execCliCommand("mkdir -p " + config().getKylinJobLogDir());
    }

    public static void deployMetadata() throws IOException {
        // install metadata to hbase
        ResourceTool.reset(config());
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata(true);
        ResourceTool.copy(KylinConfig.createInstanceFromUri(tempMetadataDir), config());
        FileUtils.deleteDirectory(new File(tempMetadataDir));

        // update cube desc signature
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }

    public static void overrideJobJarLocations() {
        File jobJar = getJobJarFile();
        File coprocessorJar = getCoprocessorJarFile();

        config().overrideMRJobJarPath(jobJar.getAbsolutePath());
        config().overrideCoprocessorLocalJar(coprocessorJar.getAbsolutePath());
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
