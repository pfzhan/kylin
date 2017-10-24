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

package io.kyligence.kap.tool.release;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.CubeMigrationCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

public class KapCubeMigrationCLI extends CubeMigrationCLI {

    private static final Logger logger = LoggerFactory.getLogger(KapCubeMigrationCLI.class);
    private static final String MIGRATING_CUBE_ROOT = "migrating_cube_";
    private static final String MIGRATING_CUBE_META = "meta";
    private static final String MIGRATING_CUBE_DATA = "data";

    @Override
    protected void renameFoldersInHdfs(CubeInstance cube) throws IOException {

        if (cube.getDescriptor().getEngineType() == IStorageAware.ID_SHARDED_HBASE)
            super.renameFoldersInHdfs(cube);
        else {
            renameKAPRealizationStoragePath(cube.getUuid());
            RawTableInstance raw = detectRawTable(cube);
            if (null != raw) {
                renameKAPRealizationStoragePath(raw.getUuid());
            }
        }
    }

    @Override
    protected void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot,
            boolean copyAcl) throws IOException {
        super.listCubeRelatedResources(cube, metaResource, dictAndSnapshot, copyAcl);

        RawTableInstance raw = detectRawTable(cube);
        if (null != raw) {
            RawTableDesc desc = raw.getRawTableDesc();
            metaResource.add(raw.getResourcePath());
            metaResource.add(desc.getResourcePath());
        }
    }

    private void renameKAPRealizationStoragePath(String uuid) throws IOException {
        String src = KapConfig.wrap(srcConfig).getParquetStoragePath() + uuid;
        String tgt = KapConfig.wrap(dstConfig).getParquetStoragePath() + uuid;

        Path tgtParent = new Path(KapConfig.wrap(dstConfig).getParquetStoragePath());
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(tgtParent)) {
            fs.mkdirs(tgtParent);
        }
        addOpt(OptType.RENAME_FOLDER_IN_HDFS, new Object[] { src, tgt });
    }

    private RawTableInstance detectRawTable(CubeInstance cube) {
        RawTableInstance rawInstance = RawTableManager.getInstance(srcConfig).getAccompanyRawTable(cube);
        return rawInstance;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        KapCubeMigrationCLI cli = new KapCubeMigrationCLI();

        switch (args.length) {
        case 2:
            if (false == "backup".equalsIgnoreCase(args[0])) {
                cli.usageForBackup();
                break;
            }
            cli.backupCube(args[1]);
            break;
        case 5:
            if (false == "restore".equalsIgnoreCase(args[0])) {
                cli.usageForRestore();
                break;
            }
            cli.restoreCube(args[1], args[2], args[3], args[4]);
            break;
        case 8:
            cli.moveCube(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
            break;
        default:
            cli.usage();
            cli.usageForBackup();
            cli.usageForRestore();
            System.exit(1);
        }
    }

    public void backupCube(String cubeName) throws IOException, InterruptedException {

        File tmp = File.createTempFile("kap_migrating_cube", "");
        FileUtils.forceDelete(tmp);
        File metaDir = new File(tmp, MIGRATING_CUBE_META);
        metaDir.mkdirs();

        logger.info("Cube related metadata is dump to: {}", metaDir.getAbsolutePath());
        srcConfig = KylinConfig.getInstanceFromEnv();
        dstConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        srcStore = ResourceStore.getStore(srcConfig);
        dstStore = ResourceStore.getStore(dstConfig);
        hdfsFS = HadoopUtil.getWorkingFileSystem();
        operations = new ArrayList<>();

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance migratingCube = cubeManager.getCube(cubeName);

        dumpCubeResourceToLocal(migratingCube);
        putLocalCubeResourcesToHDFS(migratingCube, metaDir.getAbsolutePath());

        FileUtils.deleteDirectory(tmp.getAbsoluteFile());
    }

    private void putLocalCubeResourcesToHDFS(CubeInstance cube, String localDir) throws IOException {
        String cubeName = cube.getName();
        String cubeStoragePath = KapConfig.wrap(srcConfig).getParquetStoragePath() + cube.getUuid();
        Path migrating_cube_path = new Path(getMigratingCubeRootPath(cubeName));

        if (hdfsFS.exists(migrating_cube_path))
            hdfsFS.delete(migrating_cube_path, true);

        hdfsFS.mkdirs(migrating_cube_path);
        hdfsFS.mkdirs(new Path(getMigratingCubeDataPath(cubeName)));

        hdfsFS.copyFromLocalFile(new Path(localDir), migrating_cube_path);

        String copyToPath = getMigratingCubeDataPath(cubeName);
        copyCubeOrRaw(cubeName, cubeStoragePath, copyToPath);

        RawTableInstance raw = detectRawTable(cube);
        if (raw != null) {
            String rawTableStoragePath = KapConfig.wrap(srcConfig).getParquetStoragePath() + raw.getUuid();
            copyCubeOrRaw(cubeName, rawTableStoragePath, copyToPath);
        }
    }

    private void copyCubeOrRaw(String cubeName, String copyFrom, String copyTo) throws IOException {
        if (hdfsFS.exists(new Path(copyFrom))) {
            logger.info("Move cube:{} data from: {} to: {}", cubeName, copyFrom, copyTo);
            FileUtil.copy(hdfsFS, new Path(copyFrom), hdfsFS, new Path(copyTo), true,
                    HadoopUtil.getCurrentConfiguration());
        }
    }

    private void dumpCubeResourceToLocal(CubeInstance cube) throws IOException, InterruptedException {
        copyFilesInMetaStore(cube, "true", true);
        doOpts();
        dumpSrcProject(cube.getDescriptor().getProject(), dstStore);
    }

    public void restoreCube(String cubeName, String project, String srcNameNode, String overwrite)
            throws IOException, InterruptedException {
        File tmp = File.createTempFile("kap_migrating_cube", "");
        FileUtils.forceDelete(tmp);
        String localMetaDir = tmp.getAbsolutePath();
        logger.info("Dump cube data to local dir: {}", tmp.getAbsolutePath());

        hdfsFS = HadoopUtil.getWorkingFileSystem();
        dumpRemoteCubeResourceToLocal(srcNameNode, cubeName, localMetaDir);

        dstProject = project;
        srcConfig = KylinConfig.createInstanceFromUri(localMetaDir);
        dstConfig = KylinConfig.getInstanceFromEnv();
        srcStore = ResourceStore.getStore(srcConfig);
        dstStore = ResourceStore.getStore(dstConfig);
        operations = new ArrayList<>();

        addCubeToProject(cubeName, overwrite);

        FileUtils.deleteDirectory(tmp.getAbsoluteFile());
        hdfsFS.delete(new Path(getMigratingCubeRootPath(cubeName)), true);
    }

    private void dumpRemoteCubeResourceToLocal(String srcNameNode, String cubeName, String localDir)
            throws IOException {
        if (null == srcNameNode) {
            throw new IllegalArgumentException("srcNameNode can't be null, it should point to hdfs://src-cluster");
        }

        if (false == "test".equals(srcNameNode)) {
            srcNameNode = srcNameNode.endsWith("/") ? srcNameNode.substring(0, srcNameNode.length() - 1) : srcNameNode;
            String srcPath = srcNameNode + getMigratingCubeRootPath(cubeName);
            Path dstRootCubePath = new Path(getMigratingCubeRootPath(cubeName));

            if (hdfsFS.exists(dstRootCubePath))
                hdfsFS.delete(dstRootCubePath, true);

            distCopyFile(new Path(srcPath), new Path("/tmp"));
        }
        hdfsFS.copyToLocalFile(false, new Path(getMigratingCubeMetaPath(cubeName)), new Path(localDir));
    }

    private void addCubeToProject(String cubeName, String overwrite) throws IOException, InterruptedException {
        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        copyFilesInMetaStore(cube, overwrite, true);
        addCubeAndModelIntoProject(cube, cubeName);
        doOpts();

        String copyFrom = getMigratingCubeDataPath(cubeName) + "/" + cube.getUuid();
        String copyTo = KapConfig.wrap(dstConfig).getParquetStoragePath();
        copyCubeOrRaw(cubeName, copyFrom, copyTo);

        RawTableInstance raw = detectRawTable(cube);
        if (raw != null) {
            copyFrom = getMigratingCubeDataPath(cubeName) + "/" + raw.getUuid();
            copyCubeOrRaw(cubeName, copyFrom, copyTo);
        }

    }

    private void dumpSrcProject(String prj, ResourceStore dstStore) throws IOException {
        Serializer<ProjectInstance> projectSerializer = new JsonSerializer<>(ProjectInstance.class);
        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(prj);
        project.setLastModified(0);
        dstStore.putResource(project.getResourcePath(), project, projectSerializer);
    }

    private String getMigratingCubeRootPath(String cubeName) {
        String path = "/tmp" + File.separator + MIGRATING_CUBE_ROOT + cubeName;
        return path;
    }

    private String getMigratingCubeMetaPath(String cubeName) {
        String path = "/tmp" + File.separator + MIGRATING_CUBE_ROOT + cubeName + File.separator + MIGRATING_CUBE_META;
        return path;
    }

    private String getMigratingCubeDataPath(String cubeName) {
        String path = "/tmp" + File.separator + MIGRATING_CUBE_ROOT + cubeName + File.separator + MIGRATING_CUBE_DATA;
        return path;
    }

    private void distCopyFile(Path localPath, Path remotePath) {
        try {
            logger.info("copy cube files: from {} to {}", localPath, remotePath);
            DistCp distCp = new DistCp(HadoopUtil.getCurrentConfiguration(),
                    new DistCpOptions(Lists.newArrayList(localPath), remotePath));
            distCp.execute();
        } catch (Exception e) {
            logger.info("DistCp Cube from: {} to {} failed", localPath, remotePath);
        }
    }

    public void usageForBackup() {
        System.out.println("Usage: KapCubeMigrationCLI backup cubeName \n" + "For Example: \n"
                + "KapCubeMigrationCLI backup exampleCube \n");
    }

    public void usageForRestore() {
        System.out.println("Usage: KapCubeMigrationCLI restore cubeName dstProject srcHDFSUrl \n" + "For Example: \n"
                + "KapCubeMigrationCLI restore exampleCube targetProject hdfs://example.com overwrite \n");
    }
}
