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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.CubeMigrationCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

// TODO: Should consider read/write separate
public class KapCubeMigrationCLI extends CubeMigrationCLI {

    private static final Logger logger = LoggerFactory.getLogger(KapCubeMigrationCLI.class);
    private static final String BACKUP_CUBE_ROOT = "backup_cube_";
    private static final String RESTORE_CUBE_ROOT = "restore_cube_";
    private static final String MIGRATING_CUBE_META = "meta";
    private static final String MIGRATING_CUBE_DATA = "data";
    private String type;

    protected static final Option OPTION_CUBE = OptionBuilder.withArgName("cubeName").hasArg().isRequired(false)
            .withDescription("Migration CubeName").create("cubeName");
    protected static final Option OPTION_METADATA = OptionBuilder.withArgName("onlyMetadata").hasArg().isRequired(false)
            .withDescription("Only migrate cube's metadata").create("onlyMetadata");
    protected static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false)
            .withDescription("The dest project that cube is migrating to").create("project");
    protected static final Option OPTION_HDFS_URL = OptionBuilder.withArgName("namenode").hasArg().isRequired(false)
            .withDescription("The HDFS url of source Hadoop cluster").create("namenode");
    protected static final Option OPTION_OVERWRITE = OptionBuilder.withArgName("overwrite").hasArg().isRequired(false)
            .withDescription("Decide If overwriting already exist cube in the dest project").create("overwrite");
    protected static final Option OPTION_SOURCE_URI = OptionBuilder.withArgName("srcUri").hasArg().isRequired(false)
            .withDescription("The source KYLIN config uri").create("srcUri");
    protected static final Option OPTION_DEST_URI = OptionBuilder.withArgName("dstUri").hasArg().isRequired(false)
            .withDescription("The dest KYLIN config uri").create("dstUri");
    protected static final Option OPTION_COPY_ACL = OptionBuilder.withArgName("copyACL").hasArg().isRequired(false)
            .withDescription("If needing to copy ACL").create("copyACL");
    protected static final Option OPTION_PURGE = OptionBuilder.withArgName("purge").hasArg().isRequired(false)
            .withDescription("If purging the cube").create("purge");
    protected static final Option OPTION_EXECUTE = OptionBuilder.withArgName("execute").hasArg().isRequired(false)
            .withDescription("If executing the migration").create("execute");

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
    protected void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot)
            throws IOException {
        super.listCubeRelatedResources(cube, metaResource, dictAndSnapshot);

        RawTableInstance raw = detectRawTable(cube);
        if (null != raw) {
            RawTableDesc desc = raw.getRawTableDesc();
            metaResource.add(raw.getResourcePath());
            metaResource.add(desc.getResourcePath());
        }
    }

    private void renameKAPRealizationStoragePath(String uuid) throws IOException {
        String src = KapConfig.wrap(srcConfig).getReadParquetStoragePath() + uuid;
        String tgt = KapConfig.wrap(dstConfig).getReadParquetStoragePath() + uuid;
        Path tgtParent = new Path(KapConfig.wrap(dstConfig).getReadParquetStoragePath());
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

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_METADATA);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_HDFS_URL);
        options.addOption(OPTION_OVERWRITE);
        options.addOption(OPTION_SOURCE_URI);
        options.addOption(OPTION_DEST_URI);
        options.addOption(OPTION_COPY_ACL);
        options.addOption(OPTION_PURGE);
        options.addOption(OPTION_EXECUTE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("CubeName option value: '" + optionsHelper.getOptionValue(OPTION_CUBE) + "'");
        logger.info("onlyMetadata option value: '" + optionsHelper.getOptionValue(OPTION_METADATA) + "'");
        logger.info("Project option value: '" + optionsHelper.getOptionValue(OPTION_PROJECT) + "'");
        logger.info("HDFS URL option value: '" + optionsHelper.getOptionValue(OPTION_HDFS_URL) + "'");
        logger.info("Overwrite option value: '" + optionsHelper.getOptionValue(OPTION_OVERWRITE) + "'");
        logger.info("Source KYLIN config uri option value: '" + optionsHelper.getOptionValue(OPTION_SOURCE_URI) + "'");
        logger.info("Dest KYLIN config uri option value: '" + optionsHelper.getOptionValue(OPTION_DEST_URI) + "'");
        logger.info("Copy ACL option value: '" + optionsHelper.getOptionValue(OPTION_COPY_ACL) + "'");
        logger.info("Purge option value: '" + optionsHelper.getOptionValue(OPTION_PURGE) + "'");
        logger.info("Execute option value: '" + optionsHelper.getOptionValue(OPTION_EXECUTE) + "'");

        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
        String onlyMeta = optionsHelper.getOptionValue(OPTION_METADATA);
        String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        String url = optionsHelper.getOptionValue(OPTION_HDFS_URL);

        String sourceUri = optionsHelper.getOptionValue(OPTION_SOURCE_URI);
        String destUri = optionsHelper.getOptionValue(OPTION_DEST_URI);
        String purge = optionsHelper.getOptionValue(OPTION_PURGE);
        String execute = optionsHelper.getOptionValue(OPTION_EXECUTE);
        String copyACL = optionsHelper.getOptionValue(OPTION_COPY_ACL);
        String overwrite = optionsHelper.getOptionValue(OPTION_OVERWRITE);

        if ("backup".equalsIgnoreCase(type))
            backupCube(cubeName, onlyMeta, copyACL);
        if ("restore".equalsIgnoreCase(type))
            restoreCube(cubeName, project, url, overwrite);
        if ("copy".equalsIgnoreCase(type))
            moveCube(sourceUri, destUri, cubeName, project, copyACL, purge, overwrite, execute);

    }

    public void doOpts(String[] args) throws Exception {
        this.type = args[0];
        String[] optArgs = new String[args.length - 1];
        for (int i = 0; i < optArgs.length; i++)
            optArgs[i] = args[i + 1];
        execute(optArgs);
    }

    public static void main(String[] args) {

        KapCubeMigrationCLI cli = new KapCubeMigrationCLI();

        if (args.length == 0) {
            cli.usageOptions();
            System.exit(1);
        }

        try {
            cli.doOpts(args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run KapCubeMigrationCLI", e);
        }
    }

    public void backupCube(String cubeName, String onlyMeta, String copyAcl) throws IOException, InterruptedException {

        Preconditions.checkNotNull(cubeName);
        File tmp = File.createTempFile("kap_backup_cube", "");
        FileUtils.forceDelete(tmp);
        File metaDir = new File(tmp, MIGRATING_CUBE_META);
        metaDir.mkdirs();

        boolean doOnlyMeta = Boolean.parseBoolean(onlyMeta);
        doAclCopy = Boolean.parseBoolean(copyAcl);
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
        putLocalCubeResourcesToHDFS(migratingCube, metaDir.getAbsolutePath(), doOnlyMeta);

        FileUtils.deleteDirectory(tmp.getAbsoluteFile());
    }

    private void putLocalCubeResourcesToHDFS(CubeInstance cube, String localDir, boolean metaOnly) throws IOException {
        String cubeName = cube.getName();
        String cubeStoragePath = KapConfig.wrap(srcConfig).getReadParquetStoragePath() + cube.getUuid();
        Path migrating_cube_path = new Path(getMigratingCubeRootPath(BACKUP_CUBE_ROOT, cubeName));

        if (hdfsFS.exists(migrating_cube_path))
            hdfsFS.delete(migrating_cube_path, true);

        hdfsFS.mkdirs(migrating_cube_path);
        hdfsFS.mkdirs(new Path(getMigratingCubeDataPath(BACKUP_CUBE_ROOT, cubeName)));

        hdfsFS.copyFromLocalFile(new Path(localDir), migrating_cube_path);

        if (metaOnly) {
            logger.info("Only migrate cube metadata.");
            return;
        }
        String copyToPath = getMigratingCubeDataPath(BACKUP_CUBE_ROOT, cubeName);
        copyCubeOrRaw(cubeName, cubeStoragePath, copyToPath);

        RawTableInstance raw = detectRawTable(cube);
        if (raw != null) {
            String rawTableStoragePath = KapConfig.wrap(srcConfig).getReadParquetStoragePath() + raw.getUuid();
            copyCubeOrRaw(cubeName, rawTableStoragePath, copyToPath);
        }
    }

    private void copyCubeOrRaw(String cubeName, String copyFrom, String copyTo) throws IOException {
        if (hdfsFS.exists(new Path(copyFrom))) {
            logger.info("Move cube:{} data from: {} to: {}", cubeName, copyFrom, copyTo);
            FileUtil.copy(hdfsFS, new Path(copyFrom), hdfsFS, new Path(copyTo), false,
                    HadoopUtil.getCurrentConfiguration());
        }
    }

    private void dumpCubeResourceToLocal(CubeInstance cube) throws IOException, InterruptedException {
        dumpSrcProject(cube.getDescriptor().getProject(), dstStore);
        copyFilesInMetaStore(cube);
        doOpts();
    }

    public void restoreCube(String cubeName, String project, String srcNameNode, String overwrite)
            throws IOException, InterruptedException {
        Preconditions.checkNotNull(cubeName);
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(srcNameNode);
        Preconditions.checkNotNull(overwrite);
        File tmp = File.createTempFile("kap_restore_cube", "");
        FileUtils.forceDelete(tmp);
        String localMetaDir = tmp.getAbsolutePath();
        logger.info("Dump cube data to local dir: {}", tmp.getAbsolutePath());

        doOverwrite = Boolean.parseBoolean(overwrite);

        hdfsFS = HadoopUtil.getWorkingFileSystem();
        dumpRemoteCubeResourceToLocal(srcNameNode, cubeName, localMetaDir);

        dstProject = project;
        srcConfig = KylinConfig.createInstanceFromUri(localMetaDir);
        dstConfig = KylinConfig.getInstanceFromEnv();
        srcStore = ResourceStore.getStore(srcConfig);
        dstStore = ResourceStore.getStore(dstConfig);
        operations = new ArrayList<>();

        addCubeToProject(cubeName);

        FileUtils.deleteDirectory(tmp.getAbsoluteFile());
        hdfsFS.delete(new Path(getMigratingCubeRootPath(RESTORE_CUBE_ROOT, cubeName)), true);
    }

    private void dumpRemoteCubeResourceToLocal(String srcNameNode, String cubeName, String localDir)
            throws IOException {
        if (null == srcNameNode) {
            throw new IllegalArgumentException("srcNameNode can't be null, it should point to hdfs://src-cluster");
        }

        srcNameNode = srcNameNode.endsWith("/") ? srcNameNode.substring(0, srcNameNode.length() - 1) : srcNameNode;
        String srcMetaPath = srcNameNode + getMigratingCubeMetaPath(BACKUP_CUBE_ROOT, cubeName);
        String srcDataPath = srcNameNode + getMigratingCubeDataPath(BACKUP_CUBE_ROOT, cubeName);
        Path dstRootCubePath = new Path(getMigratingCubeRootPath(RESTORE_CUBE_ROOT, cubeName));

        if (hdfsFS.exists(dstRootCubePath)) {
            hdfsFS.delete(dstRootCubePath, true);
            hdfsFS.mkdirs(dstRootCubePath);
        }

        distCopyFile(new Path(srcMetaPath), dstRootCubePath);
        distCopyFile(new Path(srcDataPath), dstRootCubePath);
        hdfsFS.copyToLocalFile(false, new Path(getMigratingCubeMetaPath(RESTORE_CUBE_ROOT, cubeName)),
                new Path(localDir));
    }

    private void addCubeToProject(String cubeName) throws IOException, InterruptedException {
        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        addCubeAndModelIntoProject(cube, cubeName);
        copyFilesInMetaStore(cube);
        doOpts();

        Path copyTo = new Path(KapConfig.wrap(dstConfig).getReadParquetStoragePath());

        if (!hdfsFS.exists(copyTo))
            hdfsFS.mkdirs(copyTo);

        Path copyFrom = new Path(getMigratingCubeDataPath(RESTORE_CUBE_ROOT, cubeName));
        FileStatus[] status = hdfsFS.listStatus(copyFrom);
        for (FileStatus s : status) {
            logger.info("Copy from: {} to: {}", s.getPath(), copyTo);
            FileUtil.copy(hdfsFS, s.getPath(), hdfsFS, copyTo, true, HadoopUtil.getCurrentConfiguration());
        }
    }

    private void dumpSrcProject(String prj, ResourceStore dstStore) throws IOException {
        Serializer<ProjectInstance> projectSerializer = new JsonSerializer<>(ProjectInstance.class);
        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(prj);
        project.setLastModified(0);
        dstStore.putResource(project.getResourcePath(), project, projectSerializer);
    }

    private String getMigratingCubeRootPath(String root, String cubeName) {
        String path = "/tmp" + File.separator + root + cubeName;
        return path;
    }

    private String getMigratingCubeMetaPath(String root, String cubeName) {
        String path = "/tmp" + File.separator + root + cubeName + File.separator + MIGRATING_CUBE_META;
        return path;
    }

    private String getMigratingCubeDataPath(String root, String cubeName) {
        String path = "/tmp" + File.separator + root + cubeName + File.separator + MIGRATING_CUBE_DATA;
        return path;
    }

    private void distCopyFile(Path sourcePath, Path remotePath) {
        try {
            logger.info("copy cube files: from {} to {}", sourcePath, remotePath);
            DistCp distCp = new DistCp(HadoopUtil.getCurrentConfiguration(),
                    new DistCpOptions(Lists.newArrayList(sourcePath), remotePath));
            distCp.execute();
        } catch (Exception e) {
            logger.info("DistCp Cube from: {} to {} failed", sourcePath, remotePath);
        }
    }

    public void usageOptions() {
        System.out.println("Usages:");
        System.out.println("KapCubeMigrationCLI backup --cubeName someCube --onlyMetadata true");
        System.out.println(
                "KapCubeMigrationCLI restore --cubeName someCube --project someProject --namenode hdfs://someip --overwrite true");
        System.out.println(
                "KapCubeMigrationCLI move --srcUri someKylinUri --dstUri someDestKylinUri --cubeName someCube --project someProject --copyAcl false --purge true --overwrite true --execute true");
    }
}
