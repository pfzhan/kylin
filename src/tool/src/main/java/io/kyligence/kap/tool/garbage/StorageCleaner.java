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
package io.kyligence.kap.tool.garbage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleaner {

    @Getter
    private Set<StorageItem> outdatedItems = Sets.newHashSet();

    private Set<StorageItem> allFileSystems = Sets.newHashSet();

    public void execute() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        val projects = NProjectManager.getInstance(config).listAllProjects();
        for (ProjectInstance project : projects) {
            val dataflows = NDataflowManager.getInstance(config, project.getName()).listAllDataflows();
            for (NDataflow dataflow : dataflows) {
                KapConfig kapConfig = KapConfig.wrap(dataflow.getConfig());
                String hdfsWorkingDir = kapConfig.getReadHdfsWorkingDirectory();
                val fs = HadoopUtil.getFileSystem(hdfsWorkingDir);
                allFileSystems.add(new StorageItem(fs, hdfsWorkingDir));
            }
        }
        allFileSystems.add(new StorageItem(HadoopUtil.getFileSystem(new Path(config.getHdfsWorkingDirectory())),
                config.getHdfsWorkingDirectory()));
        log.info("all file systems are {}", allFileSystems);
        collectDeletedProject();
        for (ProjectInstance project : projects) {
            collect(project.getName());
        }

        cleanup();
    }

    public void collectDeletedProject() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        val projects = NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toSet());
        for (StorageItem item : allFileSystems) {
            for (FileStatus projectFile : listFileStatus(item.getFs(), new Path(item.getPath()))) {
                val projectName = projectFile.getPath().getName();
                if (projectName.startsWith("_") || projectName.startsWith(".")) {
                    continue;
                }
                if (!projects.contains(projectName)) {
                    addItem(item.getFs(), projectFile);
                }
            }
        }
    }

    public void collect(String project) throws IOException {
        log.info("collect garbage for {}", project);
        collectJobTmp(project);
        collectDeletedDataflow(project);
        collectIndexData(project);
    }

    public void cleanup() throws IOException {
        log.debug("start cleanup garbage on HDFS");
        for (StorageItem item : outdatedItems) {
            log.debug("try to delete {}", item.getPath());
            item.getFs().delete(new Path(item.getPath()), true);
        }
    }

    private void collectJobTmp(String project) throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        val executableManager = NExecutableManager.getInstance(config, project);
        Set<String> activeJobs = executableManager.getAllExecutables().stream().map(AbstractExecutable::getId)
                .collect(Collectors.toSet());
        val fs = HadoopUtil.getWorkingFileSystem();
        val jobTmpPath = new Path(config.getJobTmpDir(project));
        for (FileStatus fileStatus : listFileStatus(fs, jobTmpPath)) {
            String jobDir = fileStatus.getPath().getName();
            if (activeJobs.contains(jobDir)) {
                continue;
            }
            addItem(fs, fileStatus);
        }
    }

    private void collectDeletedDataflow(String project) throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        for (StorageItem item : allFileSystems) {
            val dataflows = NDataflowManager.getInstance(config, project).listAllDataflows().stream()
                    .map(RootPersistentEntity::getId).collect(Collectors.toSet());
            for (FileStatus dataflowFile : listFileStatus(item.getFs(),
                    new Path(getDataflowBaseDir(item.getPath(), project)))) {
                if (!dataflows.contains(dataflowFile.getPath().getName())) {
                    addItem(item.getFs(), dataflowFile);
                }
            }
        }
    }

    private void collectIndexData(String project) throws IOException {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val activeIndexDataPath = Sets.<Path> newHashSet();
        val dataflowDirs = Sets.<String> newHashSet();
        dataflowManager.listAllDataflows().forEach(dataflow -> {
            KapConfig config = KapConfig.wrap(dataflow.getConfig());
            String hdfsWorkingDir = config.getReadHdfsWorkingDirectory();
            dataflowDirs.add(getDataflowDir(hdfsWorkingDir, project, dataflow.getUuid()));
            dataflow.getSegments().stream() //
                    .flatMap(segment -> segment.getLayoutsMap().values().stream()) //
                    .map(this::getDataLayoutDir).map(Path::new).forEach(activeIndexDataPath::add);
        });

        val activeSegmentPath = activeIndexDataPath.stream().map(Path::getParent).collect(Collectors.toSet());

        for (val dataflowDir : dataflowDirs) {
            val fs = HadoopUtil.getFileSystem(dataflowDir);
            for (FileStatus fileStatus : listFileStatus(fs, new Path(dataflowDir))) {
                val segmentDir = fileStatus.getPath();
                if (!activeSegmentPath.contains(segmentDir)) {
                    addItem(fs, fileStatus);
                    continue;
                }
                for (FileStatus status : listFileStatus(fs, segmentDir)) {
                    val layoutDir = status.getPath();
                    if (!activeIndexDataPath.contains(layoutDir)) {
                        addItem(fs, status);
                    }
                }
            }
        }

        for (StorageItem item : allFileSystems) {
            collectDictAndSnapshot(item.getPath(), project);
        }
    }

    private void collectDictAndSnapshot(String workingDir, String project) throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        val tableManager = NTableMetadataManager.getInstance(config, project);
        val activeDictDir = Sets.<Path> newHashSet();
        tableManager.listAllTables()
                .forEach(table -> Arrays.stream(table.getColumns())
                        .map(column -> new Path(
                                getDictDir(workingDir, project) + "/" + table.getIdentity() + "/" + column.getName()))
                        .forEach(activeDictDir::add));
        val activeDictTableDir = activeDictDir.stream().map(Path::getParent).collect(Collectors.toSet());

        val fs = HadoopUtil.getFileSystem(workingDir);
        for (FileStatus fileStatus : listFileStatus(fs, new Path(getDictDir(workingDir, project)))) {
            if (!activeDictTableDir.contains(fileStatus.getPath())) {
                addItem(fs, fileStatus);
                continue;
            }
            for (FileStatus status : listFileStatus(fs, fileStatus.getPath())) {
                val columnDictDir = status.getPath();
                if (!activeDictDir.contains(columnDictDir)) {
                    addItem(fs, status);
                }
            }
        }

        val activeSnapshotPath = Sets.<Path> newHashSet();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        dataflowManager.listAllDataflows()
                .forEach(dataflow -> dataflow.getSegments().stream()
                        .flatMap(segment -> segment.getSnapshots().values().stream())
                        .map(subPath -> new Path(workingDir, subPath)).forEach(activeSnapshotPath::add));
        val activeSnapshotTablePath = activeSnapshotPath.stream().map(Path::getParent).collect(Collectors.toSet());

        for (FileStatus fileStatus : listFileStatus(fs, new Path(getSnapshotDir(workingDir, project)))) {
            if (!activeSnapshotTablePath.contains(fileStatus.getPath())) {
                addItem(fs, fileStatus);
                continue;
            }
            for (FileStatus status : listFileStatus(fs, fileStatus.getPath())) {
                if (!activeSnapshotPath.contains(status.getPath())) {
                    addItem(fs, status);
                }
            }
        }
    }

    private void addItem(FileSystem fs, FileStatus status) {
        val config = KylinConfig.getInstanceFromEnv();
        if (status.getPath().getName().startsWith(".")) {
            return;
        }
        if (status.getModificationTime() + config.getCuboidLayoutSurvivalTimeThreshold() > System.currentTimeMillis()) {
            return;
        }
        outdatedItems.add(new StorageItem(fs, status.getPath().toString()));
    }

    private FileStatus[] listFileStatus(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return new FileStatus[0];
        }
        return fs.listStatus(path);
    }

    private String getDataflowBaseDir(String workingDir, String project) {
        return workingDir + project + "/parquet/";
    }

    private String getDataflowDir(String workingDir, String project, String dataflowId) {
        return getDataflowBaseDir(workingDir, project) + dataflowId;
    }

    private String getSnapshotDir(String workingDir, String project) {
        return workingDir + project + ResourceStore.SNAPSHOT_RESOURCE_ROOT;
    }

    public String getDictDir(String workingDir, String project) {
        return workingDir + project + ResourceStore.GLOBAL_DICT_RESOURCE_ROOT;
    }

    public String getDataLayoutDir(NDataLayout dataLayout) {
        NDataSegDetails segDetails = dataLayout.getSegDetails();
        KapConfig config = KapConfig.wrap(dataLayout.getConfig());
        String hdfsWorkingDir = config.getReadHdfsWorkingDirectory();
        return getDataflowDir(hdfsWorkingDir, segDetails.getProject(),
                segDetails.getDataSegment().getDataflow().getId()) + "/" + segDetails.getUuid() + "/"
                + dataLayout.getLayoutId();
    }

    @Data
    @Builder
    public static class StorageItem {

        private FileSystem fs;

        private String path;

    }
}
