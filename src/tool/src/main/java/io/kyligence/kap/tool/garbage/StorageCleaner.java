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

import static org.apache.kylin.common.util.HadoopUtil.GLOBAL_DICT_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.JOB_TMP_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.PARQUET_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.SNAPSHOT_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.TABLE_EXD_STORAGE_ROOT;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.JobRelatedEvent;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleaner {
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    private final boolean cleanup;
    private final Collection<String> projectNames;
    private long duration;

    public StorageCleaner() {
        this(true);
    }

    public StorageCleaner(boolean cleanup) {
        this(cleanup, Collections.emptyList());
    }

    public StorageCleaner(boolean cleanup, Collection<String> projects) {
        this.cleanup = cleanup;
        this.projectNames = projects;
    }

    @Getter
    private Set<StorageItem> outdatedItems = Sets.newHashSet();

    private Set<StorageItem> allFileSystems = Sets.newHashSet();

    public void execute() throws Exception {
        long start = System.currentTimeMillis();
        val config = KylinConfig.getInstanceFromEnv();
        long startTime = System.currentTimeMillis();
        val projects = NProjectManager.getInstance(config).listAllProjects()
                .stream().filter(projectInstance -> projectNames.isEmpty() || projectNames.contains(projectInstance.getName()))
                .collect(Collectors.toList());

        for (ProjectInstance project : projects) {
            val dataflows = NDataflowManager.getInstance(config, project.getName()).listAllDataflows();
            for (NDataflow dataflow : dataflows) {
                KapConfig kapConfig = KapConfig.wrap(dataflow.getConfig());
                String hdfsWorkingDir = kapConfig.getReadHdfsWorkingDirectory();
                val fs = HadoopUtil.getWorkingFileSystem();
                allFileSystems.add(new StorageItem(fs, hdfsWorkingDir));
            }
        }
        allFileSystems.add(new StorageItem(HadoopUtil.getWorkingFileSystem(), config.getHdfsWorkingDirectory()));
        log.info("all file systems are {}", allFileSystems);
        for (StorageItem allFileSystem : allFileSystems) {
            collectFromHDFS(allFileSystem);
        }
        UnitOfAllWorks.doInTransaction(() -> {
            collectDeletedProject();
            for (ProjectInstance project : projects) {
                collect(project.getName());
            }
            return null;
        }, true);

        long protectionTime = startTime - config.getCuboidLayoutSurvivalTimeThreshold();
        for (StorageItem item : allFileSystems) {
            for (FileTreeNode node : item.getAllNodes()) {
                val path = new Path(item.getPath(), node.getRelativePath());
                try {
                    addItem(item.getFs(), path, protectionTime);
                } catch (FileNotFoundException e) {
                    log.warn("{} not found", path);
                }
            }
        }
        boolean allSuccess = cleanup();
        duration = System.currentTimeMillis() - start;
        printConsole(allSuccess);
    }

    public void printConsole(boolean success) {
        System.out.println(ANSI_BLUE + "Kyligence Enterprise garbage report: (cleanup=" + cleanup + ")" + ANSI_RESET);
        for (StorageItem item : outdatedItems) {
            System.out.println("  Storage File: " + item.getPath());
        }
        String jobName = "Storage GC cleanup job ";
        if (!cleanup) {
            System.out.println(ANSI_BLUE + "Dry run mode, no data is deleted." + ANSI_RESET);
            jobName = "Storage GC check job ";
        }
        if (!success) {
            System.out.println(ANSI_RED + jobName + "FAILED." + ANSI_RESET);
            System.out.println(ANSI_RED + jobName + "finished in " + duration + " ms." + ANSI_RESET);
        } else {
            System.out.println(ANSI_GREEN + jobName + "SUCCEED." + ANSI_RESET);
            System.out.println(ANSI_GREEN + jobName + "finished in " + duration + " ms." + ANSI_RESET);
        }

    }

    public void collectDeletedProject() {
        val config = KylinConfig.getInstanceFromEnv();
        val projects = NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toSet());
        for (StorageItem item : allFileSystems) {
            item.getProjectNodes().removeIf(node -> projects.contains(node.getName()));
        }
    }

    public void collect(String project) {
        log.info("collect garbage for {}", project);
        val projectCleaner = new ProjectStorageCleaner(project);
        projectCleaner.execute();
    }

    public boolean cleanup() {
        boolean success = true;
        if (cleanup) {
            Stats stats = new Stats() {
                @Override
                public void heartBeat() {
                    double percent = 100D * (successItems.size() + errorItems.size()) / allItems.size();
                    String logInfo = String.format(
                            "Progress: %2.1f%%, %d resource, %d error", percent, allItems.size(), errorItems.size());
                    System.out.println(logInfo);
                }
            };
            stats.onAllStart(outdatedItems);
            for (StorageItem item : outdatedItems) {
                log.debug("try to delete {}", item.getPath());
                try {
                    stats.onItemStart(item);
                    item.getFs().delete(new Path(item.getPath()), true);
                    stats.onItemSuccess(item);
                } catch (IOException e) {
                    log.error("delete file " + item.getPath() + " failed", e);
                    stats.onItemError(item);
                    success = false;
                }
            }
        }
        return success;
    }

    private String getDataflowBaseDir(String project) {
        return project + PARQUET_STORAGE_ROOT + "/";
    }

    private String getDataflowDir(String project, String dataflowId) {
        return getDataflowBaseDir(project) + dataflowId;
    }

    class ProjectStorageCleaner {

        private final String project;

        private final Set<String> dependentFiles = Sets.newTreeSet();

        ProjectStorageCleaner(String project) {
            this.project = project;
        }

        public void execute() {
            val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            eventDao.getEvents().stream().filter(event -> event instanceof JobRelatedEvent)
                    .map(event -> manager.getJob(((JobRelatedEvent) event).getJobId())).filter(Objects::nonNull)
                    .map(AbstractExecutable::getDependentFiles).forEach(dependentFiles::addAll);
            collectJobTmp(project);
            collectDataflow(project);
            collectTable(project);

            for (StorageItem item : allFileSystems) {
                for (List<FileTreeNode> nodes : item.getProject(project).getAllCandidates()) {
                    for (FileTreeNode node : nodes) {
                        log.debug("find candidate /{}", node.getRelativePath());
                    }
                }
            }
            for (String dependentFile : dependentFiles) {
                log.debug("remove candidate {}", dependentFile);
            }
            removeDependentFiles();
        }

        private void removeDependentFiles() {
            for (StorageItem item : allFileSystems) {
                for (List<FileTreeNode> nodes : item.getProject(project).getAllCandidates()) {
                    // protect parent folder and
                    nodes.removeIf(
                            node -> dependentFiles.stream().anyMatch(df -> ("/" + node.getRelativePath()).startsWith(df)
                                    || df.startsWith("/" + node.getRelativePath())));
                }
            }
        }

        private void collectJobTmp(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val executableManager = NExecutableManager.getInstance(config, project);
            Set<String> activeJobs = executableManager.getAllExecutables().stream()
                    .map(e -> project + JOB_TMP_ROOT + "/" + e.getId()).collect(Collectors.toSet());
            for (StorageItem item : allFileSystems) {
                item.getProject(project).getJobTmps().removeIf(node -> activeJobs.contains(node.getRelativePath()));
            }
        }

        private void collectDataflow(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val activeIndexDataPath = Sets.<String> newHashSet();
            val dataflows = NDataflowManager.getInstance(config, project).listAllDataflows().stream()
                    .map(RootPersistentEntity::getId).collect(Collectors.toSet());
            dataflowManager.listAllDataflows().forEach(dataflow -> dataflow.getSegments().stream() //
                    .flatMap(segment -> segment.getLayoutsMap().values().stream()) //
                    .map(StorageCleaner.this::getDataLayoutDir).forEach(activeIndexDataPath::add));

            val activeSegmentPath = activeIndexDataPath.stream().map(s -> new File(s).getParent())
                    .collect(Collectors.toSet());
            for (StorageCleaner.StorageItem item : allFileSystems) {
                item.getProject(project).getDataflows().removeIf(node -> dataflows.contains(node.getName()));
                item.getProject(project).getSegments()
                        .removeIf(node -> activeSegmentPath.contains(node.getRelativePath()));
                item.getProject(project).getLayouts()
                        .removeIf(node -> activeIndexDataPath.contains(node.getRelativePath()));
            }
        }

        private void collectTable(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val tableManager = NTableMetadataManager.getInstance(config, project);
            val activeDictDir = Sets.<String> newHashSet();
            val activeTableExdDir = Sets.<String> newHashSet();
            val activeDictTableDir = Sets.<String> newHashSet();
            val activeSnapshotTableDir = Sets.<String> newHashSet();
            tableManager.listAllTables().forEach(table -> {
                Arrays.stream(table.getColumns())
                        .map(column -> getDictDir(project) + "/" + table.getIdentity() + "/" + column.getName())
                        .forEach(activeDictDir::add);
                activeTableExdDir.add(project + ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + table.getIdentity());
                activeSnapshotTableDir.add(project + SNAPSHOT_STORAGE_ROOT + "/" + table.getIdentity());
                activeDictTableDir.add(getDictDir(project) + "/" + table.getIdentity());
            });

            val activeSnapshotDir = Sets.<String> newHashSet();
            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            dataflowManager.listAllDataflows().forEach(dataflow -> dataflow.getSegments().stream()
                    .flatMap(segment -> segment.getSnapshots().values().stream()).forEach(activeSnapshotDir::add));

            for (StorageCleaner.StorageItem item : allFileSystems) {
                item.getProject(project).getGlobalDictTables()
                        .removeIf(node -> activeDictTableDir.contains(node.getRelativePath()));
                item.getProject(project).getGlobalDictColumns()
                        .removeIf(node -> activeDictDir.contains(node.getRelativePath()));
                item.getProject(project).getSnapshots()
                        .removeIf(node -> activeSnapshotDir.contains(node.getRelativePath()));
                item.getProject(project).getSnapshotTables()
                        .removeIf(node -> activeSnapshotTableDir.contains(node.getRelativePath()));
                item.getProject(project).getTableExds()
                        .removeIf(node -> activeTableExdDir.contains(node.getRelativePath()));
            }
        }
    }

    private void addItem(FileSystem fs, Path itemPath, long protectionTime) throws Exception {
        val status = fs.getFileStatus(itemPath);
        if (status.getPath().getName().startsWith(".")) {
            return;
        }
        if (status.getModificationTime() > protectionTime) {
            return;
        }
        outdatedItems.add(new StorageCleaner.StorageItem(fs, status.getPath().toString()));
    }

    private String getDictDir(String project) {
        return project + GLOBAL_DICT_STORAGE_ROOT;
    }

    private String getDataLayoutDir(NDataLayout dataLayout) {
        NDataSegDetails segDetails = dataLayout.getSegDetails();
        return getDataflowDir(segDetails.getProject(), segDetails.getDataSegment().getDataflow().getId()) + "/"
                + segDetails.getUuid() + "/" + dataLayout.getLayoutId();
    }

    private void collectFromHDFS(StorageItem item) throws Exception {
        val projectFolders = item.getFs().listStatus(new Path(item.getPath()), path -> !path.getName().startsWith("_")
                && (this.projectNames.isEmpty() || this.projectNames.contains(path.getName())));
        for (FileStatus projectFolder : projectFolders) {
            List<FileTreeNode> tableSnapshotParents = Lists.newArrayList();
            val projectNode = new ProjectFileTreeNode(projectFolder.getPath().getName());
            for (Pair<String, List<FileTreeNode>> pair : Arrays.asList(
                    Pair.newPair(JOB_TMP_ROOT.substring(1), projectNode.getJobTmps()),
                    Pair.newPair(GLOBAL_DICT_STORAGE_ROOT.substring(1), projectNode.getGlobalDictTables()),
                    Pair.newPair(PARQUET_STORAGE_ROOT.substring(1), projectNode.getDataflows()),
                    Pair.newPair(TABLE_EXD_STORAGE_ROOT.substring(1), projectNode.getTableExds()),
                    Pair.newPair(SNAPSHOT_STORAGE_ROOT.substring(1), tableSnapshotParents))) {
                val treeNode = new FileTreeNode(pair.getFirst(), projectNode);
                try {
                    Stream.of(item.getFs().listStatus(new Path(item.getPath(), treeNode.getRelativePath())))
                            .forEach(x -> pair.getSecond().add(new FileTreeNode(x.getPath().getName(), treeNode)));
                } catch (FileNotFoundException e) {
                    log.info("folder {} not found", new Path(item.getPath(), treeNode.getRelativePath()));
                }
            }
            item.getProjectNodes().add(projectNode);
            item.getProjects().put(projectNode.getName(), projectNode);
            for (Pair<List<FileTreeNode>, List<FileTreeNode>> pair : Arrays.asList(
                    Pair.newPair(tableSnapshotParents, projectNode.getSnapshots()), //
                    Pair.newPair(projectNode.getGlobalDictTables(), projectNode.getGlobalDictColumns()), //
                    Pair.newPair(projectNode.getDataflows(), projectNode.getSegments()), //
                    Pair.newPair(projectNode.getSegments(), projectNode.getLayouts()))) {
                val slot = pair.getSecond();
                for (FileTreeNode node : pair.getFirst()) {
                    Stream.of(item.getFs().listStatus(new Path(item.getPath(), node.getRelativePath())))
                            .forEach(x -> slot.add(new FileTreeNode(x.getPath().getName(), node)));
                }
            }
        }

    }

    @Data
    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class StorageItem {

        @NonNull
        private FileSystem fs;

        @NonNull
        private String path;

        /**
         * File hierarchy is
         *
         * /working_dir
         * |--/${project_name}
         *    |--/parquet
         *    |  +--/${dataflow_id}
         *    |     +--/${segment_id}
         *    |        +--/${layout_id}
         *    |--/job_tmp
         *    |  +--/${job_id}
         *    |--/table_exd
         *    |  +--/${table_identity}
         *    |--/dict/global_dict
         *    |  +--/${table_identity}
         *    |     +--/${column_name}
         *    +--/table_snapshot
         *       +--/${table_identity}
         *          +--/${snapshot_version}
         */

        List<FileTreeNode> projectNodes = Lists.newArrayList();

        Map<String, ProjectFileTreeNode> projects = Maps.newHashMap();

        List<FileTreeNode> getAllNodes() {
            val allNodes = projects.values().stream().flatMap(p -> p.getAllCandidates().stream())
                    .flatMap(Collection::stream).collect(Collectors.toList());
            allNodes.addAll(projectNodes);
            return allNodes;
        }

        ProjectFileTreeNode getProject(String name) {
            return projects.getOrDefault(name, new ProjectFileTreeNode(name));
        }
    }

    @Data
    @AllArgsConstructor
    @RequiredArgsConstructor
    public static class FileTreeNode {

        @NonNull
        String name;

        FileTreeNode parent;

        String getRelativePath() {
            if (parent == null) {
                return name;
            }
            return parent.getRelativePath() + "/" + name;
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @ToString(onlyExplicitlyIncluded = true, callSuper = true)
    public static class ProjectFileTreeNode extends FileTreeNode {

        public ProjectFileTreeNode(String name) {
            super(name);
        }

        List<FileTreeNode> jobTmps = Lists.newLinkedList();

        List<FileTreeNode> tableExds = Lists.newLinkedList();

        List<FileTreeNode> globalDictTables = Lists.newLinkedList();

        List<FileTreeNode> globalDictColumns = Lists.newLinkedList();

        List<FileTreeNode> snapshotTables = Lists.newLinkedList();

        List<FileTreeNode> snapshots = Lists.newLinkedList();

        List<FileTreeNode> dataflows = Lists.newLinkedList();

        List<FileTreeNode> segments = Lists.newLinkedList();

        List<FileTreeNode> layouts = Lists.newLinkedList();

        Collection<List<FileTreeNode>> getAllCandidates() {
            return Arrays.asList(jobTmps, tableExds, globalDictTables, globalDictColumns, snapshotTables, snapshots,
                    dataflows, segments, layouts);
        }

    }

    public static class Stats {

        public final  Set<StorageItem> allItems = Collections.synchronizedSet(new HashSet<>());
        public final  Set<StorageItem> startItem = Collections.synchronizedSet(new HashSet<>());
        public final  Set<StorageItem> successItems = Collections.synchronizedSet(new HashSet<>());
        public final  Set<StorageItem> errorItems = Collections.synchronizedSet(new HashSet<>());


        private void reset() {
            allItems.clear();
            startItem.clear();
            successItems.clear();
            errorItems.clear();
        }

        void onAllStart(Set<StorageItem> outDatedItems) {
            // retry enters here too, reset everything first
            reset();

            log.debug("{} items to cleanup", outDatedItems.size());
            allItems.addAll(outDatedItems);
        }

        void onItemStart(StorageItem item) {
            heartBeat();
            startItem.add(item);
        }

        void onItemError(StorageItem item) {
            errorItems.add(item);
        }

        void onItemSuccess(StorageItem item) {
            successItems.add(item);
        }

        public void onRetry() {
            // for progress printing
        }

        public void heartBeat() {
            // for progress printing
        }

        public boolean hasError() {
            return !errorItems.isEmpty();
        }
    }
}
