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

package io.kyligence.kap.tool.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;

public class MetadataChecker implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(MetadataChecker.class);

    public static final String TABLEINDEX_CUBE_RULE = "TableIndex inconsistent with Cube";
    public static final String SCHEDULERJOB_CUBE_RULE = "SchedulerJob inconsistent with Cube";
    public static final String EXECUTABLE_OUT_RULE = "Executable inconsistent with ExecutableOutput";
    public static final String CUBE_MODEL_RULE = "Cube/TableIndex inconsistent with Model";

    private KylinConfig kylinConfig;
    private ResourceStore store;

    private Map<String, Object> checkResult = new HashMap<>();

    public MetadataChecker() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        store = ResourceStore.getStore(kylinConfig);
    }

    public Map<String, Object> getCheckResult() {
        return this.checkResult;
    }

    public void checkCubeWithModel() {

        List<String> cubePaths = new ArrayList<>();
        List<String> cubeDescPaths = new ArrayList<>();
        List<String> modelPaths = new ArrayList<>();
        List<String> tableIndexPaths = new ArrayList<>();
        List<String> tableIndexDescPaths = new ArrayList<>();

        try {
            cubePaths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            cubeDescPaths = store.collectResourceRecursively(ResourceStore.CUBE_DESC_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            modelPaths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            tableIndexPaths = store.collectResourceRecursively(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            tableIndexDescPaths = store.collectResourceRecursively(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
        } catch (IOException e) {
            logger.info("Failed to get cube/cubeDesc/model resource path, details: {}", e);
        }

        List<String> allAffectedEntities = new ArrayList<>();
        List<String> toDelDescs = new ArrayList<>();

        for (String descPath : cubeDescPaths) {
            try {
                CubeDesc desc = store.getResource(descPath, CubeDesc.class, CubeDescManager.CUBE_DESC_SERIALIZER);
                if (desc == null) {
                    logger.info("No cube desc found at: {}, skip it.", descPath);
                    continue;
                }

                boolean bFind = false;
                for (String modelPath : modelPaths) {
                    if (modelPath.contains(desc.getModelName())) {
                        bFind = true;
                        break;
                    }
                }

                if (bFind == false) {
                    toDelDescs.add(descPath);
                }

            } catch (IOException e) {
                logger.info("Failed to get cubeDesc from resource path, details: {}, skip it.", e);
                continue;
            }
        }

        allAffectedEntities.addAll(toDelDescs);
        allAffectedEntities.addAll(getEntityWithSameName(toDelDescs, cubePaths));
        allAffectedEntities.addAll(getEntityWithSameName(toDelDescs, tableIndexPaths));
        allAffectedEntities.addAll(getEntityWithSameName(toDelDescs, tableIndexDescPaths));

        checkResult.put(CUBE_MODEL_RULE, allAffectedEntities);
    }

    public void checkCubeWithTableIndex() {

        List<String> cubePaths = new ArrayList<>();
        List<String> tableIndexPaths = new ArrayList<>();
        List<String> tableIndexDescPaths = new ArrayList<>();

        try {
            cubePaths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            tableIndexPaths = store.collectResourceRecursively(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            tableIndexDescPaths = store.collectResourceRecursively(RawTableDesc.RAW_TABLE_DESC_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
        } catch (IOException e) {
            logger.info("Failed to get cube/table-index resource path, details: {}", e);
        }

        getIsolatedEntity(TABLEINDEX_CUBE_RULE, tableIndexPaths, cubePaths);
        getIsolatedEntity(TABLEINDEX_CUBE_RULE, tableIndexDescPaths, cubePaths);
    }

    public void checkCubeWithSchedulerJob() {
        List<String> cubePaths = new ArrayList<>();
        List<String> schedulerJobPaths = new ArrayList<>();

        try {
            cubePaths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
            schedulerJobPaths = store.collectResourceRecursively(SchedulerJobInstance.SCHEDULER_RESOURCE_ROOT,
                    MetadataConstants.FILE_SURFIX);
        } catch (IOException e) {
            logger.info("Failed to get cube/scheduler-job resource path, details: {}", e);
        }
        getIsolatedEntity(SCHEDULERJOB_CUBE_RULE, schedulerJobPaths, cubePaths);
    }

    public void checkExecutableOutput() {
        ExecutableDao executableDao = ExecutableDao.getInstance(kylinConfig);
        List<ExecutablePO> allExecutable = new ArrayList<>();
        List<ExecutableOutputPO> allOutput = new ArrayList<>();
        try {
            allExecutable = executableDao.getJobs();
            allOutput = executableDao.getJobOutputs();
        } catch (PersistentException e) {
            logger.info("Failed to get executableDao.");
        }

        List<String> toCleanEntity = new ArrayList<>();

        for (ExecutablePO executable : allExecutable) {
            boolean ok = isExecuteOK(executable, allOutput);
            String jobId = executable.getId();
            if (ok == false) {
                toCleanEntity.add(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + jobId);
                toCleanEntity.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + jobId);
                for (ExecutablePO task : executable.getTasks()) {
                    toCleanEntity.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                }
            }
        }
        checkResult.put(EXECUTABLE_OUT_RULE, toCleanEntity);
    }

    private List<String> getEntityWithSameName(List<String> target, List<String> source) {
        List<String> toRet = new ArrayList<>();
        for (String path : target) {
            String name = getEntityName(path);
            for (String cubePath : source) {
                if (cubePath.contains(name)) {
                    toRet.add(cubePath);
                    break;
                }
            }
        }
        return toRet;
    }

    private void getIsolatedEntity(String key, List<String> srcList, List<String> dstList) {
        List<String> toCleanEntity = new ArrayList<>();
        for (String src : srcList) {
            boolean bFind = false;
            String name = getEntityName(src);
            for (String dst : dstList) {
                if (dst.contains(name)) {
                    bFind = true;
                    break;
                }
            }
            if (bFind == false) {
                toCleanEntity.add(src);
            }
        }
        if (checkResult.get(key) == null)
            checkResult.put(key, toCleanEntity);
        else
            ((List<String>) checkResult.get(key)).addAll(toCleanEntity);
    }

    private boolean isExecuteOK(ExecutablePO executable, List<ExecutableOutputPO> allOutput) {
        boolean bMatch = false;

        for (ExecutableOutputPO output : allOutput) {
            if (output.getId().equals(executable.getId())) {
                bMatch = true;
                break;
            }
        }

        boolean bTasksMatch = false;

        for (ExecutablePO task : executable.getTasks()) {
            for (ExecutableOutputPO output : allOutput) {
                if (output.getId().equals(task.getId())) {
                    bTasksMatch = true;
                    break;
                }
            }
        }

        return bMatch && bTasksMatch;
    }

    public void doOpts(String opt) {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        for (Map.Entry<String, Object> entry : checkResult.entrySet()) {
            List<String> toCleanEntity = (List<String>) entry.getValue();
            System.out.println("--------------- " + entry.getKey() + " ---------------");
            for (String path : toCleanEntity) {
                if ("check".equals(opt))
                    System.out.println("Isolated Entity: " + path);
                if ("recovery".equals(opt)) {
                    System.out.println("Deleting Isolated Entity: " + path);
                    try {
                        store.deleteResource(path);
                    } catch (IOException e) {
                        logger.info("Failed to delete resource: {}", path);
                    }
                }
            }
            System.out.println("---------------------------------------------------------------------");
        }
    }

    public void doCheck(String opt) {
        checkResult.clear();
        checkCubeWithModel();
        checkCubeWithTableIndex();
        checkCubeWithSchedulerJob();
        checkExecutableOutput();
        doOpts(opt);
    }

    private String getEntityName(String path) {
        if (path.endsWith(".json"))
            path = path.substring(0, path.length() - ".json".length());

        int cut = path.lastIndexOf("/");
        if (cut >= 0)
            path = path.substring(cut + 1);
        return path;
    }

    public static void main(String[] args) {

        MetadataChecker cli = new MetadataChecker();
        if (args.length != 1) {
            cli.usage();
            System.exit(1);
        }
        cli.doCheck(args[0]);
    }

    public void usage() {
        System.out.println("MetadataChecker operation \n"
                + "operation: check              check if there are inconsistent metadata. \n"
                + "operation: recovery           Caution: It cleans the inconsistent meta entities to make KAP work. \n"
                + "It can not recovery the removed meta entities.");
    }
}
