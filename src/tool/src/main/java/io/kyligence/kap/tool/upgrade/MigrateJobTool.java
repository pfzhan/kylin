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

package io.kyligence.kap.tool.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateJobTool extends ExecutableApplication implements IKeep {
    private static final Option OPTION_HELP = OptionBuilder.hasArg(false).withDescription("print help message.")
            .isRequired(false).withLongOpt("help").create("h");

    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");

    private static final List<String> REMOVE_EVENTS = Arrays.asList("io.kyligence.kap.event.model.PostAddCuboidEvent",
            "io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent",
            "io.kyligence.kap.event.model.PostAddSegmentEvent");

    private static final Map<String, String> JOB_TYPE_HANDLER_MAP = new HashMap<>();

    static {
        JOB_TYPE_HANDLER_MAP.put("INDEX_BUILD", "io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler");
        JOB_TYPE_HANDLER_MAP.put("INC_BUILD", "io.kyligence.kap.engine.spark.job.ExecutableAddSegmentHandler");
        JOB_TYPE_HANDLER_MAP.put("INDEX_MERGE", "io.kyligence.kap.engine.spark.job.ExecutableMergeOrRefreshHandler");
        JOB_TYPE_HANDLER_MAP.put("INDEX_REFRESH", "io.kyligence.kap.engine.spark.job.ExecutableMergeOrRefreshHandler");
    }

    private KylinConfig config = KylinConfig.getInstanceFromEnv();

    private ResourceStore resourceStore;

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_HELP);
        return options;
    }

    public static void main(String[] args) {
        val tool = new MigrateJobTool();
        tool.execute(args);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));

        config.setMetadataUrl(metadataUrl);

        resourceStore = ResourceStore.getKylinMetaStore(config);

        NProjectManager projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            removeUselessEvent(project);
            updateExecute(project);
        }
    }

    /**
     * 
     * @param projectInstance
     */
    private void removeUselessEvent(ProjectInstance projectInstance) {
        NavigableSet<String> eventPaths = resourceStore
                .listResources("/" + projectInstance.getName() + ResourceStore.EVENT_RESOURCE_ROOT);

        if (eventPaths != null) {
            for (String eventPath : eventPaths) {
                RawResource rs = resourceStore.getResource(eventPath);
                if (rs == null) {
                    continue;
                }
                try (InputStream in = rs.getByteSource().openStream()) {
                    JsonNode eventNode = JsonUtil.readValue(in, JsonNode.class);
                    if (eventNode.has("@class")) {
                        String clazz = eventNode.get("@class").textValue();
                        if (REMOVE_EVENTS.contains(clazz)) {
                            System.out.println("delete event " + eventPath);
                            resourceStore.getMetadataStore().deleteResource(eventPath, null, 0);
                        }
                    }
                } catch (Exception e) {
                    log.warn("read {} failed", eventPath, e);
                }
            }
        }
    }

    /**
     * 
     * @param projectInstance
     */
    private void updateExecute(ProjectInstance projectInstance) {
        NExecutableManager executableManager = NExecutableManager.getInstance(config, projectInstance.getName());

        List<AbstractExecutable> executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        for (AbstractExecutable executeJob : executeJobs) {
            String executePath = "/" + projectInstance.getName() + ResourceStore.EXECUTE_RESOURCE_ROOT + "/"
                    + executeJob.getId();
            RawResource rs = resourceStore.getResource(executePath);
            if (rs == null) {
                continue;
            }

            try (InputStream in = rs.getByteSource().openStream()) {
                JsonNode executeNode = JsonUtil.readValue(in, JsonNode.class);

                if (executeNode.has("name")) {
                    String name = executeNode.get("name").textValue();
                    String handlerType = JOB_TYPE_HANDLER_MAP.get(name);
                    if (handlerType != null && !executeNode.has("handler_type")) {
                        ((ObjectNode) executeNode).put("handler_type", handlerType);
                    }
                }

                addUpdateMetadataTask(executeNode);

                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(buf);
                JsonUtil.writeValue(dout, executeNode);
                dout.close();
                buf.close();

                ByteSource byteSource = ByteStreams.asByteSource(buf.toByteArray());

                rs = new RawResource(executePath, byteSource, System.currentTimeMillis(), rs.getMvcc() + 1);

                System.out.println("update execute " + executePath);
                resourceStore.getMetadataStore().putResource(rs, null, 0);

            } catch (Exception e) {
                log.warn("read {} failed", executePath, e);
            }
        }
    }

    /**
     *
     * @param executeNode
     */
    private void addUpdateMetadataTask(JsonNode executeNode) {
        if (executeNode.has("tasks")) {
            ArrayNode tasks = (ArrayNode) executeNode.get("tasks");
            if (tasks.size() == 2) {
                ObjectNode taskNode = tasks.get(0).deepCopy();

                String uuid = taskNode.get("uuid").textValue().replace("_00", "_02");
                taskNode.put("uuid", uuid);

                taskNode.put("name", "Update Metadata");

                taskNode.put("type", "io.kyligence.kap.engine.spark.job.NSparkUpdateMetadataStep");

                if (taskNode.has("params")) {
                    ObjectNode paramsNode = (ObjectNode) taskNode.get("params");
                    paramsNode.remove("distMetaUrl");

                    paramsNode.remove("className");

                    paramsNode.remove("outputMetaUrl");
                }

                if (taskNode.has("output")) {
                    ObjectNode outputNode = (ObjectNode) taskNode.get("output");
                    if (outputNode.has("status")) {
                        outputNode.put("status", "READY");
                    }
                }
                tasks.add(taskNode);
            }
        }
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
        } else {
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");

        }
    }
}
