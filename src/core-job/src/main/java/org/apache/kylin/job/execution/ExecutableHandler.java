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

package org.apache.kylin.job.execution;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public abstract class ExecutableHandler implements IKeepNames {

    protected static final String SUBJECT_NOT_EXIST_COMMENT = "subject does not exist or is broken, roll back to to-be-accelerated status";
    protected static final String MERGE_SEGMENT_EVENT_CLASS = "io.kyligence.kap.event.model.MergeSegmentEvent";
    protected static final String ADD_CUBOID_EVENT_CLASS = "io.kyligence.kap.event.model.AddCuboidEvent";
    protected static final String EVENT_MANAGER_CLASS = "io.kyligence.kap.event.manager.EventManager";
    protected static final String EVENT_CLASS = "io.kyligence.kap.event.model.Event";

    @Getter
    @Setter
    private String project;
    @Getter
    @Setter
    private String modelId;
    @Getter
    @Setter
    private String owner;
    @Getter
    @Setter
    private String segmentId;
    @Getter
    @Setter
    private String jobId;

    public abstract void handleFinished();

    public abstract void handleDiscardOrSuicidal();

    protected boolean checkSubjectExists(String project, String indexPlanId, String segmentId) {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflow df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(indexPlanId);
        if (df == null || df.checkBrokenWithRelatedInfo()) {
            log.info("job {} not finished, because index_plan {} does not exist or broken", jobId, indexPlanId);
            return false;
        }

        if (segmentId != null) {
            NDataSegment dataSegment = df.getSegment(segmentId);
            if (dataSegment == null) {
                log.info("job {} not finished, because its target segment {} does not exist", jobId, segmentId);
                return false;
            }
        }

        return true;

    }

    protected NExecutableManager getExecutableManager(String project, KylinConfig config) {
        return NExecutableManager.getInstance(config, project);
    }

    protected void postEvent(String eventClassName, String segmentId) {

        try {
            Class emClass = Class.forName(EVENT_MANAGER_CLASS);
            Class eventClass = Class.forName(EVENT_CLASS);
            val config = KylinConfig.getInstanceFromEnv();
            val manager = config.getManager(project, emClass);
            Class eClass = Class.forName(eventClassName);
            Object event = eClass.newInstance();
            eClass.getMethod("setModelId", String.class).invoke(event, modelId);
            if (segmentId != null) {
                eClass.getMethod("setSegmentId", String.class).invoke(event, segmentId);
            }
            eClass.getMethod("setJobId", String.class).invoke(event, UUID.randomUUID().toString());
            eClass.getMethod("setOwner", String.class).invoke(event, owner);
            emClass.getMethod("post", eventClass).invoke(manager, event);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException
                | InvocationTargetException e) {
            log.error("post event error ");
        }
    }

    protected DefaultChainedExecutableOnModel getExecutable() {
        val executable = getExecutableManager(project, KylinConfig.getInstanceFromEnv()).getJob(jobId);
        Preconditions.checkNotNull(executable);
        Preconditions.checkArgument(executable instanceof DefaultChainedExecutableOnModel);
        return (DefaultChainedExecutableOnModel) executable;
    }
}
