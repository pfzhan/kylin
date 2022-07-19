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

package io.kyligence.kap.tool.util;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.dao.ExecutablePO;
import org.mockito.Mockito;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.job.util.JobInfoUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.delegate.ModelMetadataBaseInvoker;

public class JobMetadataWriter {

    public static void writeJobMetaData(KylinConfig config, List<RawResource> metadata) {
        JobContextUtil.cleanUp();
        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(config);
        ModelMetadataBaseInvoker modelMetadataBaseInvoker = Mockito.mock(ModelMetadataBaseInvoker.class);
        Mockito.when(modelMetadataBaseInvoker.getModelNameById(Mockito.anyString(), Mockito.anyString()))
                .thenReturn("test");
        jobInfoDao.setModelMetadataInvoker(modelMetadataBaseInvoker);

        metadata.forEach(rawResource -> {
            String path = rawResource.getResPath();
            if (!path.equals("/calories/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5")) {
                return;
            }
            long updateTime = rawResource.getTimestamp();
            ExecutablePO executablePO = parseExecutablePO(rawResource.getByteSource(), updateTime, "calories");
            jobInfoDao.addJob(executablePO);
        });
    }

    public static void writeJobMetaData(KylinConfig config) {
        JobContextUtil.cleanUp();
        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(config);
        ModelMetadataBaseInvoker modelMetadataBaseInvoker = Mockito.mock(ModelMetadataBaseInvoker.class);
        Mockito.when(modelMetadataBaseInvoker.getModelNameById(Mockito.anyString(), Mockito.anyString())).thenReturn("test");
        jobInfoDao.setModelMetadataInvoker(modelMetadataBaseInvoker);

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
        List<String> allMetadataKey = resourceStore.collectResourceRecursively("/", "");
        NProjectManager projectManager = NProjectManager.getInstance(config);
        List<String> projectNames = projectManager.listAllProjects().stream().map(projectInstance -> projectInstance.getName()).collect(Collectors.toList());
        for (String projectName : projectNames) {
            String prefix = "/" + projectName + "/execute/";
            List<String> jobKeyList = allMetadataKey.stream().filter(key -> key.startsWith(prefix)).collect(Collectors.toList());
            for (String jobKey : jobKeyList) {
                RawResource jobRawResource = resourceStore.getResource(jobKey);
                long updateTime = jobRawResource.getTimestamp();
                ExecutablePO executablePO = parseExecutablePO(jobRawResource.getByteSource(), updateTime, projectName);
                jobInfoDao.addJob(executablePO);
            }
        }
    }

    private static ExecutablePO parseExecutablePO(ByteSource byteSource, long updateTime, String projectName) {
        try {
            return JobInfoUtil.deserializeExecutablePO(byteSource, updateTime, projectName);
        } catch (IOException e) {
            throw new RuntimeException("Error when deserializing job metadata", e);
        }
    }
}
