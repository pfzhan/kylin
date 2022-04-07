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

package org.apache.kylin.job.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

@RunWith(MockitoJUnitRunner.class)
public class JobUtilTest {

    private MockedStatic<KylinConfig> kylinConfigMockedStatic;
    private MockedStatic<NTableMetadataManager> tableMetadataManagerMockedStatic;
    private MockedStatic<NExecutableManager> executableManagerMockedStatic;
    private MockedStatic<NDataflowManager> dataflowManagerMockedStatic;
    private MockedStatic<NDataModelManager> dataModelManagerMockedStatic;

    @Mock
    private NTableMetadataManager tableMetadataManager;
    @Mock
    private NExecutableManager executableManager;
    @Mock
    private NDataflowManager dataflowManager;
    @Mock
    private NDataModelManager dataModelManager;

    @Before
    public void before() {
        kylinConfigMockedStatic = mockStatic(KylinConfig.class);
        kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(null);

        tableMetadataManagerMockedStatic = mockStatic(NTableMetadataManager.class);
        tableMetadataManagerMockedStatic.when(() -> NTableMetadataManager.getInstance(any(), any())).thenReturn(tableMetadataManager);

        executableManagerMockedStatic = mockStatic(NExecutableManager.class);
        executableManagerMockedStatic.when(() -> NExecutableManager.getInstance(any(), any())).thenReturn(executableManager);

        dataflowManagerMockedStatic = mockStatic(NDataflowManager.class);
        dataflowManagerMockedStatic.when(() -> NDataflowManager.getInstance(any(), any())).thenReturn(dataflowManager);

        dataModelManagerMockedStatic = mockStatic(NDataModelManager.class);
        dataModelManagerMockedStatic.when(() -> NDataModelManager.getInstance(any(), any())).thenReturn(dataModelManager);
    }

    @After
    public void after() {
        kylinConfigMockedStatic.close();
        tableMetadataManagerMockedStatic.close();
        executableManagerMockedStatic.close();
        dataflowManagerMockedStatic.close();
        dataModelManagerMockedStatic.close();
    }

    @Test
    public void testDeduceTargetSubject_JobType_TableSampling() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.TABLE_SAMPLING);
        Map<String, String> params = new HashMap<>();
        params.put(NBatchConstants.P_TABLE_NAME, "SSB.CUSTOMER");
        executablePO.setParams(params);

        TableDesc tableDesc = mock(TableDesc.class);

        // tableDesc == null
        when(tableMetadataManager.getTableDesc(any())).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // tableDesc != null
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testDeduceTargetSubject_JobType_Snapshot() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.SNAPSHOT_BUILD);
        Map<String, String> params = new HashMap<>();
        params.put(NBatchConstants.P_TABLE_NAME, "SSB.CUSTOMER");
        executablePO.setParams(params);

        Output output = mock(DefaultOutput.class);
        TableDesc tableDesc = mock(TableDesc.class);

        // state.isFinalState() == true && tableDesc == null
        when(executableManager.getOutput(any())).thenReturn(output);
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == true && tableDesc.getLastSnapshotPath() == null
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        when(tableDesc.getLastSnapshotPath()).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == false
        when(output.getState()).thenReturn(ExecutableState.RUNNING);
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == true && tableDesc != null && tableDesc.getLastSnapshotPath() != null
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        when(tableDesc.getLastSnapshotPath()).thenReturn("/path/to/last/snapshot");
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testDeduceTargetSubject_JobType_SecondStorage() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        executablePO.setProject("project_01");

        assertEquals("project_01", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testGetModelAlias() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setTargetModel("SSB.CUSTOMER");
        NDataModel dataModelDesc = mock(NDataModel.class);
        NDataModel dataModelWithoutInit = mock(NDataModel.class);

        // dataModelDesc == null
        when(dataModelManager.getDataModelDesc(any())).thenReturn(null);
        assertNull(JobUtil.getModelAlias(executablePO));

        // dataModelDesc != null && dataModelManager.isModelBroken(targetModel) == true
        when(dataModelManager.getDataModelDesc(any())).thenReturn(dataModelDesc);
        when(dataModelManager.isModelBroken(any())).thenReturn(true);
        when(dataModelManager.getDataModelDescWithoutInit(any())).thenReturn(dataModelWithoutInit);
        JobUtil.getModelAlias(executablePO);
        verify(dataModelManager).getDataModelDescWithoutInit(any());

        // dataModelDesc != null && dataModelManager.isModelBroken(targetModel) == false
        when(dataModelManager.getDataModelDesc(any())).thenReturn(dataModelDesc);
        when(dataModelManager.isModelBroken(any())).thenReturn(false);
        JobUtil.getModelAlias(executablePO);
        verify(dataModelDesc).getAlias();
    }
}
