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

package org.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;

@RunWith(MockitoJUnitRunner.class)
public class ModelBuildServiceTest {

    @Spy
    private ModelBuildService modelBuildService;

    @Test
    public void testCheckMultiPartitionBuildParam() {
        NDataModel model = mock(NDataModel.class);
        IncrementBuildSegmentParams params = mock(IncrementBuildSegmentParams.class);

        // Not a multi-partition model, no need check
        when(model.isMultiPartitionModel()).thenReturn(false);
        modelBuildService.checkMultiPartitionBuildParam(model, params);

        // Test throwing JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(true);
        when(params.getMultiPartitionValues()).thenReturn(new ArrayList<>());
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY.getCodeMsg(), e.getLocalizedMessage());
        }

        // Test throwing 1st JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(false);
        List<String[]> partitionValues = new ArrayList<>();
        partitionValues.add(new String[0]);
        when(params.getMultiPartitionValues()).thenReturn(partitionValues);
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getCodeMsg(), e.getLocalizedMessage());
        }

        // Test throwing 2nd JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(true);
        partitionValues = new ArrayList<>();
        partitionValues.add(new String[] {"p1"});
        when(params.getMultiPartitionValues()).thenReturn(partitionValues);
        // mock column size
        LinkedList<String> columns = mock(LinkedList.class);
        MultiPartitionDesc multiPartitionDesc = mock(MultiPartitionDesc.class);
        when(model.getMultiPartitionDesc()).thenReturn(multiPartitionDesc);
        when(multiPartitionDesc.getColumns()).thenReturn(columns);
        when(columns.size()).thenReturn(2);
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getCodeMsg(), e.getLocalizedMessage());
        }
    }

}
