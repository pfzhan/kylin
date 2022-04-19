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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.kylin.rest.util;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KylinConfig.class, NProjectManager.class})
public class AclEvaluateTest {

    private AclEvaluate aclEvaluate;

    @Before
    public void setUp() throws Exception {
        aclEvaluate = new AclEvaluate();
    }

    @Test
    public void testGetProjectInstance_throwsException() {
        try {
            ReflectionTestUtils.invokeMethod(aclEvaluate, "getProjectInstance", "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(EMPTY_PROJECT_NAME.toErrorCode().getCodeString(), ((KylinException) e).getErrorCodeString());
        }

        PowerMockito.mockStatic(KylinConfig.class);
        PowerMockito.mockStatic(NProjectManager.class);
        try {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            NProjectManager nProjectManager = mock(NProjectManager.class);
            PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenReturn(kylinConfig);
            PowerMockito.when(NProjectManager.getInstance(any())).thenReturn(nProjectManager);
            when(nProjectManager.getProject(anyString())).thenReturn(null);
            ReflectionTestUtils.invokeMethod(aclEvaluate, "getProjectInstance", "test_project_name");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(PROJECT_NOT_EXIST.getCodeMsg("test_project_name"), e.getLocalizedMessage());
        }
    }
}
