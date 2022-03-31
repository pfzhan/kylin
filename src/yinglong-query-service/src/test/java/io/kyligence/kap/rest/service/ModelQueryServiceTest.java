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
package io.kyligence.kap.rest.service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.service.params.ModelQueryParams;
import io.kyligence.kap.rest.util.ModelTriple;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SecondStorageUtil.class })
public class ModelQueryServiceTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @Before
    public void setUp() {
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
    }

    @After
    public void teardown() {
    }

    @Test
    public void testFilterModelOfBatch() {
        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.BATCH);
        List models = Arrays.asList(mt1);

        List<ModelAttributeEnum> modelAttributeSet1 = Lists.newArrayList(ModelAttributeEnum.BATCH);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, 0, 10, "", true,
                null, modelAttributeSet1, null, null, true);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable")).toReturn(Boolean.FALSE);

        List<ModelTriple> filteredModels1 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(1, filteredModels1.size());

    }

    @Test
    public void testFilterModelWithSecondStorage() {
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable")).toReturn(Boolean.TRUE);

        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.BATCH);

        List models = Arrays.asList(mt1);

        List<ModelAttributeEnum> modelAttributeSet1 = Lists.newArrayList(ModelAttributeEnum.BATCH,
                ModelAttributeEnum.SECOND_STORAGE);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, 0, 10, "", true,
                null, modelAttributeSet1, null, null, true);
        List<ModelTriple> filteredModels1 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(1, filteredModels1.size());

        List<ModelAttributeEnum> modelAttributeSet2 = Lists.newArrayList(ModelAttributeEnum.SECOND_STORAGE);
        ModelQueryParams modelQueryParams2 = new ModelQueryParams("", null, true, "default", null, null, 0, 10, "",
                true, null, modelAttributeSet2, null, null, true);
        List<ModelTriple> filteredModels2 = modelQueryService.filterModels(models, modelQueryParams2);
        Assert.assertEquals(1, filteredModels2.size());
    }

    @Test
    public void testFilterModelAttribute() {
        Set<ModelAttributeEnum> modelAttributeSet1 = Sets.newHashSet(ModelAttributeEnum.BATCH,
                ModelAttributeEnum.SECOND_STORAGE);
        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.UNKNOWN);
        Assert.assertFalse(modelQueryService.filterModelAttribute(mt1, modelAttributeSet1, false));
    }
}
