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
package io.kyligence.kap.rest.util;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import io.kyligence.kap.secondstorage.config.Node;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SecondStorageUtil.class })
public class ModelUtilsTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setUp() {
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
    }

    @After
    public void teardown() {
    }

    private void prepareSecondStorageInfo(List<SecondStorageInfo> secondStorageInfos) {
        val ssi = new SecondStorageInfo().setSecondStorageEnabled(true);
        Map<String, List<SecondStorageNode>> pairs = Maps.newHashMap();
        pairs.put("ssi", Arrays.asList(new SecondStorageNode(new Node())));
        ssi.setSecondStorageSize(1024).setSecondStorageNodes(pairs);
        secondStorageInfos.add(ssi);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "setSecondStorageSizeInfo", List.class))
                .toReturn(secondStorageInfos);
    }

    @Test
    public void testComputeExpansionRate() {
        Assert.assertEquals("0", ModelUtils.computeExpansionRate(0, 1024));
        Assert.assertEquals("-1", ModelUtils.computeExpansionRate(1024, 0));

        String result = ModelUtils.computeExpansionRate(2048, 1024);
        Assert.assertEquals("200.00", result);
    }

    @Test
    public void testIsArgMatch() {
        Assert.assertTrue(ModelUtils.isArgMatch(null, true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", false, "ADMIN"));
    }

    @Test
    public void testAddSecondStorageInfo() {
        prepareSecondStorageInfo(new ArrayList<>());
        val models = Arrays.asList((NDataModel) new NDataModelResponse());
        ModelUtils.addSecondStorageInfo("default", models);
        val dataModelResp = ((NDataModelResponse) models.get(0));
        Assert.assertEquals(1, dataModelResp.getSecondStorageNodes().size());
        Assert.assertEquals(1024, dataModelResp.getSecondStorageSize());
        Assert.assertEquals(true, dataModelResp.isSecondStorageEnabled());
    }

    @Test
    public void testGetFilterModels() {
        prepareSecondStorageInfo(new ArrayList<>());
        List<NDataModel> mockedModels = Lists.newArrayList();
        NDataModelResponse modelSpy4 = Mockito.spy(new NDataModelResponse(new NDataModel()));
        when(modelSpy4.isSecondStorageEnabled()).thenReturn(true);
        mockedModels.add(modelSpy4);
        val modelSets = ModelUtils.getFilteredModels("default", Arrays.asList(ModelAttributeEnum.SECOND_STORAGE),
                mockedModels);
        Assert.assertEquals(1, modelSets.size());
        val dataModelResp = ((NDataModelResponse) modelSets.iterator().next());
        Assert.assertEquals(1, dataModelResp.getSecondStorageNodes().size());
        Assert.assertEquals(1024, dataModelResp.getSecondStorageSize());
        Assert.assertEquals(true, dataModelResp.isSecondStorageEnabled());
    }
}
