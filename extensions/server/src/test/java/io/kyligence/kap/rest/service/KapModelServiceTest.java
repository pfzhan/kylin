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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.service.ModelService;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.KapModel;

public class KapModelServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("modelMgmtService")
    ModelService modelService;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSuccessModelUpdateOnComputedColumn()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        Serializer<DataModelDesc> serializer = modelService.getDataModelManager().getDataModelSerializer();

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        KapModel deserialize = (KapModel) serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("comment");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "change on comment is okay");
        modelService.updateModelAndDesc("default", deserialize);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage(
                "Column name for computed column DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT is already used in model ci_inner_join_model, you should apply the same expression ' PRICE * ITEM_COUNT ' here, or use a different column name.");

        Serializer<DataModelDesc> serializer = modelService.getDataModelManager().getDataModelSerializer();

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        KapModel deserialize = (KapModel) serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "another expression");
        modelService.updateModelAndDesc("default", deserialize);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict2()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage(
                "There is already a column named cal_dt on table DEFAULT.TEST_KYLIN_FACT, please change your computed column name");

        Serializer<DataModelDesc> serializer = modelService.getDataModelManager().getDataModelSerializer();

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        KapModel deserialize = (KapModel) serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("columnName");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "cal_dt");
        modelService.updateModelAndDesc("default", deserialize);
    }

}
