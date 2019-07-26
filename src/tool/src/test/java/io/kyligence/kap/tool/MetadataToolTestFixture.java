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

package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;

import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class MetadataToolTestFixture {

    public static void fixtureRestoreTest(KylinConfig kylinConfig, File junitFolder, String folder) throws IOException {
        // copy an metadata image to junit folder
        ResourceTool.copy(kylinConfig, KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()), folder);
        fixtureRestoreTest();

    }

    public static void fixtureRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        // Make the current resource store state inconsistent with the image store
        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        dataModel1.setOwner("who");
        dataModelMgr.updateDataModelDesc(dataModel1);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        dataModelMgr.dropModel(dataModel2);

        val dataModel3 = dataModelMgr.copyForWrite(dataModel2);
        dataModel3.setUuid(UUID.randomUUID().toString());
        dataModel3.setAlias("data_model_3");
        dataModel3.setMvcc(-1L);
        dataModelMgr.createDataModelDesc(dataModel3, "who");
    }
}
