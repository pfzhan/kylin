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

package io.kyligence.kap.smart.common;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.Utils;

public class SmartMasterTest {
    private static final Logger logger = LoggerFactory.getLogger(SmartMasterTest.class);

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_TPCDS() throws Exception {
        String base = "src/test/resources/tpcds/";
        File tmpMetaDir = Files.createTempDir();
        FileUtils.copyDirectory(new File(base + "meta"), tmpMetaDir);
        testInternal("TPC_DS_2", tmpMetaDir.getAbsolutePath(), base + "sql_filtered");
        FileUtils.forceDelete(tmpMetaDir);
    }

    private void testInternal(String proj, String metaDir, String sqlDir) throws Exception {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.aggGroup.strategy", "whitelist");
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        kylinConfig.setProperty("kap.smart.strategy", "batch");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        File[] sqlFiles = new File[0];
        if (sqlDir != null) {
            File sqlDirF = new File(sqlDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(sqlDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".sql")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }

        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }

        Collection<ModelMaster> masters = MasterFactory.createModelMasters(kylinConfig, proj, sqls);
        logger.info("There will be {} models.", masters.size());

        int i = 0;
        DataModelManager modelManager = DataModelManager.getInstance(kylinConfig);
        List<DataModelDesc> models = Lists.newArrayList();
        for (ModelMaster master : masters) {
            logger.info("Generating the {}th model.", i++);
            DataModelDesc modelDesc = master.proposeAll();
            models.add(modelDesc);
            modelManager.createDataModelDesc(modelDesc, proj, null);
        }
        Assert.assertEquals(14, i);

        CubeDescManager cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        List<CubeMaster> cubeMasters = MasterFactory.createCubeMasters(kylinConfig, proj, sqls);
        i = 0;
        for (CubeMaster cubeMaster : cubeMasters) {
            logger.info("Generating the {}th cube.", i++);
            CubeDesc cube = cubeMaster.proposeAll();
            cubeDescManager.createCubeDesc(cube);
            cubeManager.createCube(cube.getName(), proj, cube, null);
        }
        Assert.assertEquals(14, i);
    }
}
