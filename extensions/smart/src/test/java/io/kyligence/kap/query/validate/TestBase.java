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

package io.kyligence.kap.query.validate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.CubeSQLValidator;
import io.kyligence.kap.smart.query.validator.ModelSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

public class TestBase {

    private List<String> readSqls(String sqlFileName) throws IOException {
        List<String> sqlList = Lists.newArrayList();
        File sqlFile = new File(sqlFileName);

        Preconditions.checkArgument(sqlFile.exists());

        if (sqlFile.isDirectory()) {
            File[] sqlFiles = sqlFile.listFiles();
            Preconditions.checkArgument(sqlFiles != null && sqlFiles.length > 0,
                    "SQL files not found under " + sqlFile.getAbsolutePath());

            for (File file : sqlFiles) {
                sqlList.add(FileUtils.readFileToString(file, Charset.defaultCharset()));
            }
        } else if (sqlFile.isFile()) {
            BufferedReader br = new BufferedReader(new FileReader(sqlFile));
            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                if (line.endsWith(";")) {
                    sb.append(line);
                    sb.deleteCharAt(sb.length() - 1);
                    sqlList.add(sb.toString());
                    sb = new StringBuilder();
                } else {
                    sb.append(line).append("\n");
                }
            }
        }

        return sqlList;
    }

    private KylinConfig getKylinConfig(String metaDir) {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        Utils.exposeAllTableAndColumn(kylinConfig);
        return kylinConfig;
    }

    private void print(Map<String, SQLValidateResult> validateStatsMap) {
        for (Map.Entry<String, SQLValidateResult> entry : validateStatsMap.entrySet()) {
            String message = "";
            for (SQLAdvice sqlAdvice : entry.getValue().getSQLAdvices()) {
                message += String.format("reason:%s, \n\t  suggest:%s \n\t", sqlAdvice.getIncapableReason(),
                        sqlAdvice.getSuggestion());
            }
            System.out.println(String.format("sql:%s, \n\t  capable:%s, \n\t  %s", entry.getKey(),
                    entry.getValue().isCapable(), message));
        }
    }

    void validateModel(String metaDir, String modelName, String sqlFileName) throws IOException {
        KylinConfig kylinConfig = getKylinConfig(metaDir);
        List<String> sqls = readSqls(sqlFileName);
        System.out.println("SQL count:" + sqls.size());
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);

        AbstractSQLValidator sqlValidator = new ModelSQLValidator(kylinConfig, modelDesc);
        Map<String, SQLValidateResult> validateStatsMap = sqlValidator.batchValidate(Lists.newArrayList(sqls));
        print(validateStatsMap);
        printToFile(modelName + "_" + sqlFileName.substring(sqlFileName.lastIndexOf("/") + 1), validateStatsMap);
    }

    void validateCube(String metaDir, String cubeDescName, String sqlFileName) throws IOException {
        KylinConfig kylinConfig = getKylinConfig(metaDir);
        List<String> sqls = readSqls(sqlFileName);

        CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeDescName);

        AbstractSQLValidator sqlValidator = new CubeSQLValidator(kylinConfig, cubeDesc);
        Map<String, SQLValidateResult> validateStatsMap = sqlValidator.batchValidate(Lists.newArrayList(sqls));
        print(validateStatsMap);
    }

    private void printToFile(String name, Map<String, SQLValidateResult> validateStatsMap) {
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, SQLValidateResult> entry : validateStatsMap.entrySet()) {
            String message = "";
            for (SQLAdvice sqlAdvice : entry.getValue().getSQLAdvices()) {
                message += String.format("reason:\"%s\", suggest:\"%s\"",
                        sqlAdvice.getIncapableReason().replace(",", "，"), sqlAdvice.getSuggestion().replace(",", "，"));
            }
            sb.append(String.format("%s, capable:%s, %s\n", "", entry.getValue().isCapable(), message));
        }

        String outPath = name + ".csv";

        try {
            File file = new File(outPath);
            if (file.exists()) {
                file.delete();
            }
            File toCreate = new File(outPath);
            toCreate.createNewFile();
            FileOutputStream output = new FileOutputStream(outPath);
            byte[] bytes = sb.toString().getBytes(Charsets.UTF_8.toString());
            output.write(bytes, 0, bytes.length);
            output.flush();
            output.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    Map<String, SQLValidateResult> validateModel(String metaDir, String modelName, List<String> sqls) {
        KylinConfig kylinConfig = getKylinConfig(metaDir);
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);

        AbstractSQLValidator sqlValidator = new ModelSQLValidator(kylinConfig, modelDesc);
        return sqlValidator.batchValidate(Lists.newArrayList(sqls));
    }

    Map<String, SQLValidateResult> validateCube(String metaDir, String cubeDescName, List<String> sqls) {
        KylinConfig kylinConfig = getKylinConfig(metaDir);
        CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeDescName);

        AbstractSQLValidator sqlValidator = new CubeSQLValidator(kylinConfig, cubeDesc);
        return sqlValidator.batchValidate(Lists.newArrayList(sqls));
    }
}
