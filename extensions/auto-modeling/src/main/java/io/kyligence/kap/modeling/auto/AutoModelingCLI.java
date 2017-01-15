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

package io.kyligence.kap.modeling.auto;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.common.obf.IKeepNames;

/**
 * Before run this CLI, need to replase "PROVIDED" with "COMPILE" in kap-auto-modeling.iml
 */
public class AutoModelingCLI extends AbstractApplication implements IKeepNames {

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(true).withDescription("Specify a project to contain this cube.").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(true).withDescription("Specify a data model for this cube.").create("model");

    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true).withDescription("Specify the name of cube.").create("cube");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SQL = OptionBuilder.withArgName("sql").hasArg().isRequired(false).withDescription("Specify a directory of sql files.").create("sql");

    private Options options;

    public AutoModelingCLI() {
        super();

        options = new Options();
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_SQL);
        options.addOption(OPTION_CUBE);
    }

    protected static CubeDesc generateCube(String sqlDir, String modelName, String cubeName, String project) throws Exception {
        File sqlRoot = new File(sqlDir);
        String[] sqls = null;
        if (sqlRoot.exists()) {
            File[] sqlFiles = sqlRoot.listFiles();
            if (sqlFiles != null) {
                sqls = new String[sqlFiles.length];
                for (int i = 0; i < sqlFiles.length; i++) {
                    sqls[i] = FileUtils.readFileToString(sqlFiles[i], "utf-8");
                }
            }
        }

        return AutoModelingService.getInstance().generateCube(modelName, cubeName, sqls, project);
    }

    public static void main(String[] args) throws Exception {
        AutoModelingCLI cli = new AutoModelingCLI();
        cli.execute(args);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String model = optionsHelper.hasOption(OPTION_MODEL) ? optionsHelper.getOptionValue(OPTION_MODEL) : null;
        String project = optionsHelper.hasOption(OPTION_PROJECT) ? optionsHelper.getOptionValue(OPTION_PROJECT) : null;
        String sql = optionsHelper.hasOption(OPTION_SQL) ? optionsHelper.getOptionValue(OPTION_SQL) : null;
        String cube = optionsHelper.hasOption(OPTION_CUBE) ? optionsHelper.getOptionValue(OPTION_CUBE) : null;

        CubeDesc cubeDesc = generateCube(sql, model, cube, project);
        CubeInstance cubeInstance = CubeInstance.create(cubeDesc.getName(), cubeDesc);

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager.getInstance(kylinConfig).createCube(cubeInstance, project, null);
        CubeDescManager.getInstance(kylinConfig).createCubeDesc(cubeDesc);
    }
}
