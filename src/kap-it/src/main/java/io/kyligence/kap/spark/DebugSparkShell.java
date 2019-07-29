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

package io.kyligence.kap.spark;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.util.TempMetadataBuilder;

public class DebugSparkShell {

    public static void main(String[] args) throws Exception {
        File metaDb = new File("metastore_db");
        if (metaDb.exists())
            FileUtils.forceDelete(new File("metastore_db"));

        // logger
        System.setProperty("log4j.configuration",
                "file:" + System.getProperty("user.dir") + "/build/conf/kylin-tools-log4j.properties");

        // prepare UT metadata
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);

        // launch spark shell
        System.setProperty("derby.system.home", "/tmp/derby");
        System.setProperty("scala.usejavacp", "true");
        System.setProperty("jline.terminal", "jline.UnsupportedTerminal");
        org.apache.spark.deploy.SparkSubmit.main(new String[] { //
                "--master", "local[2]", //
                "--class", "org.apache.spark.repl.Main", //
                "--name", "Spark Shell", //
                "spark-shell" //
        });
    }
}
