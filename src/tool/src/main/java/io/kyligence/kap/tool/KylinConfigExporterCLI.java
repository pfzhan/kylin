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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.Unsafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinConfigExporterCLI implements IKeep {

    public static void main(String[] args) {
        if (ArrayUtils.isEmpty(args)) {
            log.error("Usage: KylinConfigExporterCli FILE_PATH");
            log.error("Example: KylinConfigExporterCli /tmp/t1.p");
            Unsafe.systemExit(1);
        }
        try {
            execute(args);
        } catch (Exception e) {
            Unsafe.systemExit(1);
        }
        Unsafe.systemExit(0);
    }

    public static void execute(String[] args) {
        String filePath = args[0];
        try {
            if (args.length == 1) {
                String exportString = KylinConfig.getInstanceFromEnv().exportAllToString();
                doExport(filePath, exportString);
                log.info("export config succeed,{}", filePath);
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            log.error("export configuration {},", filePath, e);
            ExceptionUtils.rethrow(e);
        }
    }

    private static void doExport(String filePath, String exportStr) throws IOException {
        File kFile = new File(filePath);
        if (kFile.exists()) {
            throw new IllegalStateException("file already exists:" + filePath);
        }
        FileUtils.writeStringToFile(kFile, exportStr);
    }
}
