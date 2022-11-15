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
 */
package org.apache.kylin.tool;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.OptionsHelper;
import org.springframework.http.HttpHeaders;

public class DiagK8sTool extends AbstractInfoExtractorTool{

    HttpHeaders headers;

    public DiagK8sTool(HttpHeaders headers) {
        super();
        this.headers = headers;
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {

        final long startTime = getLongOption(optionsHelper, OPTION_START_TIME, getDefaultStartTime());
        final long endTime = getLongOption(optionsHelper, OPTION_END_TIME, getDefaultEndTime());
        final File recordTime = new File(exportDir, "time_used_info");

        // export cube metadata
        File metaDir = new File(exportDir, "metadata");
        FileUtils.forceMkdir(metaDir);
        String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_COMPRESS, FALSE,
                "-excludeTableExd" };
        dumpMetadata(metaToolArgs, recordTime);

        //export config
        exportK8sConf(headers, exportDir, recordTime);

        // export kylin log



        //export audit log
        File auditLogDir = new File(exportDir, "audit_log");
        FileUtils.forceMkdir(auditLogDir);
        String[] auditLogToolArgs = { "-startTime", String.valueOf(startTime), "-endTime", String.valueOf(endTime),
                OPT_DIR, auditLogDir.getAbsolutePath() };
        exportAuditLog(auditLogToolArgs, recordTime);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());
    }
}
