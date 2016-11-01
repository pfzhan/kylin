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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.rest.response.SQLResponse;
import org.springframework.stereotype.Component;

@Component("massInService")
public class MassInService {
    public String storeMassIn(SQLResponse response, Functions.FilterTableType filterTableType) throws IOException {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);

        List<List<String>> result = response.getResults();
        // Assumption: one column is needed
        List<String> oneColumnResult = new ArrayList<>(result.size());
        for (List<String> line: result) {
            oneColumnResult.add(line.get(0));
        }

        String resourcePath = "";
        String filterName = RandomStringUtils.randomAlphabetic(20);

        if (filterTableType == Functions.FilterTableType.HDFS) {
            resourcePath = kapConfig.getMassinResourceIdentiferDir() + filterName;
            FileSystem fs = HadoopUtil.getFileSystem(resourcePath);

            FSDataOutputStream os = fs.create(new Path(resourcePath), false);
            BufferedOutputStream bos = IOUtils.buffer(os);
            IOUtils.writeLines(oneColumnResult, "\n", bos, "UTF-8");
            bos.close();
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }

        ExternalFilterDesc filterDesc = new ExternalFilterDesc();
        filterDesc.setName(filterName);
        filterDesc.setUuid(UUID.randomUUID().toString());
        filterDesc.setFilterResourceIdentifier(resourcePath);
        filterDesc.setFilterTableType(filterTableType);
        MetadataManager.getInstance(kylinConfig).saveExternalFilter(filterDesc);

        return filterName;
    }
}
