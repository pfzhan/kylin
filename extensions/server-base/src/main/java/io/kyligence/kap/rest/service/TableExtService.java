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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import io.kyligence.kap.engine.mr.tablestats.HiveTableExtSampleJob;

@Component("tableExtService")
public class TableExtService extends BasicService {

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public List<String> extractTableExt(String project, String submitter, String... tables) throws IOException {
        return HiveTableExtSampleJob.createSampleJob(project, submitter, tables);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getJobByTableName(String tableName) {
        return getMetaDataManager().getTableExt(tableName).getJodID();
    }

    public MetadataManager getMetaDataManager() {
        return org.apache.kylin.metadata.MetadataManager.getInstance(getConfig());
    }

    public boolean isView(String table) {
        MetadataManager metaMgr = MetadataManager.getInstance(getConfig());
        TableDesc tableDesc = metaMgr.getTableDesc(table);
        if (null != tableDesc.getTableType() && tableDesc.getTableType().equals(TableDesc.TABLE_TYPE_VIRTUAL_VIEW)) {
            return true;
        }
        return false;
    }
}
