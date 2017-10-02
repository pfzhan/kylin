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

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

@Component("tableExtService")
public class TableExtService extends BasicService {
    @Autowired
    private AclEvaluate aclEvaluate;

    public String extractTableExt(String project, String submitter, int frequency, String tableName)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        HiveTableExtSampleJob hiveTableExtSampleJob = new HiveTableExtSampleJob(project, submitter, tableName,
                frequency);
        return hiveTableExtSampleJob.start();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getJobByTableName(String tableName, String prj) {
        return getTableManager().getTableExt(tableName, prj).getJodID();
    }

    public TableExtDesc getTableExt(String tableName, String prj) {
        aclEvaluate.checkProjectWritePermission(prj);
        return getTableManager().getTableExt(tableName, prj);
    }

    public void removeTableExt(String tableName, String prj) throws IOException {
        aclEvaluate.checkProjectWritePermission(prj);
        getTableManager().removeTableExt(tableName, prj);
    }

    public void removeJobIdFromTableExt(String jobId) throws IOException {
        for (ProjectInstance pi : getProjectManager().listAllProjects()) {
            for (TableDesc desc : getTableManager().listAllTables(pi.getName())) {
                TableExtDesc extDesc = getTableManager().getTableExt(desc);
                if (extDesc.getJodID() != null && jobId.equals(extDesc.getJodID())) {
                    extDesc.setJodID(null);
                    getTableManager().saveTableExt(extDesc, pi.getName());
                }
            }
        }
    }
}
