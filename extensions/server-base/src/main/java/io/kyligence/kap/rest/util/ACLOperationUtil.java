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

package io.kyligence.kap.rest.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;

import io.kyligence.kap.metadata.acl.ColumnACLManager;
import io.kyligence.kap.metadata.acl.RowACLManager;

public class ACLOperationUtil {
    private static final TableACLManager tableACLManager = TableACLManager.getInstance(KylinConfig.getInstanceFromEnv());
    private static final ColumnACLManager columnACLManager = ColumnACLManager.getInstance(KylinConfig.getInstanceFromEnv());
    private static final RowACLManager rowACLManager = RowACLManager.getInstance(KylinConfig.getInstanceFromEnv());

    public static void delLowLevelACL(String name, String type) throws IOException {
        for (String prj : getAllPrj()) {
            delLowLevelACLByPrj(prj, name, type);
        }
    }

    public static void delLowLevelACLByPrj(String prj, String name, String type) throws IOException {
        if (tableACLManager.getTableACLByCache(prj).contains(name, type)) {
            tableACLManager.deleteTableACL(prj, name, type);
        }
        if (columnACLManager.getColumnACLByCache(prj).contains(name, type)) {
            columnACLManager.deleteColumnACL(prj, name, type);

        }
        if (rowACLManager.getRowACLByCache(prj).contains(name, type)) {
            rowACLManager.deleteRowACL(prj, name, type);
        }
    }

    public static void delLowLevelACLByTbl(String project, String table) throws IOException {
        tableACLManager.deleteTableACLByTbl(project, table);
        columnACLManager.deleteColumnACLByTbl(project, table);
        rowACLManager.deleteRowACLByTbl(project, table);
    }

    private static List<String> getAllPrj() {
        List<String> allPrjs = new ArrayList<>();
        List<ProjectInstance> projectInstances = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects();
        for (ProjectInstance pi : projectInstances) {
            allPrjs.add(pi.getName());
        }
        return allPrjs;
    }
}
