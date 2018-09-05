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

import org.apache.kylin.metadata.acl.TableACL;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Component("tableAclService")
public class TableACLService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(TableACLService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    private TableACL getTableACLByProject(String project) throws IOException {
        return getTableACLManager().getTableACLByCache(project);
    }

    public boolean exists(String project, String name, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getTableACLByProject(project).contains(name, type);
    }

    public List<String> getNoAccessList(String project, String table, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getTableACLByProject(project).getNoAccessList(table, type);
    }

    public List<String> getCanAccessList(String project, String table, Set<String> allIdentifiers, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getTableACLByProject(project).getCanAccessList(table, allIdentifiers, type);
    }

    public void addToTableACL(String project, String name, String table, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        getTableACLManager().addTableACL(project, name, table, type);
    }

    public void deleteFromTableACL(String project, String name, String table, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        getTableACLManager().deleteTableACL(project, name, table, type);
    }

    public void deleteFromTableACL(String project, String name, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        getTableACLManager().deleteTableACL(project, name, type);
    }

    public void deleteFromTableACLByTbl(String project, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        getTableACLManager().deleteTableACLByTbl(project, table);
    }
}
