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

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.tool.MetadataTool;
import lombok.val;

@Service("systemService")
public class SystemService extends BasicService {

    public void backup(BackupRequest backupRequest) throws Exception {
        String[] args = createBackupArgs(backupRequest);
        val metadataTool = new MetadataTool(getConfig());
        metadataTool.execute(args);
    }

    private String[] createBackupArgs(BackupRequest backupRequest) {
        List<String> args = Lists.newArrayList("-backup", "-dir", backupRequest.getBackupPath());
        if (StringUtils.isNotBlank(backupRequest.getProject())) {
            args.add("-project");
            args.add(backupRequest.getProject());
        }
        return args.toArray(new String[0]);
    }

}
