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
package io.kyligence.kap.rest.security;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_KERBEROS_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.File;
import java.security.PrivilegedAction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.FileUtils;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class KerberosLoginManager {

    private static final Logger logger = LoggerFactory.getLogger(KerberosLoginManager.class);

    public static final String KEYTAB_SUFFIX = ".keytab";
    public static final String TMP_KEYTAB_SUFFIX = "_tmp.keytab";

    public static KerberosLoginManager getInstance() {
        return Singletons.getInstance(KerberosLoginManager.class);
    }

    public UserGroupInformation getProjectUGI(String projectName) {
        val config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        val project = projectManager.getProject(projectName);
        val principal = project.getPrincipal();
        val keytab = project.getKeytab();
        UserGroupInformation ugi = null;
        try {
            if (project.isProjectKerberosEnabled()) {
                val keytabPath = wrapAndDownloadKeytab(projectName);
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
            } else {
                ugi = UserGroupInformation.getLoginUser();
            }
        } catch (Exception e) {
            try {
                ugi = UserGroupInformation.getLoginUser();
            } catch (Exception ex) {
                logger.error("Fetch login user error.", projectName, principal, ex);
            }
            throw new KylinException(INVALID_KERBEROS_FILE, MsgPicker.getMsg().getKerberosInfoError(), e);
        }

        return ugi;
    }

    private String wrapAndDownloadKeytab(String projectName) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        val project = projectManager.getProject(projectName);
        val principal = project.getPrincipal();
        val keytab = project.getKeytab();
        String keytabPath = null;
        if (project.isProjectKerberosEnabled()) {
            String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
            keytabPath = new Path(kylinConfHome, principal + KEYTAB_SUFFIX).toString();
            File kFile = new File(keytabPath);
            if (!kFile.exists()) {
                FileUtils.decoderBase64File(keytab, keytabPath);
            }
        }
        return keytabPath;
    }

    public void checkKerberosInfo(String principal, String keytab) {
        try {
            UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        } catch (Exception e) {
            throw new KylinException(INVALID_KERBEROS_FILE, MsgPicker.getMsg().getKerberosInfoError(), e);
        }
    }

    public void checkAndReplaceProjectKerberosInfo(String project, String principal) throws Exception {
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        String keytab = new Path(kylinConfHome, principal + TMP_KEYTAB_SUFFIX).toString();
        checkKerberosInfo(principal, keytab);

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        if (!checkExistsTablesAccess(ugi, project)) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getProjectHivePermissionError());
        }
    }

    private boolean checkExistsTablesAccess(UserGroupInformation ugi, String project) {
        val kapConfig = KapConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kapConfig.getKylinConfig());
        return ugi.doAs((PrivilegedAction<Boolean>) () -> {
            ProjectInstance projectInstance = projectManager.getProject(project);
            val tables = projectInstance.getTables();
            ISourceMetadataExplorer explorer = SourceFactory.getSource(projectInstance).getSourceMetadataExplorer();
            return explorer.checkTablesAccess(tables);
        });
    }
}
