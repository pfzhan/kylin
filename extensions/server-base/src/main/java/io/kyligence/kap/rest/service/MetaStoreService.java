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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component("metaStoreService")
public class MetaStoreService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(MetaStoreService.class);

    /**
     *
     * @param project not in use right now, will enable in the next version
     * @param timestamp the timestamp will be used as directory name
     * @throws Exception
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String backup(String project, String timestamp, String[] includes, String[] excludes) throws Exception {
        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome == null) {
            throw new RuntimeException("KYLIN_HOME undefined");
        }
        File backupRootDir = new File(KylinConfig.getKylinHome() + "/meta_backups");
        FileUtils.forceMkdir(backupRootDir);

        File backupDir = new File(backupRootDir.getAbsolutePath() + "/meta_" + timestamp);
        logger.info("Starting backup to " + backupDir.getAbsolutePath());
        FileUtils.forceMkdir(backupDir);

        KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(backupDir.getAbsolutePath());
        if (includes != null) {
            logger.info("metadata backup with includes filter: " + Arrays.toString(includes));
            ResourceTool.setIncludes(includes);
        }
        if (excludes != null) {
            logger.info("metadata backup with excludes filter: " + Arrays.toString(excludes));
            ResourceTool.setExcludes(excludes);
        }
        try {
            ResourceTool.copy(KylinConfig.getInstanceFromEnv(), kylinConfig);
        } finally {
            ResourceTool.setIncludes(null);
            ResourceTool.setExcludes(null);
        }
        logger.info("metadata store backed up to " + backupDir.getAbsolutePath());
        return backupDir.getAbsolutePath();
    }

    /**
     *
     * @param project not in use right now, will enable in the next version
     * @throws IOException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void reset(String project) throws IOException {
        ResourceTool.reset(KylinConfig.getInstanceFromEnv());
    }

}
