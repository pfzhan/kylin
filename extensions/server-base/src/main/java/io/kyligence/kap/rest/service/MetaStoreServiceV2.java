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

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.tool.CubeMetaExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by luwei on 17-4-29.
 */
@Component("metaStoreServiceV2")
public class MetaStoreServiceV2 extends MetaStoreService {

    private static final Logger logger = LoggerFactory.getLogger(MetaStoreServiceV2.class);

    /**
     * @param project the project to backup
     * @param cube    the cube to backup
     * @throws Exception
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String backup(String project, String cube) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome == null) {
            throw new BadRequestException(msg.getKYLIN_HOME_UNDEFINED());
        }
        File backupRootDir = new File(KylinConfig.getKylinHome() + "/meta_backups");
        FileUtils.forceMkdir(backupRootDir);

        // Global backup
        if (StringUtils.isEmpty(project) && StringUtils.isEmpty(cube)) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY_MM_dd_hh_mm_ss");
            String now = dateFormat.format(new Date());
            File backupDir = new File(backupRootDir.getAbsolutePath() + "/meta_" + now);
            logger.info("Starting backup to " + backupDir.getAbsolutePath());
            FileUtils.forceMkdir(backupDir);

            KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(backupDir.getAbsolutePath());
            ResourceTool.copy(KylinConfig.getInstanceFromEnv(), kylinConfig);
        } else {
            List<String> args = new ArrayList<String>();
            args.add("-destDir");
            args.add(backupRootDir.getAbsolutePath());
            if (!StringUtils.isEmpty(project)) {
                args.add("-project");
                args.add(project);
            }
            if (!StringUtils.isEmpty(cube)) {
                args.add("-cube");
                args.add(cube);
            }

            String[] cubeMetaArgs = new String[args.size()];
            args.toArray(cubeMetaArgs);
            CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
            logger.info("CubeMetaExtractor args: " + Arrays.toString(cubeMetaArgs));
            cubeMetaExtractor.execute(cubeMetaArgs);
        }
        logger.info("metadata store backed up to " + backupRootDir.getAbsolutePath());
        return backupRootDir.getAbsolutePath();
    }
}
