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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.tool.CubeMetaExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component("metaStoreService")
public class MetaStoreService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(MetaStoreService.class);

    /**
     * @param project the project to backup
     * @param cube    the cube to backup
     * @throws Exception
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String backup(String project, String cube) throws Exception {
        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome == null) {
            throw new RuntimeException("KYLIN_HOME undefined");
        }
        File backupRootDir = new File(KylinConfig.getKylinHome() + "/meta_backups");
        FileUtils.forceMkdir(backupRootDir);

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
        logger.info("metadata store backed up to " + backupRootDir.getAbsolutePath());
        return backupRootDir.getAbsolutePath();
    }

}
