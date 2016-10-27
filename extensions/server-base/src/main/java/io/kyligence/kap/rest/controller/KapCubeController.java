/**
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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.CubeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.rest.service.KapCubeService;

@Controller
@RequestMapping(value = "/cubes")
public class KapCubeController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KapCubeController.class);

    @Autowired
    private CubeService cubeService;

    @Autowired
    private KapCubeService kapCubeService;

    /**
     * get Columnar Info
     *
     * @return true
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/columnar", method = { RequestMethod.GET })
    @ResponseBody
    public List<ColumnarResponse> getColumnarInfo(@PathVariable String cubeName) {
        List<ColumnarResponse> columnar = new ArrayList<>();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        for (CubeSegment segment : cube.getSegments()) {
            String storagePath = segment.getStorageLocationIdentifier();
            if (!storagePath.startsWith("/")) {
                final KylinConfig config = KylinConfig.getInstanceFromEnv();
                storagePath = getWorkingDir(config, cube, segment);
            }

            ColumnarResponse info = null;
            try {
                info = kapCubeService.getColumnarInfo(storagePath, segment);
            } catch (IOException ex) {
                logger.error("Can't get columnar info:");
                logger.error("{}", ex);
                continue;
            }

            columnar.add(info);
        }

        return columnar;
    }

    private String getWorkingDir(KylinConfig config, CubeInstance cube, CubeSegment cubeSegment) {
        return new StringBuffer(config.getHdfsWorkingDirectory()).append("parquet/").append(cube.getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }
}
