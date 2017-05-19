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

package io.kyligence.kap.rest.controllerV2;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.rest.service.KapCubeServiceV2;
import io.kyligence.kap.storage.parquet.steps.ColumnarStorageUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping(value = "/cubes")
public class KapCubeControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KapCubeControllerV2.class);

    @Autowired
    @Qualifier("cubeMgmtServiceV2")
    private CubeServiceV2 cubeServiceV2;

    @Autowired
    @Qualifier("kapCubeServiceV2")
    private KapCubeServiceV2 kapCubeServiceV2;

    /**
     * get Columnar Info
     *
     * @return true
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/columnar", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getColumnarInfo(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        List<ColumnarResponse> columnar = new ArrayList<>();

        CubeInstance cube = cubeServiceV2.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        for (CubeSegment segment : cube.getSegments()) {
            final KylinConfig config = KylinConfig.getInstanceFromEnv();
            String storagePath = ColumnarStorageUtils.getSegmentDir(config, cube, segment);

            ColumnarResponse info;
            try {
                info = kapCubeServiceV2.getColumnarInfo(storagePath, segment);
            } catch (IOException ex) {
                logger.error("Can't get columnar info, cube {}, segment {}:", cube, segment);
                logger.error("{}", ex);
                continue;
            }

            columnar.add(info);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, columnar, "");
    }
}
