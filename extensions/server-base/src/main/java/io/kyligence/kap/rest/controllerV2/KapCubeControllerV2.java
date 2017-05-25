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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.CubeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KapCubeRequest;
import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.rest.service.KapCubeServiceV2;
import io.kyligence.kap.rest.service.RawTableServiceV2;
import io.kyligence.kap.storage.parquet.steps.ColumnarStorageUtils;

@Controller
@RequestMapping(value = "/cubes")
public class KapCubeControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KapCubeControllerV2.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("kapCubeServiceV2")
    private KapCubeServiceV2 kapCubeServiceV2;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Autowired
    @Qualifier("rawTableServiceV2")
    private RawTableServiceV2 rawTableServiceV2;

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

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
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

    /**
     * Calculate Cuboid Combination based on the AggreationGroup definition.
     *
     * @param cubeDescStr
     * @return number of cuboid, -1 if failed
     */

    @RequestMapping(value = "aggregationgroups/{aggIndex}/cuboid", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse calculateCuboidCombinationV2(@RequestHeader("Accept-Language") String lang, @PathVariable int aggIndex, @RequestBody String cubeDescStr) throws IOException {
        MsgPicker.setMsg(lang);

        CubeDesc cubeDesc = deserializeCubeDescV2(cubeDescStr);
        AggregationGroup aggregationGroup = cubeDesc.getAggregationGroups().get(aggIndex);

        if (aggregationGroup != null) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggregationGroup.calculateCuboidCombination(), "");
        } else
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, -1, "");
    }

    private CubeDesc deserializeCubeDescV2(String cubeDescStr) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + cubeDescStr);
            desc = JsonUtil.readValue(cubeDescStr, CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }
        return desc;
    }


    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescV2(@RequestHeader("Accept-Language") String lang, @RequestBody KapCubeRequest kapCubeRequest) throws IOException {
        KapMsgPicker.setMsg(lang);

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : kapCubeRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            boolean createNewCube = cubeService.unifyCubeDesc(cubeDesc, false);
            cubeDesc = cubeService.updateCubeToResourceStore(cubeDesc, projectName, createNewCube);

            if (rawTableDesc != null) {
                rawTableServiceV2.validateRawTableDesc(rawTableDesc);
                rawTableDesc.setUuid(cubeDesc.getUuid());
                boolean createNewRawTable = rawTableServiceV2.unifyRawTableDesc(rawTableDesc, false);
                rawTableDesc = rawTableServiceV2.updateRawTableToResourceStore(rawTableDesc, projectName, createNewRawTable);
            } else {
                rawTableServiceV2.deleteRawByUuid(cubeDesc.getUuid(), false);
            }
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String cubeDescData = JsonUtil.writeValueAsIndentString(cubeDesc);
        String rawTableDescData = JsonUtil.writeValueAsIndentString(rawTableDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("cubeUuid", cubeDesc.getUuid());
        data.setProperty("cubeDescData", cubeDescData);
        if (rawTableDesc != null) {
            data.setProperty("rawTableUuid", rawTableDesc.getUuid());
            data.setProperty("rawTableDescData", rawTableDescData);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescDraftV2(@RequestHeader("Accept-Language") String lang, @RequestBody KapCubeRequest kapCubeRequest) throws IOException {
        KapMsgPicker.setMsg(lang);

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : kapCubeRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            boolean createNewCube = cubeService.unifyCubeDesc(cubeDesc, true);
            cubeDesc = cubeService.updateCubeToResourceStore(cubeDesc, projectName, createNewCube);

            if (rawTableDesc != null) {
                rawTableServiceV2.validateRawTableDesc(rawTableDesc);
                rawTableDesc.setUuid(cubeDesc.getUuid());
                boolean createNewRawTable = rawTableServiceV2.unifyRawTableDesc(rawTableDesc, true);
                rawTableDesc = rawTableServiceV2.updateRawTableToResourceStore(rawTableDesc, projectName, createNewRawTable);
            } else {
                rawTableServiceV2.deleteRawByUuid(cubeDesc.getUuid(), true);
            }
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String cubeDescData = JsonUtil.writeValueAsIndentString(cubeDesc);
        String rawTableDescData = JsonUtil.writeValueAsIndentString(rawTableDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("cubeUuid", cubeDesc.getUuid());
        data.setProperty("cubeDescData", cubeDescData);
        if (rawTableDesc != null) {
            data.setProperty("rawTableUuid", rawTableDesc.getUuid());
            data.setProperty("rawTableDescData", rawTableDescData);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private CubeDesc deserializeCubeDesc(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + kapCubeRequest.getCubeDescData());
            desc = JsonUtil.readValue(kapCubeRequest.getCubeDescData(), CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }
        return desc;
    }

    private RawTableDesc deserializeRawTableDesc(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableDesc desc = null;
        if (kapCubeRequest.getRawTableDescData() == null)
            return desc;

        try {
            logger.debug("Saving rawtable " + kapCubeRequest.getRawTableDescData());
            desc = JsonUtil.readValue(kapCubeRequest.getRawTableDescData(), RawTableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        }
        return desc;
    }
}
