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

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.service.RawTableServiceV2;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;

import static io.kyligence.kap.cube.raw.RawTableDesc.STATUS_DRAFT;

@Controller
@RequestMapping(value = "/raw_desc")
public class RawTableDescControllerV2 extends BasicController {

    @Autowired
    @Qualifier("rawTableServiceV2")
    private RawTableServiceV2 rawTableServiceV2;

    /**
     * Get detail information of the "Cube ID"
     * return CubeDesc instead of CubeDesc[]
     *
     * @param rawName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{rawName}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getDesc(@RequestHeader("Accept-Language") String lang, @PathVariable String rawName) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        HashMap<String, RawTableDesc> data = new HashMap<String, RawTableDesc>();

        RawTableInstance rawInstance = rawTableServiceV2.getRawTableManager().getRawTableInstance(rawName);
        if (rawInstance == null) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), rawName));
        }
        RawTableDesc desc = rawInstance.getRawTableDesc();
        if (desc == null) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_DESC_NOT_FOUND(), rawName));
        }

        if (desc.getStatus() == null) {
            data.put("rawTable", desc);

            String draftName = rawName + "_draft";
            RawTableInstance draftRawTableInstance = rawTableServiceV2.getRawTableManager().getRawTableInstance(draftName);
            if (draftRawTableInstance != null) {
                RawTableDesc draftRawTableDesc = draftRawTableInstance.getRawTableDesc();
                if (draftRawTableDesc != null && draftRawTableDesc.getStatus() != null && draftRawTableDesc.getStatus().equals(STATUS_DRAFT)) {
                    data.put("draft", draftRawTableDesc);
                }
            }
        } else if (desc.getStatus().equals(STATUS_DRAFT)) {
            data.put("draft", desc);

            String parentName = rawName.substring(0, rawName.lastIndexOf("_draft"));
            RawTableInstance parentRawTableInstance = rawTableServiceV2.getRawTableManager().getRawTableInstance(parentName);
            if (parentRawTableInstance != null) {
                RawTableDesc parentRawTableDesc = parentRawTableInstance.getRawTableDesc();
                if (parentRawTableDesc != null && parentRawTableDesc.getStatus() == null) {
                    data.put("rawTable", parentRawTableDesc);
                }
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    public void setRawTableService(RawTableServiceV2 rawTableServiceV2) {
        this.rawTableServiceV2 = rawTableServiceV2;
    }

}
