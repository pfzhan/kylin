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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.HashMap;

import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.service.RawTableService;

@Controller
@RequestMapping(value = "/raw_desc")
public class RawTableDescController extends BasicController {

    @Autowired
    @Qualifier("rawTableService")
    private RawTableService rawTableService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    /**
     * Get detail information of the "Cube ID"
     * return CubeDesc instead of CubeDesc[]
     *
     * @param rawName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{rawName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getDesc(@PathVariable String rawName) throws IOException {

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(rawName);
        Draft draft = cubeService.getCubeDraft(rawName);
        
        // skip checking raw/draft being null, raw table not exist is fine

        HashMap<String, RawTableDesc> result = new HashMap<>();
        if (raw != null) {
            Preconditions.checkState(!raw.getRawTableDesc().isDraft());
            result.put("rawTable", raw.getRawTableDesc());
        }
        if (draft != null) {
            RawTableDesc draw = (RawTableDesc) draft.getEntities()[1];
            if (draw != null)
                Preconditions.checkState(draw.isDraft());
            result.put("draft", draw);            
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    public void setRawTableService(RawTableService rawTableService) {
        this.rawTableService = rawTableService;
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

}
