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

package io.kyligence.kap.rest.controller2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Controller
@RequestMapping(value = "/encodings")
public class EncodingControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(EncodingControllerV2.class);

    @Autowired
    @Qualifier("encodingService")
    private EncodingService encodingService;

    /**
     * Get valid encodings for the datatype, if no datatype parameter, return all encodings.
     *
     * @return suggestion map
     */

    @RequestMapping(value = "valid_encodings", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getValidEncodingsV2() {

        Set<String> allDatatypes = Sets.newHashSet();
        allDatatypes.addAll(DataType.DATETIME_FAMILY);
        allDatatypes.addAll(DataType.INTEGER_FAMILY);
        allDatatypes.addAll(DataType.NUMBER_FAMILY);
        allDatatypes.addAll(DataType.STRING_FAMILY);

        Map<String, List<String>> datatypeValidEncodings = Maps.newHashMap();
        for (String dataTypeStr : allDatatypes) {
            datatypeValidEncodings.put(dataTypeStr, encodingService.getValidEncodings(DataType.getType(dataTypeStr)));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, datatypeValidEncodings, "");
    }
}
