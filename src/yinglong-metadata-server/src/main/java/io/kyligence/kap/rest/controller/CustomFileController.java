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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.rest.service.CustomFileService;

@Controller
@RequestMapping(value = "/api/custom", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class CustomFileController extends NBasicController {

    @Autowired
    @Qualifier("customFileService")
    private CustomFileService customFileService;

    @PostMapping(value = "jar")
    @ResponseBody
    public EnvelopeResponse<Set<String>> upload(@RequestParam("file") MultipartFile file,
            @RequestParam("project") String project, @RequestParam("jar_type") String jarType) {
        checkStreamingEnabled();
        if (file.isEmpty()) {
            return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, Sets.newHashSet(), "");
        }
        checkRequiredArg("jar_type", jarType);
        String projectName = checkProjectName(project);
        Set<String> classList = customFileService.uploadJar(file, projectName, jarType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, classList, "");
    }

    @DeleteMapping(value = "jar")
    @ResponseBody
    public EnvelopeResponse<String> removeJar(@RequestParam("project") String project,
            @RequestParam("jar_name") String jarName, @RequestParam("jar_type") String jarType) {
        checkStreamingEnabled();
        checkRequiredArg("jar_type", jarType);
        String projectName = checkProjectName(project);
        String removedJar = customFileService.removeJar(projectName, jarName, jarType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, removedJar, "");
    }

}
