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

package io.kyligence.kap.rest.request;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.validation.constraints.AssertTrue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.msg.MsgPicker;
import org.springframework.validation.FieldError;

import lombok.Data;
import lombok.val;

@Data
public class ProjectRequest implements Validation {

    private String formerProjectName;

    private String projectDescData;

    @AssertTrue
    public boolean isNameValid() {
        val pattern = Pattern.compile("^(?![_])\\w+$");
        try {
            val instance = JsonUtil.readValue(projectDescData, ProjectInstance.class);
            return pattern.matcher(instance.getName()).matches();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getErrorMessage(List<FieldError> errors) {
        val message = MsgPicker.getMsg();
        if (!CollectionUtils.isEmpty(errors) && errors.size() > 0) {
            if (errors.get(0).getField().equalsIgnoreCase("nameValid")) {
                try {
                    val instance = JsonUtil.readValue(projectDescData, ProjectInstance.class);
                    return String.format(message.getINVALID_PROJECT_NAME(), instance.getName());
                } catch (IOException e) {
                    return "";
                }
            }
        }
        return "";
    }
}
