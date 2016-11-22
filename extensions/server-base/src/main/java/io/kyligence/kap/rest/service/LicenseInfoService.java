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

import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

@Component("licenseInfoService")
public class LicenseInfoService extends BasicService{

    public Map<String, String> extractLicenseInfo() {
        Map<String, String> result = new HashMap<>();
        String lic = System.getProperty("kap.license.statement");
        result.put("kap.license.statement", lic);
        result.put("kap.version", System.getProperty("kap.version"));
        result.put("kap.dates", System.getProperty("kap.dates"));
        result.put("kap.commit", System.getProperty("kap.commit"));
        result.put("kylin.commit", System.getProperty("kylin.commit"));

        try {
            if(lic != null) {
                BufferedReader reader = new BufferedReader(new StringReader(lic));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("License for KAP Evaluation")) {
                        result.put("kap.license.isEvaluation", "true");
                    }
                    if (line.startsWith("Service End:")) {
                        result.put("kap.license.serviceEnd", line.substring("Service End:".length()).trim());
                    }
                }
                reader.close();
            }

        } catch (IOException e) {
            // ignore
        }
        
        return result;
    }


}
