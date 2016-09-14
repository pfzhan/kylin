/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.controller;


import java.util.HashMap;
import java.util.Map;
import org.apache.kylin.rest.controller.BasicController;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@Component("kapSystemController")
@RequestMapping(value = "/kapsystem")
public class SystemController extends BasicController {

    @RequestMapping(value = "/license", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String,String> listLicense() {
        Map<String,String> result = new HashMap<>();
        result.put("kap.license.statement",System.getProperty("kap.license.statement"));
        result.put("kap.version",System.getProperty("kap.version"));
        result.put("kap.dates",System.getProperty("kap.dates"));
        return result;
    }

}
