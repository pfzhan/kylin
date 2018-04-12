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

package org.apache.kylin.common.metrics.metrics2;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ObjectNameFactory;

public class KylinObjectNameFactory implements ObjectNameFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(KylinObjectNameFactory.class);

    @Override
    public ObjectName createName(String type, String domain, String name) {
        try {
            if (name.startsWith(domain)) {
                ObjectName objectName = new ObjectName(name);
                return objectName;
            }

            ObjectName objectName = new ObjectName(domain, "name", name);
            if (objectName.isPattern()) {
                objectName = new ObjectName(domain, "name", ObjectName.quote(name));
            }
            return objectName;
        } catch (MalformedObjectNameException e) {
            try {
                return new ObjectName(domain, "name", ObjectName.quote(name));
            } catch (MalformedObjectNameException e1) {
                LOGGER.warn("Unable to register {} {}", type, name, e1);
                throw new RuntimeException(e1);
            }
        }
    }

    public ObjectName process(String domain, String name) throws MalformedObjectNameException {
        String[] kvArry = name.split(",");
        Hashtable<String, String> hashTable = new Hashtable<>();
        for (int i = 0; i < kvArry.length; i++) {
            String[] split = kvArry[i].split("=");
            hashTable.put(split[0], split[1]);
        }
        ObjectName objectName = new ObjectName(domain, hashTable);
        return objectName;
    }
}