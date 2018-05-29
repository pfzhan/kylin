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
package org.apache.kylin.common.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Provide switch between different implementations of a same interface.
 * Each implementation is identified by an integer ID.
 */
public class ImplementationSwitch<I> {

    private static final Logger logger = LoggerFactory.getLogger(ImplementationSwitch.class);

    final private Object[] instances;
    private Class<I> interfaceClz;
    private Map<Integer, String> impls = Maps.newHashMap();

    public ImplementationSwitch(Map<Integer, String> impls, Class<I> interfaceClz) {
        this.impls.putAll(impls);
        this.interfaceClz = interfaceClz;
        this.instances = initInstances(this.impls);
    }

    private Object[] initInstances(Map<Integer, String> impls) {
        int maxId = 0;
        for (Integer id : impls.keySet()) {
            maxId = Math.max(maxId, id);
        }
        if (maxId > 100)
            throw new IllegalArgumentException("you have more than 100 implementations?");

        Object[] result = new Object[maxId + 1];

        return result;
    }

    public synchronized I get(int id) {
        String clzName = impls.get(id);
        if (clzName == null) {
            throw new IllegalArgumentException("Implementation class missing, ID " + id + ", interface " + interfaceClz.getName());
        }

        @SuppressWarnings("unchecked")
        I result = (I) instances[id];

        if (result == null) {
            try {
                result = (I) ClassUtil.newInstance(clzName);
                instances[id] = result;
            } catch (Exception ex) {
                logger.warn("Implementation missing " + clzName + " - " + ex);
            }
        }

        if (result == null)
            throw new IllegalArgumentException("Implementations missing, ID " + id + ", interface " + interfaceClz.getName());

        return result;
    }
}
