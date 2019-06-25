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

package org.apache.kylin.util;

import java.lang.reflect.Method;
import java.util.Set;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import com.google.common.collect.Sets;

import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class BrokenEntityProxy implements MethodInterceptor {

    private static Set<String> methods = Sets.newHashSet("setBroken", "isBroken", "setProject", "getProject",
            "resourceName", "getId", "getUuid", "setUuid", "getAlias", "setAlias", "checkBrokenWithRelatedInfo",
            "toString", "getMvcc", "setMvcc", "setConfig", "getConfig", "getModelAlias", "getModel",
            "getRootFactTableName", "setRootFactTableName", "getJoinTables", "setJoinTables", "calcDependencies",
            "getDependencies", "setDependencies", "setHandledAfterBroken", "isHandledAfterBroken");

    private final String resourcePath;

    public static <T extends RootPersistentEntity> T getProxy(Class<T> cls, String resourcePath) {
        val proxy = new BrokenEntityProxy(resourcePath);
        T brokenEntity = (T) Enhancer.create(cls, proxy);
        brokenEntity.setBroken(true);
        brokenEntity.setMvcc(-1L);
        return brokenEntity;
    }

    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (methods.contains(method.getName())) {
            return proxy.invokeSuper(obj, args);
        }
        if (method.getName().equals("getResourcePath")) {
            return resourcePath;
        }

        if (method.getName().equals("checkIsNotCachedAndShared")) {
            return null;
        }
        throw new RuntimeException("call on Broken Entity's " + method.getName() + " method");
    }

}