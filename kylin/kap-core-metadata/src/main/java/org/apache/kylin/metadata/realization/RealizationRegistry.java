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

package org.apache.kylin.metadata.realization;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class RealizationRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RealizationRegistry.class);

    public static RealizationRegistry getInstance(KylinConfig config) {
        return config.getManager(RealizationRegistry.class);
    }

    // called by reflection
    static RealizationRegistry newInstance(KylinConfig config) throws IOException {
        return new RealizationRegistry(config);
    }

    // ============================================================================

    private Map<String, IRealizationProvider> providers;
    private KylinConfig config;

    private RealizationRegistry(KylinConfig config) throws IOException {
        logger.info("Initializing RealizationRegistry with metadata url " + config);
        this.config = config;
        init();
    }

    private void init() {
        providers = Maps.newConcurrentMap();

        // use reflection to load providers
        String[] providerNames = config.getRealizationProviders();
        for (String clsName : providerNames) {
            try {
                Class<? extends IRealizationProvider> cls = ClassUtil.forName(clsName, IRealizationProvider.class);
                IRealizationProvider p = (IRealizationProvider) cls.getMethod("getInstance", KylinConfig.class)
                        .invoke(null, config);
                providers.put(p.getRealizationType(), p);

            } catch (Exception | NoClassDefFoundError e) {
                if (e instanceof ClassNotFoundException || e instanceof NoClassDefFoundError)
                    logger.warn("Failed to create realization provider " + e);
                else
                    logger.error("Failed to create realization provider", e);
            }
        }

        if (providers.isEmpty())
            throw new IllegalArgumentException(
                    "Failed to find realization provider by url: " + config.getMetadataUrl());

        logger.info("RealizationRegistry is " + providers);
    }

    public Set<String> getRealizationTypes() {
        return Collections.unmodifiableSet(providers.keySet());
    }

    public IRealization getRealization(String realizationType, String name) {
        IRealizationProvider p = providers.get(realizationType);
        if (p == null) {
            logger.warn("No provider for realization type " + realizationType);
            return null;
        }

        try {
            return p.getRealization(name);
        } catch (Exception ex) {
            // exception is possible if e.g. cube metadata is wrong
            logger.warn("Failed to load realization " + realizationType + ":" + name, ex);
            return null;
        }
    }

}
