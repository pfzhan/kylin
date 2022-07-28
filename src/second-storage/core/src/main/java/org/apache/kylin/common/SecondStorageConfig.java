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
package org.apache.kylin.common;

import io.kyligence.kap.secondstorage.SecondStorageConstants;

/**
 * To access protect method of {@link KylinConfig}, we have to put this class in {@link org.apache.kylin.common}
 */
public class SecondStorageConfig {

    private final KylinConfig config;

    private SecondStorageConfig(KylinConfig config) {
        this.config = config;
    }

    public static SecondStorageConfig getInstanceFromEnv() {
        return wrap(KylinConfig.getInstanceFromEnv());
    }

    public static SecondStorageConfig wrap(KylinConfig config) {
        return new SecondStorageConfig(config);
    }

    public int getReplicaNum() {
        return Integer.parseInt(config.getOptional(SecondStorageConstants.NODE_REPLICA, "1"));
    }

}
