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

package org.apache.kylin.common.lock.jdbc;

import org.apache.kylin.common.lock.DistributedLockFactoryTest;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

@MetadataInfo(onlyProps = true)
class JdbcDistributedLockFactoryTest extends DistributedLockFactoryTest {

    @BeforeEach
    public void setup() {
       getTestConfig().getDistributedLockFactory().initialize();
    }

    @OverwriteProp(key = "kylin.metadata.distributed-lock-impl",
            value = "org.apache.kylin.common.lock.jdbc.JdbcDistributedLockFactory")
    @Test
    void testConcurrence() throws Exception {
        getTestConfig().getDistributedLockFactory().initialize();
        super.testConcurrence(UUID.randomUUID().toString(), 10, 10);
    }
}
