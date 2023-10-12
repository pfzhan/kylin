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

package org.apache.kylin.rest.scheduler;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.springframework.web.client.RestTemplate;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public abstract class AbstractSchedulerRunnable implements Runnable {
    protected String project;
    protected KylinConfig config;
    protected String tableIdentity;
    protected String partitionColumn;
    protected RestTemplate restTemplate;
    protected Boolean needRefresh = Boolean.FALSE;
    protected Set<String> needRefreshPartitionsValue = Sets.newHashSet();
    protected Queue<CheckSourceTableResult> checkSourceTableQueue = new LinkedBlockingQueue<>();

    @Override
    public void run() {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            execute();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected void execute() {
        // abstract class empty method
    }
}
