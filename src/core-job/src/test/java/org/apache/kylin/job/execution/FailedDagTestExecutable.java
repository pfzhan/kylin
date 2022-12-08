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

package org.apache.kylin.job.execution;

import static org.awaitility.Awaitility.await;

import org.apache.kylin.job.JobContext;
import org.awaitility.Duration;

public class FailedDagTestExecutable extends AbstractExecutable {

    public FailedDagTestExecutable() {
        super();
        this.setName("FailedDagTestExecutable");
    }

    public FailedDagTestExecutable(Object notSetId) {
        super(notSetId);
    }

    @Override
    public ExecuteResult doWork(JobContext context) {
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        return ExecuteResult.createError(new MockJobException());
    }

    @Override
    protected boolean needCheckState() {
        return false;
    }
}
