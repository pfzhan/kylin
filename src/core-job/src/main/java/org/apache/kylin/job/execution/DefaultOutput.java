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

import java.io.InputStream;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 */
@Getter
@Setter
@EqualsAndHashCode
public class DefaultOutput implements Output {

    private ExecutableState state;
    private Map<String, String> extra;
    private String verboseMsg;
    private InputStream verboseMsgStream;
    private long lastModified;
    private long startTime;
    private long endTime;
    private long waitTime;
    private long duration;
    private long lastRunningStartTime;
    private long createTime;
    private long byteSize;
    private String shortErrMsg;
    private String failedStepId;
    private String failedSegmentId;
    private String failedStack;
    private String failedReason;

}
