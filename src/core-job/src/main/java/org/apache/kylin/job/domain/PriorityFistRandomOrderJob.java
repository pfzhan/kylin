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

package org.apache.kylin.job.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Random;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PriorityFistRandomOrderJob implements Comparable<PriorityFistRandomOrderJob>{

    private static Random random = new Random();

    private String jobId;

    private int priority;

    private int randomOrder = random.nextInt();

    @Override
    public int compareTo(PriorityFistRandomOrderJob o) {
        if (o == null) {
            return -1;
        }
        if (this.getPriority() < o.getPriority()) {
            return -1;
        } else if (this.getPriority() > o.getPriority()){
            return 1;
        } else {
            return this.randomOrder > o.randomOrder ? 1 : -1;
        }
    }
}
