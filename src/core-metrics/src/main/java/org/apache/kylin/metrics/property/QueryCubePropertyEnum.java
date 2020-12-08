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

package org.apache.kylin.metrics.property;

import com.google.common.base.Strings;

public enum QueryCubePropertyEnum {
    PROJECT("PROJECT"), CUBE("CUBE_NAME"), SEGMENT("SEGMENT_NAME"), CUBOID_SOURCE("CUBOID_SOURCE"), CUBOID_TARGET(
            "CUBOID_TARGET"), IF_MATCH("IF_MATCH"), FILTER_MASK("FILTER_MASK"), IF_SUCCESS("IF_SUCCESS"), //
    TIME_SUM("STORAGE_CALL_TIME_SUM"), TIME_MAX("STORAGE_CALL_TIME_MAX"), WEIGHT_PER_HIT("WEIGHT_PER_HIT"), CALL_COUNT(
            "STORAGE_CALL_COUNT"), SKIP_COUNT("STORAGE_COUNT_SKIP"), SCAN_COUNT("STORAGE_COUNT_SCAN"), RETURN_COUNT(
                    "STORAGE_COUNT_RETURN"), AGGR_FILTER_COUNT(
                            "STORAGE_COUNT_AGGREGATE_FILTER"), AGGR_COUNT("STORAGE_COUNT_AGGREGATE");

    private final String propertyName;

    QueryCubePropertyEnum(String name) {
        this.propertyName = name;
    }

    public static QueryCubePropertyEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QueryCubePropertyEnum property : QueryCubePropertyEnum.values()) {
            if (property.propertyName.equals(name.toUpperCase())) {
                return property;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return propertyName;
    }
}
