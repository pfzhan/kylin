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

package org.apache.kylin.query.udf.otherUdf;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Parameter;

public class IfUDF {

    public String IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "str1") String expression1, @Parameter(name = "str2") String expression2) {
        return b ? expression1 : expression2;
    }

    public Integer IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Integer expression1, @Parameter(name = "num2") Integer expression2) {
        return b ? expression1 : expression2;
    }

    public Double IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Double expression1, @Parameter(name = "num2") Double expression2) {
        return b ? expression1 : expression2;
    }

    public Date IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "date1") Date expression1, @Parameter(name = "date2") Date expression2) {
        return b ? expression1 : expression2;
    }

    public Timestamp IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "date1") Timestamp expression1, @Parameter(name = "date2") Timestamp expression2) {
        return b ? expression1 : expression2;
    }

    public Boolean IF(@Parameter(name = "exp") Boolean b, @Parameter(name = "num1") Boolean expression1, @Parameter(name = "num2") Boolean expression2) {
        return b ? expression1 : expression2;
    }
}
