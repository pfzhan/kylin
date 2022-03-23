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

package org.apache.kylin.query.udf;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class SparkTimeUDF implements NotConstant {
    public Date TRUNC(@Parameter(name = "date") Object exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Date ADD_MONTHS(@Parameter(name = "date") Date exp1, @Parameter(name = "num2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Date ADD_MONTHS(@Parameter(name = "date") Timestamp exp1, @Parameter(name = "num2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Date ADD_MONTHS(@Parameter(name = "date") String exp1, @Parameter(name = "num2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Date DATE_ADD(@Parameter(name = "date") Object exp1, @Parameter(name = "num2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Date DATE_SUB(@Parameter(name = "date") Object exp1, @Parameter(name = "num2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String FROM_UNIXTIME(@Parameter(name = "long1") Object exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String FROM_UNIXTIME(@Parameter(name = "long1") Object exp1)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Timestamp FROM_UTC_TIMESTAMP(@Parameter(name = "t1") Object exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double MONTHS_BETWEEN(@Parameter(name = "t1") Object exp1, @Parameter(name = "t2") Object exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double MONTHS_BETWEEN(@Parameter(name = "t1") Object exp1, @Parameter(name = "t2") Object exp2,
            @Parameter(name = "t2") Boolean exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Timestamp TO_UTC_TIMESTAMP(@Parameter(name = "t1") Object exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer WEEKOFYEAR(@Parameter(name = "t1") Object exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
