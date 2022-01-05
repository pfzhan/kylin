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

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.type.NotConstant;

public class SparkStringUDF implements NotConstant {

    public String BASE64(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String DECODE(@Parameter(name = "str1") byte[] exp1, @Parameter(name = "str2") Object exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public byte[] ENCODE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer FIND_IN_SET(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LCASE(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LEVENSHTEIN(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LOCATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2, @Parameter(name = "num3") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LOCATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Integer exp2, @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String REPLACE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2, @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Integer exp2, @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RTRIM(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RTRIM(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String[] SENTENCES(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String[] SPLIT(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SUBSTRING_INDEX(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2, @Parameter(name = "num2") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String TRANSLATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2, @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String UCASE(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public byte[] UNBASE64(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }


}
