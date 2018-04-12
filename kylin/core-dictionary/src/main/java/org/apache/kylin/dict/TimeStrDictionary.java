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

package org.apache.kylin.dict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Dictionary;

/**
 */
@SuppressWarnings("serial")
public class TimeStrDictionary extends Dictionary<String> {

    // Integer.MAX_VALUE - 1 to avoid cardinality (max_id - min_id + 1) overflow
    private static final int MAX_ID = Integer.MAX_VALUE - 1;
    private static final int MAX_LENGTH_OF_POSITIVE_LONG = 19;

    @Override
    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return MAX_ID;
    }

    @Override
    public int getSizeOfId() {
        return 4;
    }

    @Override
    public int getSizeOfValue() {
        return MAX_LENGTH_OF_POSITIVE_LONG;
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        long millis = DateFormat.stringToMillis(value);
        long seconds = millis / 1000;

        if (seconds > MAX_ID) {
            return nullId();
        } else if (seconds < 0) {
            throw new IllegalArgumentException("Illegal value: " + value + ", parsed seconds: " + seconds);
        }

        return (int) seconds;
    }

    /**
     *
     * @param id
     * @return return like "0000001430812800000"
     */
    @Override
    protected String getValueFromIdImpl(int id) {
        if (id == nullId())
            return null;

        long millis = 1000L * id;
        return DateFormat.formatToTimeWithoutMilliStr(millis);
    }

    @Override
    public void dump(PrintStream out) {
        out.println(this.toString());
    }

    @Override
    public String toString() {
        return "TimeStrDictionary supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07 (does not support millisecond)";
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;

        return o instanceof TimeStrDictionary;
    }

    @Override
    public boolean contains(Dictionary<?> other) {
        return this.equals(other);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

}
