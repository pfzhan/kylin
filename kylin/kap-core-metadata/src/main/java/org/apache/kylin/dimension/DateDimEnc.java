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

package org.apache.kylin.dimension;

import java.io.Serializable;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;

/**
 * This encoding is meant to be IDENTICAL to DateStrDictionary for 100% backward compatibility.
 */
public class DateDimEnc extends AbstractDateDimEnc implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int ID_9999_12_31 = 3652426;

    public static final String ENCODING_NAME = "date";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new DateDimEnc(args);
        }
    };

    private static class DateDimValueCodec implements IValueCodec {

        private static final long serialVersionUID = 1L;
        private DataType datatype = null;

        public DateDimValueCodec(String[] args) {
            if (args != null && args.length == 1) {
                datatype = DataType.getType(args[0]);
            }
        }

        @Override
        public long valueToCode(String value) {
            //if data type is integer, DateFormat.stringToMillis recognizes format like "20001010"
            long millis = DateFormat.stringToMillis(value);

            return getNumOfDaysSince0000FromMillis(millis);
        }

        @Override
        public String codeToValue(long code) {
            long millisFromNumOfDaysSince0000 = getMillisFromNumOfDaysSince0000(code);
            if (datatype != null && datatype.isIntegerFamily()) {
                return DateFormat.formatToCompactDateStr(millisFromNumOfDaysSince0000);
            } else {
                return String.valueOf(millisFromNumOfDaysSince0000);
            }
        }
    }

    //keep this for ser/der
    public DateDimEnc() {
        super(3, new DateDimValueCodec(null));
    }

    public DateDimEnc(String[] args) {
        super(3, new DateDimValueCodec(args));
    }

    public static long getNumOfDaysSince0000FromMillis(long millis) {
        // 86400000 = 1000 * 60 * 60 * 24
        // -719530 is offset of 0000-01-01
        return (int) (millis / 86400000 + 719530);
    }

    public static long getMillisFromNumOfDaysSince0000(long n) {
        long millis = ((long) n - 719530) * 86400000;
        return millis;
    }

    public static String[] replaceEncodingArgs(String encoding, String[] encodingArgs, String encodingName, DataType type) {
        // https://issues.apache.org/jira/browse/KYLIN-2495
        if (DateDimEnc.ENCODING_NAME.equals(encodingName)) {
            if (type.isIntegerFamily()) {
                if (encodingArgs.length != 0) {
                    throw new IllegalArgumentException("Date encoding should not specify arguments: " + encoding);
                }
                return new String[] { type.toString() };
            }
        }

        return encodingArgs;
    }

}
