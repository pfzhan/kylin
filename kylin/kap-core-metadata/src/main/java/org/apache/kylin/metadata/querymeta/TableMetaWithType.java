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

package org.apache.kylin.metadata.querymeta;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Created by luwei on 17-4-26.
 */
@SuppressWarnings("serial")
public class TableMetaWithType extends TableMeta {
    public static enum tableTypeEnum implements Serializable {

        LOOKUP, FACT

    }

    private HashSet<tableTypeEnum> TYPE;

    public TableMetaWithType(String tABLE_CAT, String tABLE_SCHEM, String tABLE_NAME, String tABLE_TYPE, String rEMARKS, String tYPE_CAT, String tYPE_SCHEM, String tYPE_NAME, String sELF_REFERENCING_COL_NAME, String rEF_GENERATION) {
        TABLE_CAT = tABLE_CAT;
        TABLE_SCHEM = tABLE_SCHEM;
        TABLE_NAME = tABLE_NAME;
        TABLE_TYPE = tABLE_TYPE;
        REMARKS = rEMARKS;
        TYPE_CAT = tYPE_CAT;
        TYPE_SCHEM = tYPE_SCHEM;
        TYPE_NAME = tYPE_NAME;
        SELF_REFERENCING_COL_NAME = sELF_REFERENCING_COL_NAME;
        REF_GENERATION = rEF_GENERATION;
        TYPE = new HashSet<tableTypeEnum>();
    }

    public HashSet<tableTypeEnum> getTYPE() {
        return TYPE;
    }

    public void setTYPE(HashSet<tableTypeEnum> TYPE) {
        this.TYPE = TYPE;
    }
}
