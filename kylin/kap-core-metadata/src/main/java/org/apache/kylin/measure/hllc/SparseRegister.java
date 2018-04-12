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
package org.apache.kylin.measure.hllc;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by xiefan on 16-12-9.
 */
public class SparseRegister implements Register, java.io.Serializable {

    private Map<Integer, Byte> sparseRegister = new TreeMap<>();

    public SparseRegister() {
    }

    public DenseRegister toDense(int p) {
        DenseRegister dr = new DenseRegister(p);
        for (Map.Entry<Integer, Byte> entry : sparseRegister.entrySet()) {
            dr.set(entry.getKey(), entry.getValue());
        }
        return dr;
    }

    @Override
    public void set(int pos, byte value) {
        sparseRegister.put(pos, value);
    }

    @Override
    public byte get(int pos) {
        Byte b = sparseRegister.get(pos);
        return b == null ? 0 : b;
    }

    @Override
    public void merge(Register another) {
        assert another.getRegisterType() != RegisterType.DENSE;
        if (another.getRegisterType() == RegisterType.SPARSE) {
            SparseRegister sr = (SparseRegister) another;
            for (Map.Entry<Integer, Byte> entry : sr.sparseRegister.entrySet()) {
                byte v = get(entry.getKey());
                if (entry.getValue() > v)
                    sparseRegister.put(entry.getKey(), entry.getValue());
            }
        } else if (another.getRegisterType() == RegisterType.SINGLE_VALUE) {
            SingleValueRegister sr = (SingleValueRegister) another;
            if (sr.getSize() > 0) {
                byte v = get(sr.getSingleValuePos());
                if (sr.getValue() > v)
                    sparseRegister.put(sr.getSingleValuePos(), sr.getValue());
            }
        }
    }

    @Override
    public void clear() {
        sparseRegister.clear();
    }

    @Override
    public int getSize() {
        return sparseRegister.size();
    }

    @Override
    public RegisterType getRegisterType() {
        return RegisterType.SPARSE;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sparseRegister == null) ? 0 : sparseRegister.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SparseRegister other = (SparseRegister) obj;
        if (sparseRegister == null) {
            if (other.sparseRegister != null)
                return false;
        } else if (!sparseRegister.equals(other.sparseRegister))
            return false;
        return true;
    }

    public Collection<Map.Entry<Integer, Byte>> getAllValue() {
        return Collections.unmodifiableCollection(sparseRegister.entrySet());
    }

}
