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

package io.kyligence.kap.metadata.acl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SensitiveDataMaskInfo {

    private Map<String, Map<String, SensitiveDataMask>> infos = new HashMap<>();

    public boolean hasMask() {
        return !infos.isEmpty();
    }

    public void addMasks(String dbName, String tableName, Collection<SensitiveDataMask> masks) {
        for (SensitiveDataMask mask : masks) {
            infos.putIfAbsent(dbName + "." + tableName, new HashMap<>());
            SensitiveDataMask originalMask = infos.get(dbName + "." + tableName).get(mask.column);
            if (originalMask != null) {
                infos.get(dbName + "." + tableName).put(
                        mask.column,
                        new SensitiveDataMask(mask.column, mask.getType().merge(originalMask.getType())));
            } else {
                infos.get(dbName + "." + tableName).put(mask.column, mask);
            }
        }
    }

    public SensitiveDataMask getMask(String dbName, String tableName, String columnName) {
        if (infos.containsKey(dbName + "." + tableName)) {
            return infos.get(dbName + "." + tableName).get(columnName);
        }
        return null;
    }

}
