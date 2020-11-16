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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

public class DependentColumnInfo {

    // map<col -> map<depCol, depColInfo>>
    Map<String, Map<String, DependentColumn>> infos = new HashMap<>();

    public boolean needMask() {
        return !infos.isEmpty();
    }

    public void add(String dbName, String tableName, Collection<DependentColumn> columnInfos) {
        for (DependentColumn info : columnInfos) {
            infos.putIfAbsent(dbName + "." + tableName + "." + info.getColumn(), new HashMap<>());

            DependentColumn dependentColumn = infos.get(dbName + "." + tableName + "." + info.getColumn()).get(info.getDependentColumnIdentity());
            if (dependentColumn != null) {
                infos.get(dbName + "." + tableName + "." + info.getColumn()).put(info.getDependentColumnIdentity(), dependentColumn.merge(info));
            } else {
                infos.get(dbName + "." + tableName + "." + info.getColumn()).put(info.getDependentColumnIdentity(), info);
            }
        }
    }

    public void validate() {
        infos.values().stream()
                .flatMap(map -> map.values().stream())
                .forEach(col -> {
            if (!get(col.getDependentColumnIdentity()).isEmpty()) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getNotSupportNestedDependentCol());
            }
        });
    }


    public Collection<DependentColumn> get(String columnIdentity) {
        return infos.getOrDefault(columnIdentity, new HashMap<>()).values();
    }

}
