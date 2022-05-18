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

package io.kyligence.kap.metadata.state;

import io.kyligence.kap.common.state.IStateSwitch;

import java.util.HashMap;
import java.util.Map;

public class MetadataStateSwitch implements IStateSwitch {
    private JdbcShareStateStore jdbcResourceStateStore;

    public MetadataStateSwitch() {
        jdbcResourceStateStore = JdbcShareStateStore.getInstance();
    }

    @Override
    public void init(String instanceName, Map<String, String> initStateMap) {
        String serializeToString = convertMapToString(initStateMap);
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        if(shareStateInfo == null) {
            jdbcResourceStateStore.insert(instanceName, serializeToString);
        } else {
            if(!serializeToString.equals(shareStateInfo.getShareState())) {
                Map<String, String> curStateMap = convertStringToMap(shareStateInfo.getShareState());
                initStateMap.forEach(curStateMap::put);
                jdbcResourceStateStore.update(instanceName, convertMapToString(curStateMap));
            }
        }
    }

    @Override
    public void put(String instanceName, String stateName, String stateValue) {
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        if(shareStateInfo != null) {
            Map<String, String> stateMap = convertStringToMap(shareStateInfo.getShareState());
            stateMap.put(stateName, stateValue);
            String serializeToString = convertMapToString(stateMap);
            jdbcResourceStateStore.update(instanceName, serializeToString);
        }
    }

    @Override
    public String get(String instanceName, String stateName) {
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        String stateValue = null;
        if(shareStateInfo != null) {
            Map<String, String> stateMap = convertStringToMap(shareStateInfo.getShareState());
            stateValue = stateMap.get(stateName);
        }
        return stateValue;
    }

    public static String convertMapToString(Map<String, String> map) {
        if(map == null || map.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        map.forEach((key, val) -> stringBuilder.append(key).append("-").append(val).append(";"));
        // if PostgreSQl is employed for metadata, to avoid error "invalid byte sequence for encoding "UTF8": 0x00"
        return stringBuilder.toString().replace("\u0000", "");
    }

    public static Map<String, String> convertStringToMap(String string) {
        Map<String, String> result = new HashMap<>();
        if(string == null || string.isEmpty()) {
            return result;
        }
        String[] nameAndValueArray = string.split(";");
        for (String nameAndValue : nameAndValueArray) {
            String[] pair = nameAndValue.split("-");
            result.put(pair[0].trim(), pair[1].trim());
        }
        return result;
    }
}
