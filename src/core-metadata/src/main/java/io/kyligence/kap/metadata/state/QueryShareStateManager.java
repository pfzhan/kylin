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
import io.kyligence.kap.common.state.StateSwitchConstant;
import io.kyligence.kap.common.util.AddressUtil;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.Singletons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryShareStateManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryShareStateManager.class);

    private static final String SHARE_STATE_CLOSE = "close";

    private static final String INSTANCE_NAME = AddressUtil.concatInstanceName();

    private IStateSwitch stateSwitch;

    public static QueryShareStateManager getInstance() {
        QueryShareStateManager instance = null;
        try {
            instance = Singletons.getInstance(QueryShareStateManager.class);
        } catch (RuntimeException e) {
            logger.error("QueryShareStateManager init failed: ", e);
        }
        return instance;
    }

    private QueryShareStateManager() {
        if (!isShareStateSwitchEnabled()) {
            return;
        }
        stateSwitch = new MetadataStateSwitch();

        Map<String, String> initStateMap = new HashMap<>();
        initStateMap.put(StateSwitchConstant.QUERY_LIMIT_STATE, "false");
        stateSwitch.init(INSTANCE_NAME, initStateMap);
    }

    public static boolean isShareStateSwitchEnabled() {
        return !SHARE_STATE_CLOSE.equals(KapConfig.getInstanceFromEnv().getShareStateSwitchImplement());
    }

    public void setState(List<String> instanceNameList, String stateName, String stateValue) {
        if (!isShareStateSwitchEnabled()) {
            return;
        }
        logger.info("Receive state set signal, instance:{}, stateName:{}, stateValue:{}", instanceNameList,
                stateName, stateValue);
        for (String instance : instanceNameList) {
            stateSwitch.put(instance, stateName, stateValue);
        }
    }

    public String getState(String stateName) {
        String stateValue = null;
        if (isShareStateSwitchEnabled()) {
            stateValue = stateSwitch.get(INSTANCE_NAME, stateName);
            logger.info("Get state value, instance:{}, stateName:{}, stateValue:{}", INSTANCE_NAME, stateName,
                    stateValue);
        }
        return stateValue;
    }

}
