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

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateSwitchTest extends NLocalFileMetadataTestCase {

    private KylinConfig kylinConfig;

    @Before
    public void setup() {
        createTestMetadata();
        kylinConfig = getTestConfig();
        kylinConfig.setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryShareStateManager() {
        QueryShareStateManager manager;
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            kylinConfig.setProperty("kylin.query.share-state-switch-implement", "jdbc");
            manager = QueryShareStateManager.getInstance();

            String stateName = "QueryLimit";
            String stateVal = "false";
            String stateValFromDatabase = manager.getState(stateName);
            Assert.assertEquals(stateVal, stateValFromDatabase);

            String updateStateVal = "true";
            List<String> instanceList = Collections.singletonList(AddressUtil.concatInstanceName());
            manager.setState(instanceList, stateName, updateStateVal);
            stateValFromDatabase = manager.getState(stateName);
            Assert.assertEquals(updateStateVal, stateValFromDatabase);
        }
    }

    @Test
    public void testMetadataStateSwitchFunction() {
        MetadataStateSwitch metadataStateSwitch = new MetadataStateSwitch();

        String instanceName = "127.0.0.1:8080";
        String stateName = "QueryLimit";
        String stateVal = "false";

        // if there is no data in metadata
        metadataStateSwitch.put(instanceName, stateName, stateVal);
        String stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(null, stateValFromDatabase);

        Map<String, String> initStateMap = new HashMap<>();
        initStateMap.put(stateName, stateVal);

        // if no data existed when init
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);

        // if data existed when init
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);

        String updateStateVal = "true";
        metadataStateSwitch.put(instanceName, stateName, updateStateVal);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(updateStateVal, stateValFromDatabase);

        // test the 'else' branch in init method
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);
    }

    @Test
    public void testMapStringConvert() {
        Map<String, String> testMap = null;
        String convertedStr;
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        Assert.assertEquals("", convertedStr);
        testMap = new HashMap<>();
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        Assert.assertEquals("", convertedStr);

        String testStr = null;
        Map<String, String> convertedMap;
        convertedMap = MetadataStateSwitch.convertStringToMap(testStr);
        Assert.assertEquals(0, convertedMap.size());
        testStr = "";
        convertedMap = MetadataStateSwitch.convertStringToMap(testStr);
        Assert.assertEquals(0, convertedMap.size());

        testMap.clear();
        testMap.put("QueryLimit", "true");
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        convertedStr = convertedStr.replace("\u0000", "");
        convertedMap = MetadataStateSwitch.convertStringToMap(convertedStr);
        Assert.assertEquals(1, convertedMap.size());
        Assert.assertEquals("true", convertedMap.get("QueryLimit"));

        testMap.clear();
        testMap.put("QueryLimit", "false");
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        convertedStr = convertedStr.replace("\u0000", "");
        convertedMap = MetadataStateSwitch.convertStringToMap(convertedStr);
        Assert.assertEquals(1, convertedMap.size());
        Assert.assertEquals("false", convertedMap.get("QueryLimit"));
    }

}
