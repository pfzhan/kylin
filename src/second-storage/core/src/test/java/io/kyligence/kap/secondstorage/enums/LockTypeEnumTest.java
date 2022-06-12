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
package io.kyligence.kap.secondstorage.enums;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LockTypeEnumTest {
    private final List<String> lockTypes = Arrays.stream(LockTypeEnum.values()).map(x -> x.name())
            .collect(Collectors.toList());

    @Test
    void testCheckLocks() {
        List<String> requestLocks = Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name());
        List<String> existLocks = Arrays.asList(LockTypeEnum.ALL.name());
        List<String> existLocks2 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> requestLocks2 = Arrays.asList(LockTypeEnum.QUERY.name());

        LockTypeEnum.checkLocks(null, null);
        LockTypeEnum.checkLocks(requestLocks, null);
        LockTypeEnum.checkLocks(null, existLocks);

        Exception exception = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(requestLocks, existLocks));
        Assertions.assertEquals(exception.getMessage(), MsgPicker.getMsg().getProjectLocked());
        MsgPicker.setMsg("cn");
        Exception exception2 = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(requestLocks, existLocks2));
        Assertions.assertEquals(exception2.getMessage(), MsgPicker.getMsg().getProjectLocked());
        MsgPicker.setMsg("en");

        Exception exception3 = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(existLocks2, requestLocks));
        Assertions.assertEquals(exception3.getMessage(), MsgPicker.getMsg().getProjectLocked());

        LockTypeEnum.checkLocks(existLocks2, requestLocks2);
    }

    @Test
    void testCheckSuccess() {
        LockTypeEnum.check(lockTypes);
    }

    @Test
    void testCheckNull() {
        Assertions.assertThrows(KylinException.class, () -> LockTypeEnum.check(null),
                REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("lockType"));
    }

    @Test
    void testCheckError() {
        List<String> lockTypesError = new ArrayList<>(lockTypes);
        lockTypesError.add(UUID.randomUUID().toString());
        Assertions.assertThrows(KylinException.class, () -> LockTypeEnum.check(lockTypesError),
                PARAMETER_INVALID_SUPPORT_LIST.getMsg("lockType", "QUERY, LOAD, ALL"));
    }

    @Test
    void testParse() {
        Assertions.assertNull(LockTypeEnum.parse(null));
        Assertions.assertNull(LockTypeEnum.parse(UUID.randomUUID().toString()));
        Assertions.assertEquals(LockTypeEnum.LOAD, LockTypeEnum.parse(LockTypeEnum.LOAD.name()));
    }

    @Test
    void testSubtract() {
        List<String> list1 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list2 = new ArrayList<>();
        List<String> resultList1 = LockTypeEnum.subtract(list1, list2);
        Assertions.assertIterableEquals(resultList1, list1);

        List<String> resultList2 = LockTypeEnum.subtract(list2, list1);
        Assertions.assertIterableEquals(resultList2, list2);

        List<String> list3 = Arrays.asList(LockTypeEnum.ALL.name());
        List<String> resultList3 = LockTypeEnum.subtract(list1, list3);
        Assertions.assertIterableEquals(resultList3, new ArrayList<>());

        List<String> resultList4 = LockTypeEnum.subtract(list2, list3);
        Assertions.assertIterableEquals(resultList4, new ArrayList<>());

        List<String> list5 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list6 = Arrays.asList(LockTypeEnum.QUERY.name());

        List<String> resultList5 = LockTypeEnum.subtract(list5, list6);
        Assertions.assertIterableEquals(resultList5, Arrays.asList(LockTypeEnum.LOAD.name()));

        List<String> resultList6 = LockTypeEnum.subtract(list6, list5);
        Assertions.assertIterableEquals(resultList6, Arrays.asList(LockTypeEnum.QUERY.name()));
    }

    @Test
    void testMerge() {
        List<String> list1 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list2 = new ArrayList<>();
        List<String> resultList1 = LockTypeEnum.merge(list1, list2);
        Assertions.assertIterableEquals(resultList1, list1);

        List<String> resultList2 = LockTypeEnum.merge(list2, list1);
        Assertions.assertIterableEquals(resultList2, list1);

        List<String> list3 = Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.ALL.name());
        List<String> list4 = Arrays.asList(LockTypeEnum.LOAD.name());

        List<String> resultList3 = LockTypeEnum.merge(list3, list4);
        Assertions.assertIterableEquals(resultList3, Arrays.asList(LockTypeEnum.ALL.name()));

        List<String> resultList4 = LockTypeEnum.merge(list4, list3);
        Assertions.assertIterableEquals(resultList4, Arrays.asList(LockTypeEnum.ALL.name()));

        List<String> list5 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list6 = Arrays.asList(LockTypeEnum.QUERY.name());

        List<String> resultList5 = LockTypeEnum.merge(list5, list6);
        Assertions.assertIterableEquals(resultList5,
                Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));

        List<String> resultList6 = LockTypeEnum.merge(list6, list5);
        Assertions.assertIterableEquals(resultList6,
                Arrays.asList(LockTypeEnum.QUERY.name(), LockTypeEnum.LOAD.name()));
    }
}
