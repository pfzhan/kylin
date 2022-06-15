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

import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.LOCK;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.check;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.parse;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.values;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LockOperateTypeEnumTest {
    private final List<String> lockOperateTypes = Arrays.stream(values()).map(Enum::name).collect(Collectors.toList());

    @Test
    void testCheckSuccess() {
        lockOperateTypes.forEach(LockOperateTypeEnum::check);
    }

    @Test
    void testCheckError() {
        String lockOperateType = UUID.randomUUID().toString();
        Assertions.assertThrows(KylinException.class, () -> check(lockOperateType),
                REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("lockOperateType"));
    }

    @Test
    void testParse() {
        Assertions.assertNull(parse(null));
        Assertions.assertNull(parse(UUID.randomUUID().toString()));
        Assertions.assertEquals(LOCK, parse(LOCK.name()));
    }
}
