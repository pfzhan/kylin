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

import org.apache.kylin.common.exception.KylinException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class LockOperateTypeEnumTest {
    private final List<String> lockOperateTypes = Arrays.stream(LockOperateTypeEnum.values()).map(x -> x.name()).collect(Collectors.toList());

    @Test
    public void testCheckSuccess() {
        lockOperateTypes.stream().forEach(x -> LockOperateTypeEnum.check(x));
    }

    @Test
    public void testCheckError() {
        Exception exception = Assertions.assertThrows(KylinException.class, () -> LockOperateTypeEnum.check(UUID.randomUUID().toString()));
        Assertions.assertEquals(exception.getMessage(), "'lockOperateType' is required.");
    }

    @Test
    public void testParse() {
        Assertions.assertEquals(LockOperateTypeEnum.parse(null), null);
        Assertions.assertEquals(LockOperateTypeEnum.parse(UUID.randomUUID().toString()), null);
        Assertions.assertEquals(LockOperateTypeEnum.parse(LockOperateTypeEnum.LOCK.name()), LockOperateTypeEnum.LOCK);
    }
}
