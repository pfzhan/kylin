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

import java.util.Locale;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

public enum LockOperateTypeEnum {
    LOCK, UNLOCK;


    public static LockOperateTypeEnum parse(String value) {
        if (value == null) {
            return null;
        }
        for (LockOperateTypeEnum typeEnum : LockOperateTypeEnum.values()) {
            if (typeEnum.name().equals(value)) {
                return typeEnum;
            }
        }
        return null;
    }
    public static void check(String lockOperateType) {
        LockOperateTypeEnum typeEnum = LockOperateTypeEnum.parse(lockOperateType);
        if (typeEnum == null) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "'%s' is required.", "lockOperateType"));
        }
    }
}
