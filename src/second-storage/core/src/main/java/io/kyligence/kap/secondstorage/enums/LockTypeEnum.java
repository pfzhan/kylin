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

import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_LOCKING;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;

public enum LockTypeEnum {
    QUERY, LOAD, ALL;

    public static List<String> subtract(List<String> existLockTypes, List<String> newLockTypes) {
        if (CollectionUtils.isEmpty(existLockTypes)) {
            return new ArrayList<>();
        } else if (CollectionUtils.isEmpty(newLockTypes)) {
            return new ArrayList<>(existLockTypes);
        } else if (newLockTypes.contains(LockTypeEnum.ALL.name())) {
            return new ArrayList<>();
        } else {
            List<String> resultList = new ArrayList<>();
            existLockTypes.stream().forEach(x -> {
                if (!newLockTypes.contains(x)) {
                    resultList.add(x);
                }
            });
            return resultList;
        }
    }

    public static List<String> merge(List<String> existLockTypes, List<String> newLockTypes) {
        if (CollectionUtils.isEmpty(existLockTypes)) {
            return new ArrayList<>(newLockTypes);
        } else if (CollectionUtils.isEmpty(newLockTypes)) {
            return new ArrayList<>(existLockTypes);
        } else if (existLockTypes.contains(LockTypeEnum.ALL.name())
                || newLockTypes.contains(LockTypeEnum.ALL.name())) {
            return Arrays.asList(LockTypeEnum.ALL.name());
        } else {
            List<String> resultList = new ArrayList<>(existLockTypes);
            newLockTypes.stream().forEach(x -> {
                if (!resultList.contains(x)) {
                    resultList.add(x);
                }
            });
            return resultList;
        }
    }

    public static LockTypeEnum parse(String value) {
        if (value == null) {
            return null;
        }
        for (LockTypeEnum lockTypeEnum : LockTypeEnum.values()) {
            if (lockTypeEnum.name().equals(value)) {
                return lockTypeEnum;
            }
        }
        return null;
    }

    public static boolean locked(String requestLock, List<String> existLocks) {
        if (requestLock == null) return false;
        return locked(Arrays.asList(requestLock), existLocks);
    }

    public static boolean locked(List<String> requestLocks, List<String> existLocks) {
        if (CollectionUtils.isEmpty(requestLocks) || CollectionUtils.isEmpty(existLocks)) {
            return false;
        }

        Set<String> requestLockSet = new HashSet<>(requestLocks);

        if (requestLockSet.contains(LockTypeEnum.ALL.name()) && !existLocks.isEmpty()) {
            return true;
        }

        Set<String> existLockSet = new HashSet<>(existLocks);

        if (existLockSet.contains(LockTypeEnum.ALL.name()) || CollectionUtils.intersection(requestLockSet, existLockSet).size() > 0) {
            return true;
        }

        return false;
    }

    public static void checkLock(String requestLock, List<String> existLocks) {
        if (requestLock == null) return;
        if (locked(Arrays.asList(requestLock), existLocks)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_LOCKING, String.format(Locale.ROOT, MsgPicker.getMsg().getProjectLocked()));
        }
    }

    public static void checkLocks(List<String> requestLocks, List<String> existLocks) {
        if (locked(requestLocks, existLocks)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_LOCKING, String.format(Locale.ROOT, MsgPicker.getMsg().getProjectLocked()));
        }
    }

    public static void check(List<String> lockTypes) {
        if (lockTypes == null || CollectionUtils.isEmpty(lockTypes)) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, "lockType");
        }
        lockTypes.stream().forEach(x -> {
            LockTypeEnum typeEnum = LockTypeEnum.parse(x);
            if (typeEnum == null) {
                throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "lockType", "QUERY, LOAD, ALL");
            }
        });
    }
}
