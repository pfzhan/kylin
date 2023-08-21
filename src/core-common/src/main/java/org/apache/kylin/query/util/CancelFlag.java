/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.util;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

public class CancelFlag {
    public final AtomicBoolean atomicBoolean;
    private static final ThreadLocal<CancelFlag> CONTEXT_CANCEL_FLAG = ThreadLocal
            .withInitial(() -> new CancelFlag(new AtomicBoolean(false)));

    public CancelFlag(AtomicBoolean atomicBoolean) {
        this.atomicBoolean = Preconditions.checkNotNull(atomicBoolean);
    }

    public static CancelFlag getContextCancelFlag() {
        return CONTEXT_CANCEL_FLAG.get();
    }

    public boolean isCancelRequested() {
        return this.atomicBoolean.get();
    }

    public void requestCancel() {
        this.atomicBoolean.compareAndSet(false, true);
    }

    public void clearCancel() {
        this.atomicBoolean.compareAndSet(true, false);
    }
}
