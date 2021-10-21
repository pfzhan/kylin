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
package io.kyligence.kap.common.persistence.transaction;

import java.util.function.Consumer;

import io.kyligence.kap.common.persistence.event.ResourceRelatedEvent;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UnitOfWorkParams<T> {

    private UnitOfWork.Callback<T> processor;

    private UnitOfWork.Callback<T> epochChecker;

    private Consumer<ResourceRelatedEvent> writeInterceptor;

    @Builder.Default
    private boolean all = false;

    @Builder.Default
    private String unitName = UnitOfWork.GLOBAL_UNIT;

    @Builder.Default
    private long epochId = UnitOfWork.DEFAULT_EPOCH_ID;

    @Builder.Default
    private int maxRetry = 3;

    @Builder.Default
    private boolean readonly = false;

    @Builder.Default
    private boolean useSandbox = true;

    @Builder.Default
    private boolean skipAuditLog = false;

    private String tempLockName;

}
