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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kyligence.kap.common.scheduler.SchedulerEventNotifier;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * if you extend a new broadcast event, please clear about:
 * 1. aboutBroadcast Scope
 * 2. is need to be handled by itself?
 * 3. For all current broadcast event, when last same broadcast haven't been sent to other nodes,
 * current broadcast event will be ignored. therefore, you have to override equals and hashcode methods.
 *
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
@Getter
@Setter
public class BroadcastEventReadyNotifier extends SchedulerEventNotifier {

    public BroadcastScopeEnum getBroadcastScope() {
        return BroadcastScopeEnum.WHOLE_NODES;
    }

    public boolean needBroadcastSelf() {
        return true;
    }

    public enum BroadcastScopeEnum {
        /**
         * All、Job、Query
         */
        WHOLE_NODES,

        /**
         * All、Job
         */
        LEADER_NODES,

        /**
         * All、Query
         */
        QUERY_AND_ALL,

        ALL_NODES, JOB_NODES, QUERY_NODES
    }
}
