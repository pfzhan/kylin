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
package io.kyligence.kap.common.util;

import java.io.Serializable;

import lombok.Getter;

public class ClusterConstant implements Serializable {
    private ClusterConstant() {
    }

    public static final String QUERY = ServerModeEnum.QUERY.name;
    public static final String ALL = ServerModeEnum.ALL.name;
    public static final String JOB = ServerModeEnum.JOB.name;
    public static final String DATA_LOADING = ServerModeEnum.DATA_LOADING.name;
    public static final String SMART = ServerModeEnum.SMART.name;
    public static final String METADATA = ServerModeEnum.METADATA.name;

    @Getter
    public enum ServerModeEnum {
        QUERY("query"), ALL("all"), JOB("job"), DATA_LOADING("data_loading"), SMART("smart"), METADATA("metadata");

        private String name;

        ServerModeEnum(String name) {
            this.name = name;
        }

    }
}
