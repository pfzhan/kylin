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
package io.kyligence.kap.tool.util;

import lombok.extern.slf4j.Slf4j;

import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

@Slf4j
public class ToolMainWrapper {

    private ToolMainWrapper() {
        // only for tool main function, wrapper main code.
    }

    public interface Wrapper {
        void run() throws Exception;
    }

    public static void wrap(String[] args, Wrapper wrapper) {
        wrap(1, args, wrapper);
    }

    public static void wrap(int errorExitCode, String[] args, Wrapper wrapper) {
        try {
            wrapper.run();
        } catch (Exception e) {
            log.error("Failed to run tool: {} {}", Thread.currentThread().getStackTrace()[2].getClassName(),
                    org.apache.commons.lang3.StringUtils.join(args, org.apache.commons.lang3.StringUtils.SPACE), e);
            systemExitWhenMainThread(errorExitCode);
        }
    }

}
