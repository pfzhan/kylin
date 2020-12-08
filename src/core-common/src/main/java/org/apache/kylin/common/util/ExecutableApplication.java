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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_EXECUTE_SHELL;
import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_PARSE_SHELL;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kylin.common.exception.KylinException;

public abstract class ExecutableApplication implements Application {

    protected abstract Options getOptions();

    protected abstract void execute(OptionsHelper optionsHelper) throws Exception;

    public void execute(String[] args) {
        OptionsHelper optionsHelper = new OptionsHelper();
        try {
            optionsHelper.parseOptions(getOptions(), args);
            execute(optionsHelper);
        } catch (ParseException e) {
            optionsHelper.printUsage("error parsing args ", getOptions());
            throw new KylinException(FAILED_PARSE_SHELL, "error parsing args", e);
        } catch (Exception e) {
            throw new KylinException(FAILED_EXECUTE_SHELL, "error execute " + this.getClass().getName(), e);
        }
    }
}
