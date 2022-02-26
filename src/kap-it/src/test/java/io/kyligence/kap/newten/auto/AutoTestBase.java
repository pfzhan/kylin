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

package io.kyligence.kap.newten.auto;

import java.io.IOException;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.newten.SuggestTestBase;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoTestBase extends SuggestTestBase {

    @Override
    protected SmartMaster proposeWithSmartMaster(String project, List<TestScenario> testScenarios) throws IOException {
        List<String> sqlList = collectQueries(testScenarios);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));
        String[] sqls = sqlList.toArray(new String[0]);
        AbstractContext context = AccelerationContextUtil.newSmartContext(getTestConfig(), project, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        return smartMaster;
    }

}
