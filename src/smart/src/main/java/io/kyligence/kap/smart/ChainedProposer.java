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

package io.kyligence.kap.smart;

import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChainedProposer extends AbstractProposer {

    @Getter
    private final ImmutableList<AbstractProposer> proposerList;

    public ChainedProposer(AbstractContext proposeContext, ImmutableList<AbstractProposer> proposerList) {
        super(proposeContext);
        this.proposerList = proposerList;
        assert !proposerList.contains(this);
    }

    @Override
    public void execute() {
        for (AbstractProposer proposer : proposerList) {
            long start = System.currentTimeMillis();
            log.info("Enter the step of `{}`", proposer.getIdentifierName());

            proposer.execute();

            val nums = getAccelerationNumMap();
            log.info("The step of `{}` completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                    proposer.getIdentifierName(), //
                    System.currentTimeMillis() - start, //
                    nums.get(SmartMaster.AccStatusType.SUCCESS), //
                    nums.get(SmartMaster.AccStatusType.PENDING), //
                    nums.get(SmartMaster.AccStatusType.FAILED));
        }
    }

    private Map<SmartMaster.AccStatusType, Integer> getAccelerationNumMap() {
        Map<SmartMaster.AccStatusType, Integer> result = Maps.newHashMap();
        result.putIfAbsent(SmartMaster.AccStatusType.SUCCESS, 0);
        result.putIfAbsent(SmartMaster.AccStatusType.PENDING, 0);
        result.putIfAbsent(SmartMaster.AccStatusType.FAILED, 0);
        val accelerateInfoMap = proposeContext.getAccelerateInfoMap();
        for (Map.Entry<String, AccelerateInfo> entry : accelerateInfoMap.entrySet()) {
            if (entry.getValue().isPending()) {
                result.computeIfPresent(SmartMaster.AccStatusType.PENDING, (k, v) -> v + 1);
            } else if (entry.getValue().isFailed()) {
                result.computeIfPresent(SmartMaster.AccStatusType.FAILED, (k, v) -> v + 1);
            } else {
                result.computeIfPresent(SmartMaster.AccStatusType.SUCCESS, (k, v) -> v + 1);
            }
        }
        return result;
    }

    @Override
    public String getIdentifierName() {
        throw new NotImplementedException("No need to use the name of ChainProposer");
    }
}
