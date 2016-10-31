/**
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

package io.kyligence.kap.rest.service;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("configService")
public class ConfigService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

    /**
     * Only support the following keys currently:
     * kylin.hbase.default.compression.codec
     * kylin.job.cubing.inmem.sampling.percent
     * kylin.cube.algorithm
     * kylin.cube.aggrgroup.max.combination
     * @param key
     * @return
     */
    public String getDefaultValue(String key) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if ("kylin.hbase.default.compression.codec".equals(key)) {
            return kylinConfig.getHbaseDefaultCompressionCodec();
        } else if ("kylin.job.cubing.inmem.sampling.percent".equals(key)) {
            return String.valueOf(kylinConfig.getCubingInMemSamplingPercent());
        } else if ("kylin.cube.algorithm".equals(key)) {
            return kylinConfig.getCubeAlgorithm();
        } else if ("kylin.cube.aggrgroup.max.combination".equals(key)) {
            return String.valueOf(kylinConfig.getCubeAggrGroupMaxCombination());
        } else
            return "";
    }
}
