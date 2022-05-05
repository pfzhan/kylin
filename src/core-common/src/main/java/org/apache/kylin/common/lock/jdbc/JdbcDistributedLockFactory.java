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

package org.apache.kylin.common.lock.jdbc;

import java.util.concurrent.locks.Lock;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDistributedLockFactory extends DistributedLockFactory {

    @Override
    public Lock getLockForClient(String client, String key) {
        DataSource dataSource = null;
        try {
            dataSource = JdbcDistributedLockUtil.getDataSource();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        DefaultLockRepository lockRepository = new DefaultLockRepository(dataSource, client);
        lockRepository.setPrefix(JdbcDistributedLockUtil.getGlobalDictLockTablePrefix());
        lockRepository.afterPropertiesSet();
        return new JdbcLockRegistry(lockRepository).obtain(key);
    }

    @Override
    public void initialize() {
        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setJDBCDistributedLockURL(config.getJDBCDistributedLockURL().toString());
            JdbcDistributedLockUtil.createDistributedLockTableIfNotExist();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
