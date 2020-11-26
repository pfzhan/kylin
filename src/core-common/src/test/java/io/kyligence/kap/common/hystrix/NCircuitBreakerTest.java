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

package io.kyligence.kap.common.hystrix;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NCircuitBreakerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata();
        NCircuitBreaker.start(KapConfig.getInstanceFromEnv());
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testVerifyProjectCreation() {
        NCircuitBreaker.verifyProjectCreation(10);

        thrown.expect(KylinException.class);
        NCircuitBreaker.verifyProjectCreation(100);
    }

    @Test
    public void testVerifyModelCreation() {
        NCircuitBreaker.verifyModelCreation(10);

        thrown.expect(KylinException.class);
        NCircuitBreaker.verifyModelCreation(100);
    }

    @Test
    public void testVerifyQueryResultRowCount() {
        NCircuitBreaker.verifyQueryResultRowCount(2_000_000L);

        thrown.expect(KylinException.class);
        NCircuitBreaker.verifyQueryResultRowCount(2_000_001L);
    }

}
