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

package io.kyligence.kap.rest.service;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

/*
 * This class is a dup from kylin's ServiceTestBase, because:
 * - kap has different LocalFileMetadataTestCase
 * - cannot depend on kylin-server:test:jar because it was not generated?!
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:applicationContext.xml", "classpath:kylinSecurity.xml" })
@ActiveProfiles("testing")
public class ServiceTestBase extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("spring.profiles.active", "testing");
        
        // disable job scheduler temporarily
        System.setProperty("kylin.job.scheduler.default", "77");
        System.setProperty("kylin.job.lock", "org.apache.kylin.job.lock.MockJobLock");

        staticCreateTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void tearDownResource() {
        System.clearProperty("kylin.job.scheduler.default");
        System.clearProperty("kylin.job.lock");
    }

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Broadcaster.getInstance(config).notifyClearAll();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    /**
     * better keep this method, otherwise cause error
     * org.apache.kylin.rest.service.TestBase.initializationError
     */
    @Test
    public void test() throws Exception {
    }
}
